using System.Collections.Concurrent;
using System.Data;
using System.Threading.Tasks;
using ArmDb.Common.Abstractions;
using ArmDb.Concurrency;
using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using Microsoft.Extensions.Logging;
using Constraint = ArmDb.SchemaDefinition.Constraint;
using ForeignKeyConstraint = ArmDb.SchemaDefinition.ForeignKeyConstraint;

namespace ArmDb.Storage;

internal sealed class StorageEngine : IStorageEngine
{
  private readonly BufferPoolManager _bpm;
  private readonly ILogger<StorageEngine> _logger;
  private readonly ConcurrentDictionary<string, BTree> _tables = new();
  private readonly ConcurrentDictionary<string, TableDefinition> _tableDefinitions = new();

  // TODO: Implement latch crabbing in the B-Tree should eliminate the need for course grained
  // table locking.
  // https://github.com/lawnboyy/arm-db/issues/1
  private readonly StripedSemaphoreMap<string> _tableLocks = new(1024);

  // ID Counters (In a real DB, these would be persisted in a control file or the system tables themselves)
  private int _columnId = 1;
  private int _constraintId = 1;
  private int _systemDatabaseId = 0;
  private int _systemDatabasesTableId = 0;
  private int _userTablesStartingId = 100;
  private int _nextUserTableId = -1;

  // System Table names
  public static readonly string SYS_COLUMNS_TABLE_NAME = "sys_columns";
  public static readonly string SYS_CONSTRAINTS_TABLE_NAME = "sys_constraints";
  public static readonly string SYS_DATABASES_TABLE_NAME = "sys_databases";
  public static readonly string SYS_TABLES_TABLE_NAME = "sys_tables";

  // System Table Column names
  public static readonly string SYS_DATABASES_TABLE_DATABASE_NAME_COLUMN_NAME = "database_name";
  public static readonly string SYS_TABLES_TABLE_TABLE_NAME_COLUMN_NAME = "table_name";
  public static readonly string SYS_COLUMNS_TABLE_TABLE_ID_COLUMN_NAME = "table_id";
  public static readonly string SYS_CONSTRAINTS_TABLE_TABLE_ID_COLUMN_NAME = "table_id";

  private StorageEngine(BufferPoolManager bpm, ILogger<StorageEngine> logger)
  {
    _bpm = bpm;
    _logger = logger;
  }

  public static async Task<IStorageEngine> CreateStorageEngineAsync(BufferPoolManager bpm, ILogger<StorageEngine> logger)
  {
    var storageEngine = new StorageEngine(bpm, logger);
    // This is the bootstrapping initialization of the system database for
    // capturing metadata about databases, tables, columns, and constraints.
    await storageEngine.LoadSystemTablesAsync();

    return storageEngine;
  }

  /// <summary>
  /// Creates a new database and captures the information in the sys_databases table.
  /// </summary>
  /// <param name="databaseName">The name of the database to create</param>
  /// <returns>A task that will return the newly created database ID</returns>
  /// <exception cref="NotImplementedException"></exception>
  public async Task<int> CreateDatabaseAsync(string databaseName)
  {
    // This is the bootstrapping, lazy initialization of the system database for
    // capturing metadata about databases, tables, columns, and constraints. If it
    // does not exist, then they will be created.
    // await LoadSystemTablesAsync();

    // Get the sys_databases table...
    var sysDatabasesBTree = _tables[SYS_DATABASES_TABLE_NAME];

    // TODO: Ensure that the database doesn't already exist...
    var databaseExists = await DatabaseExists(databaseName);
    if (databaseExists)
    {
      throw new InvalidOperationException($"Database with name {databaseName} already exists!");
    }

    // TODO: Extract the largest key and increment it for the new database ID
    var databaseId = 1;

    // Insert the new database metadata into the sys_databases table...
    var newDatabaseRow = new Record(
        DataValue.CreateInteger(databaseId),
        DataValue.CreateString(databaseName),
        DataValue.CreateDateTime(DateTime.UtcNow)
    );
    await sysDatabasesBTree.InsertAsync(newDatabaseRow);

    return databaseId;
  }

  public async Task CreateTableAsync(int databaseId, string tableName, TableDefinition tableDefIn)
  {
    // Ensure the database exists before we attempt to create the table.
    if (!await DatabaseExists(databaseId))
    {
      throw new InvalidOperationException($"Database with ID {databaseId} does not exist!");
    }

    if (_tables.ContainsKey(tableName))
    {
      // In a real scenario, handle "IF NOT EXISTS" or throw exception
      throw new InvalidOperationException($"Table {tableName} already exists.");
    }

    // Protect access to the critical section where the table ID is incremented and disk space allocated
    // for the new table.
    var tableSemaphore = _tableLocks[tableName];
    await tableSemaphore.WaitAsync();

    try
    {
      // If we were waiting on the table lock, then it may have been created by another thread. Therefore,
      // check our table lookup again and throw if the table already exists.
      if (_tables.ContainsKey(tableName))
      {
        throw new InvalidOperationException($"Table {tableName} already exists.");
      }

      // First assign a table ID to this new table...
      // Make sure our increment operation is thread safe.
      Interlocked.Increment(ref _nextUserTableId);
      var tableDef = tableDefIn.WithId(_nextUserTableId);

      // 2. Instantiate a new B-Tree for this table...
      var btree = await BTree.CreateAsync(_bpm, tableDef);
      // Add the B-Tree to the tables lookup...
      _tables[tableName] = btree;
      _tableDefinitions[tableName] = tableDef;

      // Now we need to insert the user table metadata into the system database.      
      await AddUserTableToSystemDatabaseAsync(tableDef, tableName, databaseId);
    }
    finally
    {
      tableSemaphore.Release();
    }
  }

  /// <summary>
  /// Get the table definition for the given table name if it exists. If it does not
  /// exist, it will return null.
  /// </summary>
  /// <param name="tableName">The name of the table of which to retrieve the definition.</param>
  /// <returns>The table definition if it exists, otherwise, null.</returns>
  public async Task<TableDefinition?> GetTableDefinitionAsync(string tableName)
  {
    if (_tableDefinitions.ContainsKey(tableName))
      return _tableDefinitions[tableName];

    // The table definition has not been loaded yet, so look up the table definition in the system tables.
    var sysTables = _tables[SYS_TABLES_TABLE_NAME];
    Record? result = null;
    await foreach (var row in sysTables.ScanAsync(SYS_TABLES_TABLE_TABLE_NAME_COLUMN_NAME, DataValue.CreateString(tableName)))
    {
      result = row;
      break;
    }

    if (result != null)
    {
      var tableId = result[0].GetAs<int>();
      // var databaseId = result[1];
      var tableDefinition = new TableDefinition(tableName, tableId);
      // Now look up the column and constration information to hydrate the table definition.
      var columns = await GetColumnDefinitions(tableId);
      foreach (var col in columns)
      {
        tableDefinition.AddColumn(col);
      }

      // TODO: Now add the constraints...
      // https://github.com/lawnboyy/arm-db/issues/2
      // var constraints = await GetConstraints(tableId);
      // foreach (var constraint in constraints)
      // {
      //   tableDefinition.AddConstraint(constraint);
      // }

      return tableDefinition;
    }

    return null;
  }

  public ValueTask DisposeAsync()
  {
    return new ValueTask();
  }

  public async Task InsertRowAsync(string tableName, Record row)
  {
    // Lookup the B-Tree for this table...
    var btree = _tables[tableName];
    // Insert the record...
    await btree.InsertAsync(row);
  }

  public async IAsyncEnumerable<Record> ScanAsync(string tableName, Key? min = null, bool minInclusive = false, Key? max = null, bool maxInclusive = false)
  {
    // Lookup the table...
    var btree = _tables[tableName];

    await foreach (var row in btree.ScanAsync(min, minInclusive, max, maxInclusive))
    {
      yield return row;
    }
  }

  private async Task<bool> DatabaseExists(int databaseId)
  {
    // Lookup the table...
    var sysDatabasesBTree = _tables[SYS_DATABASES_TABLE_NAME];
    var result = await sysDatabasesBTree.SearchAsync(Key.CreateKey(databaseId));
    return result != null;
  }

  private async Task<bool> DatabaseExists(string databaseName)
  {
    // Lookup the table...
    var sysDatabasesBTree = _tables[SYS_DATABASES_TABLE_NAME];
    var results = new List<Record>();
    await foreach (var row in sysDatabasesBTree.ScanAsync(SYS_DATABASES_TABLE_DATABASE_NAME_COLUMN_NAME, DataValue.CreateString(databaseName)))
    {
      results.Add(row);
      break;
    }

    return results.Count > 0;
  }

  private async Task<bool> SystemDatabaseExistsOnDisk()
  {
    // Fetch the metadata page, for which the page index will 0.
    try
    {
      var metadataPageId = new PageId(_systemDatabasesTableId, 0);
      var metadataPage = await _bpm.FetchPageAsync(metadataPageId);
      return metadataPage != null;
    }
    catch (CouldNotLoadPageFromDiskException)
    {
      return false;
    }
  }

  private async Task<int> GetMaxUserTableId()
  {
    var sysTables = _tables[SYS_TABLES_TABLE_NAME];
    var maxKey = await sysTables.GetMaxKey();
    var maxTableId = maxKey.Values[0].GetAs<int>();
    return maxTableId;
  }

  private async Task<IReadOnlyList<ColumnDefinition>> GetColumnDefinitions(int tableId)
  {
    // Look up the columns for this table...
    var sysColumnsTable = _tables[SYS_COLUMNS_TABLE_NAME];
    List<ColumnDefinition> columns = new();
    await foreach (var row in sysColumnsTable.ScanAsync(SYS_COLUMNS_TABLE_TABLE_ID_COLUMN_NAME, DataValue.CreateInteger(tableId)))
    {
      var colName = row.Values[2].GetAs<string>();
      var colDataTypeInfoStr = row.Values[3].GetAs<string>();
      var colDataTypeInfo = DataTypeInfo.FromString(colDataTypeInfoStr);
      var ordinalPosition = row.Values[4].GetAs<int>();
      var isNullable = row.Values[5].GetAs<bool>();
      var column = new ColumnDefinition(colName, colDataTypeInfo, isNullable, ordinalPosition /* TODO: Handle default value expressions */);
      columns.Add(column);
    }

    // Sort the columns by ordinal position...
    columns.Sort((col1, col2) =>
    {
      if (col1.OrdinalPosition > col2.OrdinalPosition) return 1;
      if (col1.OrdinalPosition < col2.OrdinalPosition) return -1;

      return 0;
    });

    return columns;
  }

  // TODO: Implement this method to hydrate constraints from sys_constraints table...
  // https://github.com/lawnboyy/arm-db/issues/2
  // private async Task<IReadOnlyList<Constraint>> GetConstraints(TableDefinition tableDef)
  // {
  //   // Look up the columns for this table...
  //   var sysConstraintsTable = _tables[SYS_CONSTRAINTS_TABLE_NAME];
  //   List<Constraint> constraints = new();
  //   await foreach (var row in sysConstraintsTable.ScanAsync(SYS_CONSTRAINTS_TABLE_TABLE_ID_COLUMN_NAME, DataValue.CreateInteger(tableId)))
  //   {
  //     // First get the constraint type...
  //     var constraintType = row.Values[3].GetAs<string>();
  //     Constraint constraint = null;
  //     switch (constraintType)
  //     {
  //       case "PrimaryKeyConstraint":
  //         constraint = new PrimaryKeyConstraint(tableDef.Name, )
  //         break;
  //     }

  //     var name = row.Values[2].GetAs<string>();
  //     var colDataTypeInfoStr = row.Values[3].GetAs<string>();
  //     var colDataTypeInfo = DataTypeInfo.FromString(colDataTypeInfoStr);
  //     var ordinalPosition = row.Values[4].GetAs<int>();
  //     var isNullable = row.Values[5].GetAs<bool>();
  //     var column = new Constraint();
  //     constraints.Add(column);
  //   }

  //   return constraints;
  // }

  /// <summary>
  /// Loads the system database tables from disk if they exist. If they don't exist yet, 
  /// they will be created.
  /// </summary>
  /// <returns>An awaitable task</returns>
  private async Task LoadSystemTablesAsync()
  {
    // Check to see if the system database exists yet...
    if (await SystemDatabaseExistsOnDisk())
    {
      await LoadSystemDatabase();
    }
    else
    {
      await CreateSystemDatabase();
    }

    // Set the max user table ID...
    var maxTableId = await GetMaxUserTableId();
    _nextUserTableId = (maxTableId < _userTablesStartingId) ? _userTablesStartingId : ++maxTableId;
  }

  private async Task LoadSystemDatabase()
  {
    // Load sys_databases table
    var sysDatabasesDef = GetSysDatabasesTableDefinition();
    await LoadTableAsync(sysDatabasesDef);

    // Load sys_tables table
    var sysTablesDef = GetSysTablesTableDefinition();
    await LoadTableAsync(sysTablesDef);

    // Load sys_columns table
    var sysColumnsDef = GetSysColumnsTableDefinition();
    await LoadTableAsync(sysColumnsDef);

    // Load the sys_constraints table
    var sysConstraintsDef = GetSysConstraintsTableDefinition();
    await LoadTableAsync(sysConstraintsDef);
  }

  private async Task LoadTableAsync(TableDefinition tableDef)
  {
    var table = await BTree.LoadAsync(_bpm, tableDef);
    // Load the table and definition into the lookup...
    _tables[tableDef.Name] = table;
    _tableDefinitions[tableDef.Name] = tableDef;
  }

  private async Task CreateSystemDatabase()
  {
    var systemTableDefinitions = new List<TableDefinition>();

    // 1. Initialize sys_databases
    var sysDatabasesDef = GetSysDatabasesTableDefinition();
    systemTableDefinitions.Add(sysDatabasesDef);
    var sysDatabasesBTree = await BTree.CreateAsync(_bpm, sysDatabasesDef);
    _tables[SYS_DATABASES_TABLE_NAME] = sysDatabasesBTree;
    _tableDefinitions[SYS_DATABASES_TABLE_NAME] = sysDatabasesDef;

    // Insert the Default "System" Database record (ID 0)
    var systemDbRow = new Record(
        DataValue.CreateInteger(_systemDatabaseId),
        DataValue.CreateString("System"),
        DataValue.CreateDateTime(DateTime.UtcNow)
    );
    await sysDatabasesBTree.InsertAsync(systemDbRow);

    // 2. Initialize sys_tables
    var sysTablesDef = GetSysTablesTableDefinition();
    systemTableDefinitions.Add(sysTablesDef);
    var sysTablesBTree = await BTree.CreateAsync(_bpm, sysTablesDef);
    _tables[SYS_TABLES_TABLE_NAME] = sysTablesBTree;
    _tableDefinitions[SYS_TABLES_TABLE_NAME] = sysTablesDef;

    // 3. Initialize sys_columns
    var sysColumnsDef = GetSysColumnsTableDefinition();
    systemTableDefinitions.Add(sysColumnsDef);
    var sysColBTree = await BTree.CreateAsync(_bpm, sysColumnsDef);
    _tables[SYS_COLUMNS_TABLE_NAME] = sysColBTree;
    _tableDefinitions[SYS_COLUMNS_TABLE_NAME] = sysColumnsDef;

    // 4. Initialize sys_constraints
    var sysConstraintsDef = GetSysConstraintsTableDefinition();
    systemTableDefinitions.Add(sysConstraintsDef);
    var sysConstraintsBTree = await BTree.CreateAsync(_bpm, sysConstraintsDef);
    _tables[SYS_CONSTRAINTS_TABLE_NAME] = sysConstraintsBTree;
    _tableDefinitions[SYS_CONSTRAINTS_TABLE_NAME] = sysConstraintsDef;

    // 5. Logical Registration: Loop through all created system tables and register them into the catalogs
    //    (sys_tables, sys_columns, sys_constraints)
    foreach (var tableDef in systemTableDefinitions)
    {
      // A. Insert into sys_tables
      var tableSchemaRow = new Record(
          DataValue.CreateInteger(tableDef.TableId),
          DataValue.CreateInteger(_systemDatabaseId),
          DataValue.CreateString(tableDef.Name),
          DataValue.CreateDateTime(DateTime.UtcNow)
      );
      var sysTables = _tables[SYS_TABLES_TABLE_NAME];
      await sysTables.InsertAsync(tableSchemaRow);

      // B. Insert into sys_columns
      var ordinal = 0;
      foreach (var columnDef in tableDef.Columns)
      {
        var columnSchemaRow = new Record(
            DataValue.CreateInteger(_columnId++),
            DataValue.CreateInteger(tableDef.TableId),
            DataValue.CreateString(columnDef.Name),
            DataValue.CreateString(columnDef.DataType.ToString()),
            DataValue.CreateInteger(ordinal++),
            DataValue.CreateBoolean(columnDef.IsNullable),
            columnDef.DefaultValueExpression == null
                ? DataValue.CreateNull(PrimitiveDataType.Varchar)
                : DataValue.CreateString(columnDef.DefaultValueExpression)
        );
        var sysColumns = _tables[SYS_COLUMNS_TABLE_NAME];
        await sysColumns.InsertAsync(columnSchemaRow);
      }

      // C. Insert into sys_constraints
      foreach (var constraintDef in tableDef.Constraints)
      {
        var constraintType = constraintDef is PrimaryKeyConstraint ? nameof(PrimaryKeyConstraint) : nameof(ForeignKeyConstraint);
        var constraintRow = new Record(
            DataValue.CreateInteger(_constraintId++),
            DataValue.CreateInteger(tableDef.TableId),
            DataValue.CreateString(constraintDef.Name),
            DataValue.CreateString(constraintType),
            DataValue.CreateString(GetConstraintDefinitionString(constraintDef)),
            DataValue.CreateDateTime(DateTime.UtcNow)
        );
        var sysConstraints = _tables[SYS_CONSTRAINTS_TABLE_NAME];
        await sysConstraints.InsertAsync(constraintRow);
      }
    }
  }

  private async Task AddUserTableToSystemDatabaseAsync(TableDefinition tableDef, string tableName, int databaseId)
  {
    // Construct a data row for this table for the system tables table...
    var tableSchemaRow = new Record(
        DataValue.CreateInteger(tableDef.TableId),
        DataValue.CreateInteger(databaseId),
        DataValue.CreateString(tableName),
        DataValue.CreateDateTime(DateTime.UtcNow)
    );
    var sysTables = _tables[SYS_TABLES_TABLE_NAME];

    // TODO: Implement proper page latching in the B-Tree for better performance. For now we'll lock the table.
    // https://github.com/lawnboyy/arm-db/issues/1
    // Lock the sys_tables for the insertion.
    var sysTablesLock = _tableLocks[SYS_TABLES_TABLE_NAME];
    await sysTablesLock.WaitAsync();
    try
    {
      await sysTables.InsertAsync(tableSchemaRow);
    }
    finally
    {
      sysTablesLock.Release();
    }

    // Insert column information for each column in the sys_columns table...
    var ordinal = 0;
    var sysColumnsLock = _tableLocks[SYS_COLUMNS_TABLE_NAME];
    foreach (var columnDef in tableDef.Columns)
    {
      var columnSchemaRow = new Record(
          DataValue.CreateInteger(_columnId++),
          DataValue.CreateInteger(tableDef.TableId),
          DataValue.CreateString(columnDef.Name),
          DataValue.CreateString(columnDef.DataType.ToString()),
          DataValue.CreateInteger(ordinal++),
          DataValue.CreateBoolean(columnDef.IsNullable),
          columnDef.DefaultValueExpression == null
              ? DataValue.CreateNull(PrimitiveDataType.Varchar)
              : DataValue.CreateString(columnDef.DefaultValueExpression)
      );
      var sysColumns = _tables[SYS_COLUMNS_TABLE_NAME];

      // TODO: Implement proper page latching in the B-Tree for better performance. For now we'll lock the table for inserts.
      // https://github.com/lawnboyy/arm-db/issues/1
      await sysColumnsLock.WaitAsync();
      try
      {
        await sysColumns.InsertAsync(columnSchemaRow);
      }
      finally
      {
        sysColumnsLock.Release();
      }
    }

    // 6. Register metadata in sys_constraints
    var sysConstraintsLock = _tableLocks[SYS_CONSTRAINTS_TABLE_NAME];
    foreach (var constraintDef in tableDef.Constraints)
    {
      var constraintType = constraintDef is PrimaryKeyConstraint ? nameof(PrimaryKeyConstraint) : nameof(ForeignKeyConstraint);
      var constraintRow = new Record(
          DataValue.CreateInteger(_constraintId++),
          DataValue.CreateInteger(tableDef.TableId),
          DataValue.CreateString(constraintDef.Name),
          DataValue.CreateString(constraintType),
          DataValue.CreateString(GetConstraintDefinitionString(constraintDef)),
          DataValue.CreateDateTime(DateTime.UtcNow)
      );
      var sysConstraints = _tables[SYS_CONSTRAINTS_TABLE_NAME];

      // TODO: Implement proper page latching in the B-Tree for better performance. For now we'll lock the table for inserts.
      // https://github.com/lawnboyy/arm-db/issues/1
      await sysConstraintsLock.WaitAsync();
      try
      {
        await sysConstraints.InsertAsync(constraintRow);
      }
      finally
      {
        sysConstraintsLock.Release();
      }
    }
  }

  private string GetConstraintDefinitionString(Constraint constraint)
  {
    if (constraint is PrimaryKeyConstraint pk)
    {
      return $"PRIMARY KEY ({string.Join(", ", pk.ColumnNames)})";
    }
    // Fallback for other constraint types not fully implemented in string gen yet
    return constraint.ToString() ?? string.Empty;
  }

  private TableDefinition GetSysDatabasesTableDefinition()
  {
    var tableDef = new TableDefinition(SYS_DATABASES_TABLE_NAME, _systemDatabasesTableId++);

    var dbId = new ColumnDefinition("database_id", new DataTypeInfo(PrimitiveDataType.Int), false);
    var dbName = new ColumnDefinition(SYS_DATABASES_TABLE_DATABASE_NAME_COLUMN_NAME, new DataTypeInfo(PrimitiveDataType.Varchar, 128), false);
    var creationDate = new ColumnDefinition("creation_date", new DataTypeInfo(PrimitiveDataType.DateTime), false);

    tableDef.AddColumn(dbId);
    tableDef.AddColumn(dbName);
    tableDef.AddColumn(creationDate);

    tableDef.AddConstraint(new PrimaryKeyConstraint(SYS_DATABASES_TABLE_NAME, new[] { "database_id" }, "PK_sys_databases"));

    return tableDef;
  }

  private TableDefinition GetSysTablesTableDefinition()
  {
    var tableDef = new TableDefinition(SYS_TABLES_TABLE_NAME, _systemDatabasesTableId++);

    var tableIdCol = new ColumnDefinition("table_id", new DataTypeInfo(PrimitiveDataType.Int), false);
    var databaseIdCol = new ColumnDefinition("database_id", new DataTypeInfo(PrimitiveDataType.Int), false);
    var tableNameCol = new ColumnDefinition("table_name", new DataTypeInfo(PrimitiveDataType.Varchar, 128), false);
    var createDateCol = new ColumnDefinition("creation_date", new DataTypeInfo(PrimitiveDataType.DateTime), false);

    tableDef.AddColumn(tableIdCol);
    tableDef.AddColumn(databaseIdCol);
    tableDef.AddColumn(tableNameCol);
    tableDef.AddColumn(createDateCol);

    tableDef.AddConstraint(new PrimaryKeyConstraint(SYS_TABLES_TABLE_NAME, new[] { "table_id" }, "PK_sys_tables"));

    return tableDef;
  }

  private TableDefinition GetSysColumnsTableDefinition()
  {
    var tableDef = new TableDefinition(SYS_COLUMNS_TABLE_NAME, _systemDatabasesTableId++);

    var columnIdCol = new ColumnDefinition("column_id", new DataTypeInfo(PrimitiveDataType.Int), false);
    var tableIdCol = new ColumnDefinition("table_id", new DataTypeInfo(PrimitiveDataType.Int), false);
    var colNameCol = new ColumnDefinition("column_name", new DataTypeInfo(PrimitiveDataType.Varchar, 128), false);
    var dataTypeInfoCol = new ColumnDefinition("data_type", new DataTypeInfo(PrimitiveDataType.Varchar, 512), false);
    var ordinalPosCol = new ColumnDefinition("ordinal_position", new DataTypeInfo(PrimitiveDataType.Int), false);
    var isNullableCol = new ColumnDefinition("is_nullable", new DataTypeInfo(PrimitiveDataType.Boolean), false);
    var defaultValueExpCol = new ColumnDefinition("default_value_expression", new DataTypeInfo(PrimitiveDataType.Varchar, 1024));

    tableDef.AddColumn(columnIdCol);
    tableDef.AddColumn(tableIdCol);
    tableDef.AddColumn(colNameCol);
    tableDef.AddColumn(dataTypeInfoCol);
    tableDef.AddColumn(ordinalPosCol);
    tableDef.AddColumn(isNullableCol);
    tableDef.AddColumn(defaultValueExpCol);

    tableDef.AddConstraint(new PrimaryKeyConstraint(SYS_COLUMNS_TABLE_NAME, new[] { "column_id" }, "PK_sys_columns"));

    return tableDef;
  }

  private TableDefinition GetSysConstraintsTableDefinition()
  {
    var tableDef = new TableDefinition(SYS_CONSTRAINTS_TABLE_NAME, _systemDatabasesTableId++);

    var constId = new ColumnDefinition("constraint_id", new DataTypeInfo(PrimitiveDataType.Int), false);
    var tableId = new ColumnDefinition("table_id", new DataTypeInfo(PrimitiveDataType.Int), false);
    var constName = new ColumnDefinition("constraint_name", new DataTypeInfo(PrimitiveDataType.Varchar, 128), false);
    var constType = new ColumnDefinition("constraint_type", new DataTypeInfo(PrimitiveDataType.Varchar, 16), false);
    var def = new ColumnDefinition("definition", new DataTypeInfo(PrimitiveDataType.Varchar, 2048), true);
    var creationDate = new ColumnDefinition("creation_date", new DataTypeInfo(PrimitiveDataType.DateTime), false);

    tableDef.AddColumn(constId);
    tableDef.AddColumn(tableId);
    tableDef.AddColumn(constName);
    tableDef.AddColumn(constType);
    tableDef.AddColumn(def);
    tableDef.AddColumn(creationDate);

    tableDef.AddConstraint(new PrimaryKeyConstraint(SYS_CONSTRAINTS_TABLE_NAME, new[] { "constraint_id" }, "PK_sys_constraints"));

    return tableDef;
  }
}