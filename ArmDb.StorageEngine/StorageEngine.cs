using System.Collections.Concurrent;
using System.Data;
using ArmDb.Common.Abstractions;
using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using Microsoft.Extensions.Logging;

namespace ArmDb.Storage;

internal sealed class StorageEngine : IStorageEngine
{
  private readonly BufferPoolManager _bpm;
  private readonly ILogger<StorageEngine> _logger;
  private readonly ConcurrentDictionary<string, BTree> _tables = new();
  private readonly ConcurrentDictionary<string, TableDefinition> _tableDefinitions = new();

  // TODO: The current column ID will need to be persisted somewhere...
  private int _columnId = 0;
  private int _systemDatabaseId = 0;

  public static readonly string SYS_COLUMNS_TABLE_NAME = "sys_columns";
  public static readonly string SYS_CONSTRAINTS_TABLE_NAME = "sys_constraints";
  public static readonly string SYS_DATABASES_TABLE_NAME = "sys_databases";
  public static readonly string SYS_TABLES_TABLE_NAME = "sys_tables";


  internal StorageEngine(BufferPoolManager bpm, ILogger<StorageEngine> logger)
  {
    _bpm = bpm;
    _logger = logger;
  }

  public async Task CreateTableAsync(int databaseId, string tableName, TableDefinition tableDef)
  {
    // Check if the system tables exist; create them if they are not present...
    await CreateSystemTablesAsync();

    // TODO: Check to see if the table already exists...
    // Instantiate a new B-Tree for this table...
    var btree = await BTree.CreateAsync(_bpm, tableDef);
    // Add the B-Tree to the tables lookup...
    _tables[tableName] = btree;
    // Add the table schema to our table definition lookup...
    _tableDefinitions[tableName] = tableDef;

    // Construct a data row for this table for the system tables table...
    var tableSchemaRow = new Record(
      DataValue.CreateInteger(tableDef.TableId),
      DataValue.CreateInteger(databaseId),
      DataValue.CreateString(tableName),
      DataValue.CreateDateTime(DateTime.UtcNow)
    );
    // Insert the table schema row into sys_tables
    var sysTables = _tables[SYS_TABLES_TABLE_NAME];
    await sysTables.InsertAsync(tableSchemaRow);

    // Insert column information for each column in the sys_columns table...
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
      // Insert the table schema row into sys_tables
      var sysColumns = _tables[SYS_COLUMNS_TABLE_NAME];
      await sysColumns.InsertAsync(columnSchemaRow);
    }
  }

  public async Task<TableDefinition> GetTableDefinitionAsync(string tableName)
  {
    return _tableDefinitions[tableName];
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

  private async Task CreateSystemTablesAsync()
  {
    // Assume that if the sys_tables table exists, then all the system tables exist...
    // TODO: We'll need to load the system tables into the Storage Engine at boot up...
    if (_tables.ContainsKey(SYS_TABLES_TABLE_NAME))
      return;

    var systemTableDefinitions = new List<TableDefinition>();

    // Assume that if the sys_tables table does not exist, then none of the system tables exist...
    var sysTablesTableDef = GetSysTablesTableDefinition();
    systemTableDefinitions.Add(sysTablesTableDef);
    var sysTablesBTree = await BTree.CreateAsync(_bpm, sysTablesTableDef);
    // Add the B-Tree to the tables lookup...
    _tables[SYS_TABLES_TABLE_NAME] = sysTablesBTree;
    // Add the table schema to our table definition lookup...
    _tableDefinitions[SYS_TABLES_TABLE_NAME] = sysTablesTableDef;

    // Now create the sys_columns table
    var sysColumnsTableDef = GetSysColumnsTableDefinition();
    systemTableDefinitions.Add(sysColumnsTableDef);
    var sysColBTree = await BTree.CreateAsync(_bpm, sysColumnsTableDef);
    _tables[SYS_COLUMNS_TABLE_NAME] = sysColBTree;

    // Add all system tables to sys_tables table...
    foreach (var tableDef in systemTableDefinitions)
    {
      var tableSchemaRow = new Record(
        DataValue.CreateInteger(tableDef.TableId),
        DataValue.CreateInteger(_systemDatabaseId),
        DataValue.CreateString(tableDef.Name),
        DataValue.CreateDateTime(DateTime.UtcNow)
      );
      // Insert the table schema row into sys_tables
      var sysTables = _tables[SYS_TABLES_TABLE_NAME];
      await sysTables.InsertAsync(tableSchemaRow);

      // Now add all the tables' columns to the sys_columns table
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
        // Insert the table schema row into sys_tables
        var sysColumns = _tables[SYS_COLUMNS_TABLE_NAME];
        await sysColumns.InsertAsync(columnSchemaRow);
      }
    }

    // TODO: Add all the constraints to the sys_constraints table
  }

  private TableDefinition GetSysTablesTableDefinition()
  {
    var tableDef = new TableDefinition(SYS_TABLES_TABLE_NAME, 1);

    // Add columns...
    var tableIdCol = new ColumnDefinition("table_id", new DataTypeInfo(PrimitiveDataType.Int), false);
    var databaseIdCol = new ColumnDefinition("database_id", new DataTypeInfo(PrimitiveDataType.Int), false);
    var tableNameCol = new ColumnDefinition("table_name", new DataTypeInfo(PrimitiveDataType.Varchar, 128), false);
    var createDateCol = new ColumnDefinition("creation_date", new DataTypeInfo(PrimitiveDataType.DateTime), false);
    tableDef.AddColumn(tableIdCol);
    tableDef.AddColumn(databaseIdCol);
    tableDef.AddColumn(tableNameCol);
    tableDef.AddColumn(createDateCol);

    // Add constraints...
    var primaryKeyConstraint = new PrimaryKeyConstraint(SYS_TABLES_TABLE_NAME, [tableIdCol.Name], "PK_sys_tables");
    tableDef.AddConstraint(primaryKeyConstraint);

    // TODO: Add the foreign key constraint to the sys_databases table...

    return tableDef;
  }

  private TableDefinition GetSysColumnsTableDefinition()
  {
    var tableDef = new TableDefinition(SYS_COLUMNS_TABLE_NAME, 2);

    // Add columns...
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

    // Add constraints...
    var primaryKeyConstraint = new PrimaryKeyConstraint(SYS_COLUMNS_TABLE_NAME, [columnIdCol.Name], "PK_sys_columns");
    tableDef.AddConstraint(primaryKeyConstraint);

    // TODO: Add the foreign key constraint to the sys_databases table...

    return tableDef;
  }
}

