using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ArmDb.SchemaDefinition;
using ArmDb.Storage;
using ArmDb.Common.Utils;
using ArmDb.Common.Abstractions;
using Record = ArmDb.DataModel.Record;
using ArmDb.DataModel;

namespace ArmDb.UnitTests.Storage;

public class StorageEngineTests : IDisposable
{
  private readonly BufferPoolManager _bpm;
  private readonly DiskManager _diskManager;
  private readonly IFileSystem _fileSystem;
  private readonly string _baseTestDir;

  public StorageEngineTests()
  {
    // 1. Setup Integration Dependencies (Real File System)
    _fileSystem = new FileSystem();
    // Create a unique temporary directory for this test class instance
    _baseTestDir = Path.Combine(Path.GetTempPath(), $"ArmDb_StorageEngine_Tests_{Guid.NewGuid()}");

    // Setup DiskManager
    var diskManagerLogger = NullLogger<DiskManager>.Instance;
    _diskManager = new DiskManager(_fileSystem, diskManagerLogger, _baseTestDir);

    // Setup BufferPoolManager
    var bpmOptions = new BufferPoolManagerOptions
    {
      PoolSizeInPages = 100,
    };
    var bpmLogger = NullLogger<BufferPoolManager>.Instance;
    _bpm = new BufferPoolManager(Options.Create(bpmOptions), _diskManager, bpmLogger);
  }

  public void Dispose()
  {
    // Ensure all dirty pages are flushed and files handles closed
    _bpm.DisposeAsync().AsTask().GetAwaiter().GetResult();

    try
    {
      if (Directory.Exists(_baseTestDir))
      {
        Directory.Delete(_baseTestDir, recursive: true);
      }
    }
    catch (Exception ex)
    {
      Console.WriteLine($"Warning: Failed to clean up test directory '{_baseTestDir}': {ex.Message}");
    }
  }

  [Fact]
  public async Task CreateDatabaseAsync_PersistsDatabase_InSystemCatalog()
  {
    // Arrange
    // This should bootstrap the system tables
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);
    var dbName = "Finance";

    // Act
    // Create the new database
    var dbId = await storageEngine.CreateDatabaseAsync(dbName);

    // Assert
    // 1. Scan sys_databases to verify the record exists
    var sysDatabasesRows = new List<Record>();
    await foreach (var row in storageEngine.ScanAsync(StorageEngine.SYS_DATABASES_TABLE_NAME, null, false, null, false))
    {
      sysDatabasesRows.Add(row);
    }

    // 2. Verify we find the record matching the name
    // Schema: [database_id, database_name, creation_date]
    var dbRecord = sysDatabasesRows.FirstOrDefault(r => r.Values[1].ToString() == dbName);

    Assert.NotNull(dbRecord);
    Assert.Equal(dbId, dbRecord.Values[0].GetAs<int>());

    // 3. Verify the "System" database (ID 0) also exists (side effect of bootstrap)
    var systemRecord = sysDatabasesRows.FirstOrDefault(r => r.Values[0].GetAs<int>() == 0);
    Assert.NotNull(systemRecord);
    Assert.Equal("System", systemRecord.Values[1].ToString());
  }

  [Fact]
  public async Task CreateDatabaseAsync_ThrowsException_OnDuplicateName()
  {
    // Arrange
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);
    var dbName = "DuplicateDB";
    await storageEngine.CreateDatabaseAsync(dbName);

    // Act & Assert
    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
    {
      await storageEngine.CreateDatabaseAsync(dbName);
    });
  }

  [Fact]
  public async Task CreateTableAsync_ThrowsException_IfDatabaseDoesNotExist()
  {
    // Arrange
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);
    // We must ensure the engine is bootstrapped so we know sys_databases actually exists to query against.
    // Creating the System database/tables via the first call usually handles this.
    await storageEngine.CreateDatabaseAsync("ValidDB");

    var invalidDatabaseId = 9999;
    var tableName = "OrphanTable";
    var tableDef = new TableDefinition(tableName);
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Orphan", ["Id"]));

    // Act & Assert
    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
    {
      await storageEngine.CreateTableAsync(invalidDatabaseId, tableName, tableDef);
    });
  }

  [Fact]
  public async Task CreateTableAsync_RegistersConstraints_InSysConstraints()
  {
    // Arrange
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);
    var dbId = await storageEngine.CreateDatabaseAsync("ConstraintTestDB");
    var tableName = "ConstrainedTable";
    var tableDef = new TableDefinition(tableName);
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Test_Table", new[] { "Id" }, "PK_Test"));

    // Act
    await storageEngine.CreateTableAsync(dbId, tableName, tableDef);

    // Assert
    var sysConstraintsRows = new List<Record>();
    await foreach (var row in storageEngine.ScanAsync(StorageEngine.SYS_CONSTRAINTS_TABLE_NAME))
    {
      sysConstraintsRows.Add(row);
    }

    // Verify the PK constraint exists
    // Schema: [constraint_id, table_id, constraint_name, constraint_type, definition, creation_date]
    // Index 2: constraint_name
    // Index 3: constraint_type
    // Index 4: definition
    var constraintRecord = sysConstraintsRows.FirstOrDefault(r => r.Values[2].ToString() == "PK_Test");

    Assert.NotNull(constraintRecord);
    Assert.Equal("PrimaryKeyConstraint", constraintRecord.Values[3].ToString());
    Assert.Contains("PRIMARY KEY (Id)", constraintRecord.Values[4].ToString());
  }

  [Fact]
  public async Task CreateTableAsync_ThrowsException_IfTableAlreadyExists()
  {
    // Arrange
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);
    var dbId = await storageEngine.CreateDatabaseAsync("DuplicateTableDB");
    var tableName = "ExistingTable";
    var tableDef = new TableDefinition(tableName);
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Existing", new[] { "Id" }));

    // Create the table once
    await storageEngine.CreateTableAsync(dbId, tableName, tableDef);

    // Act & Assert
    // Try creating it again with the same name
    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
    {
      await storageEngine.CreateTableAsync(dbId, tableName, tableDef);
    });
  }

  [Fact]
  public async Task GetTableDefinitionAsync_ReturnsNull_ForNonExistentTable()
  {
    // Arrange
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);
    // Ensure the system is initialized (optional but safe)
    await storageEngine.CreateDatabaseAsync("SetupDB");

    // Act
    var result = await storageEngine.GetTableDefinitionAsync("GhostTable");

    // Assert
    Assert.Null(result);
  }

  [Fact]
  public async Task GetTableDefinition_ReturnsDefinition_AfterRestart()
  {
    // Arrange
    var dbName = "RestartDB";
    var tableName = "PersistentTable";
    var tableDef = new TableDefinition(tableName);
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Persist", ["Id"]));

    // 1. Create DB and Table in the primary engine (simulating initial run)
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);

    var dbId = await storageEngine.CreateDatabaseAsync(dbName);
    await storageEngine.CreateTableAsync(dbId, tableName, tableDef);

    // 2. Simulate a Restart: Create a NEW StorageEngine instance 
    // It shares the same BufferPool and DiskManager (so data persists), 
    // but its internal _tableDefinitions cache is empty.
    var newEngineLogger = NullLogger<StorageEngine>.Instance;
    // Use factory for the second instance as well
    var restartEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, newEngineLogger);

    // Act
    // This call should fail if Lazy Loading is not implemented, 
    // because "PersistentTable" is not in restartEngine's memory.
    var retrievedDef = await restartEngine.GetTableDefinitionAsync(tableName);

    // Assert
    Assert.NotNull(retrievedDef);
    Assert.Equal(tableName, retrievedDef.Name);
    Assert.Equal("Id", retrievedDef.Columns[0].Name);
  }

  [Fact]
  public async Task CreateTableAsync_StoresTableDefinition_And_Retrievable()
  {
    // Arrange
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);
    var tableName = "Users";
    var tableDef = new TableDefinition(tableName);
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddColumn(new ColumnDefinition("Username", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Users", ["Id"]));

    // Act
    // Create the table physically via the engine. 
    // We pass 0 (System Database) which is created automatically during bootstrap.
    await storageEngine.CreateTableAsync(0, tableName, tableDef);

    // Attempt to retrieve the definition back
    // In a real implementation, this checks the internal map or system catalog.
    var retrievedDef = await storageEngine.GetTableDefinitionAsync(tableName);

    // Assert
    Assert.NotNull(retrievedDef);
    Assert.Equal(tableName, retrievedDef.Name);
    Assert.Equal(2, retrievedDef.Columns.Count);
    Assert.Equal("Id", retrievedDef.Columns[0].Name);
  }

  [Fact]
  public async Task InsertRecordAsync_PersistsData_And_CanBeScanned()
  {
    // Arrange
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);
    var tableName = "Products";
    var tableDef = new TableDefinition(tableName);
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddColumn(new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Products", ["Id"]));

    await storageEngine.CreateTableAsync(0, tableName, tableDef);

    var record = new Record(
        DataValue.CreateInteger(101),
        DataValue.CreateString("Gadget")
    );

    // Act
    await storageEngine.InsertRowAsync(tableName, record);

    // Assert
    // We verify by scanning the table to see if the record comes back.
    // This implies IEngine needs a ScanAsync method.
    var results = new List<Record>();
    await foreach (var row in storageEngine.ScanAsync(tableName, null, false, null, false))
    {
      results.Add(row);
    }

    Assert.Single(results);
    // Assuming Record.Equals() implements value equality (which your Record class does)
    Assert.Equal(record, results[0]);
  }

  [Fact]
  public async Task CreateTableAsync_BootstrapsSystemTables_And_RegistersMetadata()
  {
    // Arrange
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);

    // 1. Arrange & Act: Create a User Table ("Customers")
    // We assume the StorageEngine handles the "Chicken and Egg" problem internally.
    // When we create the first table, it should detect that sys_tables and sys_columns 
    // do not exist and automatically create/bootstrap them before registering the user table.

    // Update: We must create a valid database first, because CreateTableAsync now validates the DB ID.
    // Calling CreateDatabaseAsync will trigger the bootstrap.
    var testDatabaseId = await storageEngine.CreateDatabaseAsync("TestDB");

    var customersDef = new TableDefinition("Customers", 100);
    customersDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    customersDef.AddColumn(new ColumnDefinition("Email", new DataTypeInfo(PrimitiveDataType.Varchar, 100), false));
    customersDef.AddConstraint(new PrimaryKeyConstraint("PK_Customers", new[] { "Id" }));

    // This single call should register the user table
    await storageEngine.CreateTableAsync(testDatabaseId, "Customers", customersDef);

    // 2. Assert: Verify metadata was automatically inserted into sys_tables
    var sysTablesRows = new List<Record>();
    await foreach (var row in storageEngine.ScanAsync("sys_tables", null, false, null, false))
    {
      sysTablesRows.Add(row);
    }

    // Verify sys_tables (ID 1) was bootstrapped and registered
    Assert.Contains(sysTablesRows, r => r.Values[2].ToString() == StorageEngine.SYS_TABLES_TABLE_NAME);

    // Verify sys_columns (ID 2) was bootstrapped and registered
    Assert.Contains(sysTablesRows, r => r.Values[2].ToString() == StorageEngine.SYS_COLUMNS_TABLE_NAME);

    // Verify the "Customers" user table was registered
    // Schema: [table_id, database_id, table_name]
    Assert.Contains(sysTablesRows, r => r.Values[2].ToString() == "Customers"
                                        && r.Values[1].GetAs<int>() == testDatabaseId
                                        && r.Values[0].GetAs<int>() == 100);

    // 3. Assert: Verify metadata was automatically inserted into sys_columns
    var sysColumnsRows = new List<Record>();
    await foreach (var row in storageEngine.ScanAsync("sys_columns", null, false, null, false))
    {
      sysColumnsRows.Add(row);
    }

    // Verify columns for Customers exist
    // Schema for sys_columns varies, usually: [column_id, table_id, column_name, ...]
    // Checking column_name at index 2 (based on StorageEngine implementation)
    Assert.Contains(sysColumnsRows, r => r.Values[2].ToString() == "Email");
    Assert.Contains(sysColumnsRows, r => r.Values[2].ToString() == "Id");

    // Verify columns for system tables also exist (proving complete bootstrap)
    Assert.Contains(sysColumnsRows, r => r.Values[2].ToString() == "table_name"); // from sys_tables
    Assert.Contains(sysColumnsRows, r => r.Values[2].ToString() == "data_type");  // from sys_columns (actually defined as 'data_type' in your latest code)
  }
}