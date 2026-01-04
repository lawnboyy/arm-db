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
  private readonly StorageEngine _storageEngine;
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

    // 2. Instantiate the System Under Test (SUT)
    _storageEngine = new StorageEngine(_bpm, NullLogger<StorageEngine>.Instance);
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
    var dbName = "Finance";

    // Act
    // This should trigger the bootstrap of system tables, then create the new database
    var dbId = await _storageEngine.CreateDatabaseAsync(dbName);

    // Assert
    // 1. Scan sys_databases to verify the record exists
    var sysDatabasesRows = new List<Record>();
    await foreach (var row in _storageEngine.ScanAsync(StorageEngine.SYS_DATABASES_TABLE_NAME, null, false, null, false))
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
  public async Task CreateTableAsync_StoresTableDefinition_And_Retrievable()
  {
    // Arrange
    var tableName = "Users";
    var tableDef = new TableDefinition(tableName);
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddColumn(new ColumnDefinition("Username", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Users", ["Id"]));

    // Act
    // Create the table physically via the engine. 
    // We pass 0 (System Database) which is created automatically during bootstrap.
    await _storageEngine.CreateTableAsync(0, tableName, tableDef);

    // Attempt to retrieve the definition back
    // In a real implementation, this checks the internal map or system catalog.
    var retrievedDef = await _storageEngine.GetTableDefinitionAsync(tableName);

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
    var tableName = "Products";
    var tableDef = new TableDefinition(tableName);
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddColumn(new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Products", ["Id"]));

    await _storageEngine.CreateTableAsync(0, tableName, tableDef);

    var record = new Record(
        DataValue.CreateInteger(101),
        DataValue.CreateString("Gadget")
    );

    // Act
    await _storageEngine.InsertRowAsync(tableName, record);

    // Assert
    // We verify by scanning the table to see if the record comes back.
    // This implies IEngine needs a ScanAsync method.
    var results = new List<Record>();
    await foreach (var row in _storageEngine.ScanAsync(tableName, null, false, null, false))
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
    // 1. Arrange & Act: Create a User Table ("Customers")
    // We assume the StorageEngine handles the "Chicken and Egg" problem internally.
    // When we create the first table, it should detect that sys_tables and sys_columns 
    // do not exist and automatically create/bootstrap them before registering the user table.

    // Update: We must create a valid database first, because CreateTableAsync now validates the DB ID.
    // Calling CreateDatabaseAsync will trigger the bootstrap.
    var testDatabaseId = await _storageEngine.CreateDatabaseAsync("TestDB");

    var customersDef = new TableDefinition("Customers", 100);
    customersDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    customersDef.AddColumn(new ColumnDefinition("Email", new DataTypeInfo(PrimitiveDataType.Varchar, 100), false));
    customersDef.AddConstraint(new PrimaryKeyConstraint("PK_Customers", new[] { "Id" }));

    // This single call should register the user table
    await _storageEngine.CreateTableAsync(testDatabaseId, "Customers", customersDef);

    // 2. Assert: Verify metadata was automatically inserted into sys_tables
    var sysTablesRows = new List<Record>();
    await foreach (var row in _storageEngine.ScanAsync("sys_tables", null, false, null, false))
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
    await foreach (var row in _storageEngine.ScanAsync("sys_columns", null, false, null, false))
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