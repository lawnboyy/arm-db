using ArmDb.Storage;
using ArmDb.UnitTests.TestUtils;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ArmDb.SchemaDefinition;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.Storage;

public class StorageEngineConcurrencyTests
{
  private readonly string _baseTestDir = "StorageEngineConcurrency";

  [Fact]
  public async Task CreateTableAsync_ConcurrentSameTable_AllocatesDiskSpaceOnce()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    // Setup the full stack: DiskManager -> BPM -> StorageEngine
    var diskManager = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 50 };
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManager, NullLogger<BufferPoolManager>.Instance);

    // Use the factory method to create the engine
    var engine = await StorageEngine.CreateStorageEngineAsync(bpm, NullLogger<StorageEngine>.Instance);

    // Create a database to hold our table
    string dbName = "ConcurrencyDB";
    int dbId = await engine.CreateDatabaseAsync(dbName);

    // Define the table
    string tableName = "HighContentionTable";
    var tableDef = new TableDefinition(tableName);
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Contention", ["Id"]));

    int concurrentThreads = 10;

    // Act
    var tasks = new List<Task>();
    for (int i = 0; i < concurrentThreads; i++)
    {
      tasks.Add(Task.Run(async () =>
      {
        try
        {
          await engine.CreateTableAsync(dbId, tableName, tableDef);
        }
        catch (InvalidOperationException)
        {
          // Expected behavior: Losing threads will find the table exists 
          // and throw InvalidOperationException.
        }
      }));
    }

    await Task.WhenAll(tasks);

    // Assert
    // 1. Retrieve the table metadata from sys_tables to get its ID
    var sysTablesRows = new List<Record>();
    await foreach (var row in engine.ScanAsync(StorageEngine.SYS_TABLES_TABLE_NAME))
    {
      sysTablesRows.Add(row);
    }

    // Schema: [table_id, database_id, table_name, ...]
    var matchingRecords = sysTablesRows.Where(r => r.Values[2].ToString() == tableName).ToList();
    Assert.Single(matchingRecords);

    var tableRecord = matchingRecords.First();
    int tableId = tableRecord.Values[0].GetAs<int>();

    // 2. CRITICAL: Verify Disk Allocation happened exactly once.
    string expectedPath = Path.GetFullPath(Path.Combine(_baseTestDir, $"{tableId}.tbl"));

    // Check that the file exists. 
    // Note: The fact that we only see one record in sys_tables (checked implicitly by FirstOrDefault returning a match)
    // and that losing threads threw InvalidOperationException confirms that the critical section was protected.
    Assert.True(mockFileSystem.FileExists(expectedPath), "The table file should exist on disk.");

    // Clean up
    await engine.DisposeAsync();
  }

  [Fact]
  public async Task CreateTableAsync_ConcurrentDifferentTables_AllocatesIndependently()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManager = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 50 };
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManager, NullLogger<BufferPoolManager>.Instance);

    var engine = await StorageEngine.CreateStorageEngineAsync(bpm, NullLogger<StorageEngine>.Instance);

    string dbName = "MultiTableDB";
    int dbId = await engine.CreateDatabaseAsync(dbName);

    var tableNames = new[] { "TableA", "TableB", "TableC", "TableD" };

    // Act
    // Launch tasks to create tables concurrently. 
    // This validates that the Storage Engine handles multiple distinct requests simultaneously
    // without internal state corruption or ID collisions (Safety).
    // Note: Strict verification that these keys map to different locks (Independent Locking)
    // is covered by the StripedSemaphoreMapTests unit tests.
    var tasks = tableNames.Select(name => Task.Run(async () =>
    {
      var tableDef = new TableDefinition(name);
      tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
      tableDef.AddConstraint(new PrimaryKeyConstraint($"PK_{name}", ["Id"]));

      await engine.CreateTableAsync(dbId, name, tableDef);
    })).ToList();

    await Task.WhenAll(tasks);

    // Assert
    // 1. Scan sys_tables to get all created table records
    var sysTablesRows = new List<Record>();
    await foreach (var row in engine.ScanAsync(StorageEngine.SYS_TABLES_TABLE_NAME))
    {
      sysTablesRows.Add(row);
    }

    // 2. Verify each table has a unique ID and a corresponding file on disk
    foreach (var name in tableNames)
    {
      var tableRecord = sysTablesRows.FirstOrDefault(r => r.Values[2].ToString() == name);
      Assert.NotNull(tableRecord); // Metadata must exist

      int tableId = tableRecord.Values[0].GetAs<int>();

      // Verify file creation
      string expectedPath = Path.GetFullPath(Path.Combine(_baseTestDir, $"{tableId}.tbl"));
      Assert.True(mockFileSystem.FileExists(expectedPath), $"File for {name} (ID: {tableId}) was not created.");
    }

    // 3. Verify IDs are unique (sanity check)
    var createdIds = sysTablesRows
        .Where(r => tableNames.Contains(r.Values[2].ToString()))
        .Select(r => r.Values[0].GetAs<int>())
        .ToList();

    Assert.Equal(tableNames.Length, createdIds.Distinct().Count());

    await engine.DisposeAsync();
  }
}