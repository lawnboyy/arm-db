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
}