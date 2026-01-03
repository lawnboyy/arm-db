using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;
using ArmDb.Common.Utils;
using ArmDb.Common.Abstractions;
using Record = ArmDb.DataModel.Record;
using ArmDb.DataModel;

namespace ArmDb.Server.Tests
{
  public class StorageEngineTests : IDisposable
  {
    private readonly Engine _storageEngine;
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
      _storageEngine = new Engine(_bpm, NullLogger<Engine>.Instance);
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
      // This should create the underlying files on the real disk via DiskManager.
      await _storageEngine.CreateTableAsync(tableName, tableDef);

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
      tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Products", new[] { "Id" }));

      await _storageEngine.CreateTableAsync(tableName, tableDef);

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
  }
}