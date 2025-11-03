using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;
using ArmDb.Common.Abstractions;
using ArmDb.Common.Utils;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeTests : IDisposable
{
  private readonly IFileSystem _fileSystem;
  private readonly DiskManager _diskManager;
  private readonly BufferPoolManager _bpm;
  private readonly string _baseTestDir;
  private readonly TableDefinition _tableDef;

  public BTreeTests()
  {
    _fileSystem = new FileSystem(); // Using the real file system
    _baseTestDir = Path.Combine(Path.GetTempPath(), $"ArmDb_BTree_Tests_{Guid.NewGuid()}");

    var diskManagerLogger = NullLogger<DiskManager>.Instance;
    _diskManager = new DiskManager(_fileSystem, diskManagerLogger, _baseTestDir);

    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 100 };
    var bpmLogger = NullLogger<BufferPoolManager>.Instance;
    _bpm = new BufferPoolManager(Options.Create(bpmOptions), _diskManager, bpmLogger);

    // Define a simple table for all tests
    _tableDef = new TableDefinition("TestTable");
    _tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    _tableDef.AddColumn(new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Varchar, 100), isNullable: true));
    _tableDef.AddConstraint(new PrimaryKeyConstraint("TestTable", new[] { "Id" }));
  }

  public void Dispose()
  {
    _bpm.DisposeAsync().AsTask().GetAwaiter().GetResult(); // Flush all dirty pages
    try
    {
      if (Directory.Exists(_baseTestDir))
      {
        Directory.Delete(_baseTestDir, recursive: true);
      }
    }
    catch (Exception ex)
    {
      Console.WriteLine($"Error cleaning up test directory '{_baseTestDir}': {ex.Message}");
    }
  }
}