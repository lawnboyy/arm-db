using ArmDb.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ArmDb.Common.Abstractions;
using Record = ArmDb.DataModel.Record;
using ArmDb.Common.Utils;

namespace ArmDb.UnitTests.Storage;

public class StorageEngineGenesisTests : IDisposable
{
  private readonly BufferPoolManager _bpm;
  private readonly DiskManager _diskManager;
  private readonly IFileSystem _fileSystem;
  private readonly string _baseTestDir;

  public StorageEngineGenesisTests()
  {
    // 1. Setup Integration Dependencies (Real File System)
    _fileSystem = new FileSystem();
    // Create a unique temporary directory for this test class instance
    _baseTestDir = Path.Combine(Path.GetTempPath(), $"ArmDb_Genesis_Tests_{Guid.NewGuid()}");

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
  public async Task Bootstrap_CreatesAllSystemTables_WithCorrectSchema()
  {
    // 1. Act: Initialize the engine (trigger Genesis)
    var engine = await StorageEngine.CreateStorageEngineAsync(_bpm, NullLogger<StorageEngine>.Instance);

    // 2. Assert: System Database Exists
    var databases = await ScanAllAsync(engine, StorageEngine.SYS_DATABASES_TABLE_NAME);
    var systemDb = databases.FirstOrDefault(r => r.Values[1].ToString() == "System");
    Assert.NotNull(systemDb);
    int systemDbId = systemDb.Values[0].GetAs<int>();
    Assert.Equal(0, systemDbId); // Standard ID for System DB

    // 3. Assert: All System Tables Exist in 'sys_tables'
    var tables = await ScanAllAsync(engine, StorageEngine.SYS_TABLES_TABLE_NAME);

    // Helper to find table ID by name
    int GetTableId(string name)
    {
      var row = tables.FirstOrDefault(r => r.Values[2].ToString() == name);
      Assert.True(row != null, $"System table '{name}' is missing from sys_tables.");
      Assert.Equal(systemDbId, row.Values[1].GetAs<int>()); // Must belong to System DB
      return row.Values[0].GetAs<int>();
    }

    var sysDatabasesTableId = GetTableId("sys_databases");
    var sysTablesTableId = GetTableId("sys_tables");
    var sysColumnsTableId = GetTableId("sys_columns");
    var sysConstraintsTableId = GetTableId("sys_constraints");

    // 4. Assert: Verify Column Definitions in 'sys_columns' for each table
    var allColumns = await ScanAllAsync(engine, StorageEngine.SYS_COLUMNS_TABLE_NAME);

    // Define expected schemas based on your JSON files
    VerifySchema(allColumns, sysDatabasesTableId, new[]
    {
            ("database_id", "Int"),
            ("database_name", "Varchar"),
            ("creation_date", "DateTime")
        });

    VerifySchema(allColumns, sysTablesTableId, new[]
    {
            ("table_id", "Int"),
            ("database_id", "Int"),
            ("table_name", "Varchar"),
            ("creation_date", "DateTime")
        });

    VerifySchema(allColumns, sysColumnsTableId, new[]
    {
            ("column_id", "Int"),
            ("table_id", "Int"),
            ("column_name", "Varchar"),
            ("data_type_info", "Varchar"),
            ("ordinal_position", "Int"),
            ("is_nullable", "Boolean"),
            ("default_value_expression", "Varchar")
        });

    VerifySchema(allColumns, sysConstraintsTableId, new[]
    {
            ("constraint_id", "Int"),
            ("table_id", "Int"),
            ("constraint_name", "Varchar"),
            ("constraint_type", "Varchar"),
            ("definition", "Varchar"),
            ("creation_date", "DateTime")
        });

    // 5. Assert: Verify Constraints in 'sys_constraints'
    var allConstraints = await ScanAllAsync(engine, StorageEngine.SYS_CONSTRAINTS_TABLE_NAME);

    VerifyConstraint(allConstraints, sysDatabasesTableId, "PK_sys_databases", "PrimaryKey");
    VerifyConstraint(allConstraints, sysDatabasesTableId, "UQ_sys_databases_name", "Unique");

    VerifyConstraint(allConstraints, sysTablesTableId, "PK_sys_tables", "PrimaryKey");
    VerifyConstraint(allConstraints, sysTablesTableId, "FK_sys_tables_database_id", "ForeignKey");
    VerifyConstraint(allConstraints, sysTablesTableId, "UQ_sys_tables_db_name", "Unique");

    VerifyConstraint(allConstraints, sysColumnsTableId, "PK_sys_columns", "PrimaryKey");
    VerifyConstraint(allConstraints, sysColumnsTableId, "FK_sys_columns_table_id", "ForeignKey");
    VerifyConstraint(allConstraints, sysColumnsTableId, "UQ_sys_columns_table_col", "Unique");
    VerifyConstraint(allConstraints, sysColumnsTableId, "UQ_sys_columns_table_ord", "Unique");

    VerifyConstraint(allConstraints, sysConstraintsTableId, "PK_sys_constraints", "PrimaryKey");
    VerifyConstraint(allConstraints, sysConstraintsTableId, "FK_sys_constraints_table_id", "ForeignKey");
    VerifyConstraint(allConstraints, sysConstraintsTableId, "UQ_sys_constraints_table_name", "Unique");
  }

  // --- Helpers ---

  private async Task<List<Record>> ScanAllAsync(IStorageEngine engine, string tableName)
  {
    var results = new List<Record>();
    await foreach (var row in engine.ScanAsync(tableName))
    {
      results.Add(row);
    }
    return results;
  }

  private void VerifySchema(List<Record> allColumns, int tableId, (string name, string typeSnippet)[] expectedCols)
  {
    // Filter columns for this specific table
    var tableCols = allColumns.Where(r => r.Values[1].GetAs<int>() == tableId)
                              .OrderBy(r => r.Values[4].GetAs<int>()) // Order by ordinal_position
                              .ToList();

    Assert.Equal(expectedCols.Length, tableCols.Count);

    for (int i = 0; i < expectedCols.Length; i++)
    {
      var actualName = tableCols[i].Values[2].ToString();
      var actualType = tableCols[i].Values[3].ToString(); // data_type_info

      Assert.Equal(expectedCols[i].name, actualName);
      Assert.Contains(expectedCols[i].typeSnippet, actualType, StringComparison.OrdinalIgnoreCase);
    }
  }

  private void VerifyConstraint(List<Record> allConstraints, int tableId, string constraintName, string constraintTypeSnippet)
  {
    var constraint = allConstraints.FirstOrDefault(r =>
        r.Values[1].GetAs<int>() == tableId &&
        r.Values[2].ToString() == constraintName);

    Assert.True(constraint != null, $"Missing constraint '{constraintName}' for table ID {tableId}");

    // Verify type (Index 3 in sys_constraints)
    Assert.Contains(constraintTypeSnippet, constraint.Values[3].ToString(), StringComparison.OrdinalIgnoreCase);
  }
}