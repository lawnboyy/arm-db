using System.Text.Json;
using ArmDb.SchemaDefinition;
using ArmDb.Common.Abstractions;
using ArmDb.Server.Bootstrap;

namespace ArmDb.UnitTests.Server; // Example test namespace

// Simple Stub/Mock for IFileSystem for testing purposes
public class MockFileSystem : IFileSystem
{
  public Dictionary<string, string> Files { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
  public HashSet<string> ExistingDirectories { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
  public Func<string, string, string> PathCombineFunc { get; set; } = Path.Combine; // Use real Path.Combine by default

  public bool DirectoryExists(string path) => ExistingDirectories.Contains(path);
  public bool FileExists(string path) => Files.ContainsKey(path);
  public Task<string> ReadAllTextAsync(string path)
  {
    if (Files.TryGetValue(path, out var content))
    {
      return Task.FromResult(content);
    }
    throw new FileNotFoundException($"Mock File Not Found: {path}");
  }
  public string CombinePath(string path1, string path2) => PathCombineFunc(path1, path2);

  // Helper to add a file for setup
  public void AddFile(string path, string content) => Files[path] = content;
  // Helper to add a directory
  public void AddDirectory(string path) => ExistingDirectories.Add(path);

  public void EnsureDirectoryExists(string path)
  {
    throw new NotImplementedException();
  }

  public Task<long> GetFileLengthAsync(string path)
  {
    throw new NotImplementedException();
  }

  public Task SetFileLengthAsync(string path, long length)
  {
    throw new NotImplementedException();
  }

  public Task<int> ReadFileAsync(string path, long fileOffset, Memory<byte> destination)
  {
    throw new NotImplementedException();
  }

  public Task WriteFileAsync(string path, long fileOffset, ReadOnlyMemory<byte> source)
  {
    throw new NotImplementedException();
  }

  public Task DeleteFileAsync(string path)
  {
    throw new NotImplementedException();
  }
}


public class SystemTableLoaderTests
{
  private const string DefDir = "/definitions"; // Example path

  // --- Sample Valid JSON ---
  // (Minimal valid structure for testing deserialization)
  private const string SysDatabasesJson = @"{ ""Name"": ""sys_databases"", ""Columns"": [], ""Constraints"": [] }";
  private const string SysColumnsJson = @"{ ""Name"": ""sys_columns"", ""Columns"": [], ""Constraints"": [] }";
  private const string SysConstraintsJson = @"{ ""Name"": ""sys_constraints"", ""Columns"": [], ""Constraints"": [] }";
  private const string SysTablesJson_Detailed = @"{
      ""Name"": ""sys_tables"",
      ""Columns"": [
        { ""Name"": ""table_id"", ""DataType"": { ""PrimitiveType"": ""Int"" }, ""IsNullable"": false, ""DefaultValueExpression"": null },
        { ""Name"": ""database_id"", ""DataType"": { ""PrimitiveType"": ""Int"" }, ""IsNullable"": false, ""DefaultValueExpression"": null },
        { ""Name"": ""table_name"", ""DataType"": { ""PrimitiveType"": ""Varchar"", ""MaxLength"": 128 }, ""IsNullable"": false, ""DefaultValueExpression"": null },
        { ""Name"": ""creation_date"", ""DataType"": { ""PrimitiveType"": ""DateTime"" }, ""IsNullable"": false, ""DefaultValueExpression"": null }
      ],
      ""Constraints"": [
        { ""ConstraintType"": ""PrimaryKey"", ""Name"": ""PK_sys_tables"", ""ColumnNames"": [ ""table_id"" ] },
        { ""ConstraintType"": ""ForeignKey"", ""Name"": ""FK_sys_tables_database_id"", ""ReferencingColumnNames"": [ ""database_id"" ], ""ReferencedTableName"": ""sys_databases"", ""ReferencedColumnNames"": [ ""database_id"" ], ""OnUpdateAction"": ""NoAction"", ""OnDeleteAction"": ""Cascade"" },
        { ""ConstraintType"": ""Unique"", ""Name"": ""UQ_sys_tables_db_name"", ""ColumnNames"": [ ""database_id"", ""table_name"" ] }
      ]
    }";


  [Fact]
  public async Task LoadCatalogDefinitionsAsync_DirectoryNotFound_ThrowsDirectoryNotFoundException()
  {
    // Arrange
    var mockFs = new MockFileSystem();
    // Ensure directory *does not* exist
    mockFs.ExistingDirectories.Remove(DefDir);

    // Act & Assert
    await Assert.ThrowsAsync<DirectoryNotFoundException>(() =>
        SystemTableLoader.LoadCatalogDefinitionsAsync(DefDir, mockFs)); // Pass mock FS
  }

  [Fact]
  public async Task LoadCatalogDefinitionsAsync_InvalidJson_ThrowsJsonException()
  {
    // Arrange
    var mockFs = new MockFileSystem();
    mockFs.AddDirectory(DefDir);
    mockFs.AddFile(mockFs.CombinePath(DefDir, "sys_databases.json"), SysDatabasesJson);
    mockFs.AddFile(mockFs.CombinePath(DefDir, "sys_tables.json"), @"{ ""Name"": ""sys_tables"", ""Columns"": [ INVALID JSON }"); // Malformed JSON
    mockFs.AddFile(mockFs.CombinePath(DefDir, "sys_columns.json"), SysColumnsJson);
    mockFs.AddFile(mockFs.CombinePath(DefDir, "sys_constraints.json"), SysConstraintsJson);

    // Act & Assert
    await Assert.ThrowsAsync<JsonException>(() =>
        SystemTableLoader.LoadCatalogDefinitionsAsync(DefDir, mockFs));
  }

  [Fact]
  public async Task LoadCatalogDefinitionsAsync_ValidDetailedFile_DeserializesColumnsAndConstraints()
  {
    // Arrange
    var mockFs = new MockFileSystem();
    mockFs.AddDirectory(DefDir);
    // Provide minimal valid JSON for files we aren't focusing on in this test
    mockFs.AddFile(mockFs.CombinePath(DefDir, "sys_databases.json"), SysDatabasesJson);
    mockFs.AddFile(mockFs.CombinePath(DefDir, "sys_columns.json"), SysColumnsJson);
    mockFs.AddFile(mockFs.CombinePath(DefDir, "sys_constraints.json"), SysConstraintsJson);
    // Provide the detailed JSON for the file under test
    mockFs.AddFile(mockFs.CombinePath(DefDir, "sys_tables.json"), SysTablesJson_Detailed);

    // Act
    var result = await SystemTableLoader.LoadCatalogDefinitionsAsync(DefDir, mockFs);

    // Assert
    Assert.NotNull(result);
    // Ensure all 4 files were processed (even if some were minimal)
    Assert.Equal(4, result.Count);

    // Find the specific TableDefinition we provided detailed JSON for
    var sysTablesDef = result.FirstOrDefault(td => td.Name.Equals("sys_tables", StringComparison.OrdinalIgnoreCase));
    Assert.NotNull(sysTablesDef); // Ensure sys_tables was found

    // --- Assert Columns ---
    Assert.Equal(4, sysTablesDef.Columns.Count);

    // Spot check a column (e.g., table_name)
    var nameCol = sysTablesDef.GetColumn("table_name"); // Use helper method
    Assert.NotNull(nameCol);
    Assert.Equal("table_name", nameCol.Name);
    Assert.False(nameCol.IsNullable);
    Assert.NotNull(nameCol.DataType);
    Assert.Equal(PrimitiveDataType.Varchar, nameCol.DataType.PrimitiveType);
    Assert.Equal(128, nameCol.DataType.MaxLength);
    Assert.Null(nameCol.DefaultValueExpression);

    // Spot check another column (e.g., table_id)
    var idCol = sysTablesDef.GetColumn("table_id");
    Assert.NotNull(idCol);
    Assert.Equal("table_id", idCol.Name);
    Assert.False(idCol.IsNullable);
    Assert.NotNull(idCol.DataType);
    Assert.Equal(PrimitiveDataType.Int, idCol.DataType.PrimitiveType);
    Assert.Null(idCol.DataType.MaxLength); // Check omission worked


    // --- Assert Constraints ---
    Assert.Equal(3, sysTablesDef.Constraints.Count);

    // Check Primary Key
    var pk = sysTablesDef.GetPrimaryKeyConstraint(); // Use helper
    Assert.NotNull(pk);
    Assert.IsType<PrimaryKeyConstraint>(pk); // Verify type
    Assert.Equal("PK_sys_tables", pk.Name);
    Assert.Single(pk.ColumnNames);
    Assert.Equal("table_id", pk.ColumnNames[0]);

    // Check Foreign Key
    var fk = sysTablesDef.Constraints.OfType<ForeignKeyConstraint>().FirstOrDefault(c => c.Name == "FK_sys_tables_database_id");
    Assert.NotNull(fk);
    Assert.Single(fk.ReferencingColumnNames);
    Assert.Equal("database_id", fk.ReferencingColumnNames[0]);
    Assert.Equal("sys_databases", fk.ReferencedTableName);
    Assert.Single(fk.ReferencedColumnNames);
    Assert.Equal("database_id", fk.ReferencedColumnNames[0]);
    Assert.Equal(ReferentialAction.NoAction, fk.OnUpdateAction);
    Assert.Equal(ReferentialAction.Cascade, fk.OnDeleteAction);

    // Check Unique Constraint
    var uq = sysTablesDef.Constraints.OfType<UniqueConstraint>().FirstOrDefault(c => c.Name == "UQ_sys_tables_db_name");
    Assert.NotNull(uq);
    Assert.Equal(2, uq.ColumnNames.Count);
    Assert.Equal("database_id", uq.ColumnNames[0]);
    Assert.Equal("table_name", uq.ColumnNames[1]);
  }

  [Fact]
  public async Task LoadCatalogDefinitionsAsync_FileReadError_ThrowsException()
  {
    // Arrange
    var mockFs = new MockFileSystem();
    mockFs.AddDirectory(DefDir);
    mockFs.AddFile(mockFs.CombinePath(DefDir, "sys_databases.json"), SysDatabasesJson);

    // Act & Assert
    // We expect the inner exception to be wrapped
    var outerEx = await Assert.ThrowsAsync<FileNotFoundException>(() =>
        SystemTableLoader.LoadCatalogDefinitionsAsync(DefDir, mockFs));
    Assert.Contains("Required catalog definition file not found", outerEx.Message);
  }
}