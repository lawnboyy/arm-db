using ArmDb.Common.Utils;

namespace ArmDb.IntegrationTests.Common.Utils;

public class FileSystemTests : IDisposable
{
  private readonly FileSystem _fileSystem;
  private readonly string _testDirectory;

  // Setup: Create a unique temporary directory for each test instance
  public FileSystemTests()
  {
    _fileSystem = new FileSystem();
    // Create a unique subdirectory in the system temp path for test files
    _testDirectory = Path.Combine(Path.GetTempPath(), $"ArmDbTest_{Guid.NewGuid()}");
    Directory.CreateDirectory(_testDirectory); // Use sync for setup simplicity
  }

  // Helper to get a unique file path within the test directory
  private string GetTestFilePath(string fileName)
  {
    return Path.Combine(_testDirectory, fileName);
  }

  [Fact]
  public async Task SetFileLengthAsync_ExtendsEmptyFile_SetsCorrectLength()
  {
    // Arrange
    var filePath = GetTestFilePath("extend_empty.tmp");
    File.Create(filePath).Dispose(); // Ensure empty file exists
    long targetLength = 2048;

    // Act
    await _fileSystem.SetFileLengthAsync(filePath, targetLength);

    // Assert
    var fileInfo = new FileInfo(filePath);
    fileInfo.Refresh(); // Ensure we get the latest info
    Assert.True(fileInfo.Exists);
    Assert.Equal(targetLength, fileInfo.Length);
  }

  [Fact]
  public async Task SetFileLengthAsync_ExtendsExistingFile_SetsCorrectLength()
  {
    // Arrange
    var filePath = GetTestFilePath("extend_existing.tmp");
    await File.WriteAllBytesAsync(filePath, new byte[100]); // Start with 100 bytes
    long targetLength = 512;

    // Act
    await _fileSystem.SetFileLengthAsync(filePath, targetLength);

    // Assert
    var fileInfo = new FileInfo(filePath);
    fileInfo.Refresh();
    Assert.True(fileInfo.Exists);
    Assert.Equal(targetLength, fileInfo.Length);
  }

  [Fact]
  public async Task SetFileLengthAsync_TruncatesExistingFile_SetsCorrectLength()
  {
    // Arrange
    var filePath = GetTestFilePath("truncate.tmp");
    await File.WriteAllBytesAsync(filePath, new byte[1000]); // Start with 1000 bytes
    long targetLength = 123;

    // Act
    await _fileSystem.SetFileLengthAsync(filePath, targetLength);

    // Assert
    var fileInfo = new FileInfo(filePath);
    fileInfo.Refresh();
    Assert.True(fileInfo.Exists);
    Assert.Equal(targetLength, fileInfo.Length);

    // Double-check by reading
    var content = await File.ReadAllBytesAsync(filePath);
    Assert.Equal(targetLength, content.Length);
  }

  [Fact]
  public async Task SetFileLengthAsync_SetToZero_SetsCorrectLength()
  {
    // Arrange
    var filePath = GetTestFilePath("zero.tmp");
    await File.WriteAllBytesAsync(filePath, new byte[50]); // Start with 50 bytes
    long targetLength = 0;

    // Act
    await _fileSystem.SetFileLengthAsync(filePath, targetLength);

    // Assert
    var fileInfo = new FileInfo(filePath);
    fileInfo.Refresh();
    Assert.True(fileInfo.Exists);
    Assert.Equal(targetLength, fileInfo.Length);
  }

  [Fact]
  public async Task SetFileLengthAsync_NegativeLength_ThrowsArgumentOutOfRangeException()
  {
    // Arrange
    var filePath = GetTestFilePath("negative_len.tmp");
    File.Create(filePath).Dispose();
    long targetLength = -1;

    // Act & Assert
    // The ArgumentOutOfRangeException might be thrown by the method itself or
    // by the underlying FileStream.SetLength inside Task.Run.
    await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
        _fileSystem.SetFileLengthAsync(filePath, targetLength));
  }

  [Fact]
  public async Task SetFileLengthAsync_FileDoesNotExist_ThrowsFileNotFoundException()
  {
    // Arrange
    var filePath = GetTestFilePath("not_exist.tmp");
    long targetLength = 100;
    Assert.False(File.Exists(filePath)); // Pre-condition

    // Act & Assert
    // Exception comes from FileStream constructor inside Task.Run
    await Assert.ThrowsAsync<FileNotFoundException>(() =>
        _fileSystem.SetFileLengthAsync(filePath, targetLength));
  }

  [Fact]
  public async Task SetFileLengthAsync_PathIsDirectory_ThrowsUnauthorizedAccessExceptionOrIOException()
  {
    // Arrange
    var dirPath = GetTestFilePath("a_directory_path"); // Using method to get path in test dir
    Directory.CreateDirectory(dirPath); // Create the directory
    long targetLength = 100;

    // Act & Assert
    // Trying to open a directory with write access usually fails.
    // The exact exception can sometimes vary by OS or exact circumstances,
    // UnauthorizedAccessException is common on Windows, IOException might occur too.
    await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() => // Check for base IOException or derived types like UnauthorizedAccessException
       _fileSystem.SetFileLengthAsync(dirPath, targetLength));
  }


  // Teardown: Clean up the temporary directory and its contents
  public void Dispose()
  {
    try
    {
      if (Directory.Exists(_testDirectory))
      {
        Directory.Delete(_testDirectory, recursive: true);
      }
    }
    catch (Exception ex)
    {
      Console.WriteLine($"Error cleaning up test directory '{_testDirectory}': {ex.Message}");
      // Don't let cleanup exceptions fail the test run itself usually
    }
  }
}