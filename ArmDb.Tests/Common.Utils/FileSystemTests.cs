using ArmDb.Common.Utils;

namespace ArmDb.IntegrationTests.Common.Utils;

public class FileSystemTests : IDisposable
{
  private readonly FileSystem _fileSystem;
  private readonly string _testDirectory;
  private readonly byte[] _sampleData;

  // Setup: Create a unique temporary directory for each test instance
  public FileSystemTests()
  {
    _fileSystem = new FileSystem();
    // Create a unique subdirectory in the system temp path for test files
    _testDirectory = Path.Combine(Path.GetTempPath(), $"ArmDbTest_{Guid.NewGuid()}");
    Directory.CreateDirectory(_testDirectory);

    // Create sample data (e.g., 256 bytes with values 0-255)
    _sampleData = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
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

  [Fact]
  public async Task ReadFileAsync_ReadFullFile_ReturnsCorrectDataAndCount()
  {
    // Arrange
    var filePath = GetTestFilePath("full_read.bin");
    await CreateTestFileAsync(filePath, _sampleData);
    var buffer = new Memory<byte>(new byte[_sampleData.Length]);

    // Act
    int bytesRead = await _fileSystem.ReadFileAsync(filePath, 0, buffer);

    // Assert
    Assert.Equal(_sampleData.Length, bytesRead);
    Assert.True(buffer.Span.SequenceEqual(_sampleData)); // Verify content matches
  }

  [Fact]
  public async Task ReadFileAsync_ReadPartialStart_ReturnsCorrectDataAndCount()
  {
    // Arrange
    var filePath = GetTestFilePath("partial_start.bin");
    await CreateTestFileAsync(filePath, _sampleData);
    int bytesToRead = 50;
    var buffer = new Memory<byte>(new byte[bytesToRead]);
    var expectedData = _sampleData.AsSpan(0, bytesToRead).ToArray();

    // Act
    int bytesRead = await _fileSystem.ReadFileAsync(filePath, 0, buffer);

    // Assert
    Assert.Equal(bytesToRead, bytesRead);
    Assert.True(buffer.Span.SequenceEqual(expectedData));
  }

  [Fact]
  public async Task ReadFileAsync_ReadPartialMiddle_ReturnsCorrectDataAndCount()
  {
    // Arrange
    var filePath = GetTestFilePath("partial_middle.bin");
    await CreateTestFileAsync(filePath, _sampleData);
    int offset = 100;
    int bytesToRead = 64;
    var buffer = new Memory<byte>(new byte[bytesToRead]);
    var expectedData = _sampleData.AsSpan(offset, bytesToRead).ToArray();

    // Act
    int bytesRead = await _fileSystem.ReadFileAsync(filePath, offset, buffer);

    // Assert
    Assert.Equal(bytesToRead, bytesRead);
    Assert.True(buffer.Span.SequenceEqual(expectedData));
  }

  [Fact]
  public async Task ReadFileAsync_ReadPastEndOfFile_ReturnsPartialDataAndCorrectCount()
  {
    // Arrange
    var filePath = GetTestFilePath("read_past_eof.bin");
    // Use only first 150 bytes for this file
    var fileContent = _sampleData.AsSpan(0, 150).ToArray();
    await CreateTestFileAsync(filePath, fileContent);

    int offset = 100; // Start reading at offset 100
    int bufferSize = 100; // Try to read 100 bytes
    int expectedBytesRead = 50; // Only 50 bytes remain (150 - 100)
    var buffer = new Memory<byte>(new byte[bufferSize]);
    buffer.Span.Fill(0xEE); // Fill buffer with a known value (0xEE) so we can check that the rest of the buffer is unchanged.
    var expectedData = fileContent.AsSpan(offset, expectedBytesRead).ToArray();

    // Act
    int bytesRead = await _fileSystem.ReadFileAsync(filePath, offset, buffer);

    // Assert
    Assert.Equal(expectedBytesRead, bytesRead);
    // Check only the part of the buffer that should have been written
    Assert.True(buffer.Span.Slice(0, bytesRead).SequenceEqual(expectedData));
    // Check that the rest of the buffer wasn't overwritten
    Assert.Equal((byte)0xEE, buffer.Span[bytesRead]);
  }

  [Fact]
  public async Task ReadFileAsync_ReadAtEndOfFile_ReturnsZeroBytes()
  {
    // Arrange
    var filePath = GetTestFilePath("read_at_eof.bin");
    await CreateTestFileAsync(filePath, _sampleData);
    long offset = _sampleData.Length; // Start reading exactly at EOF
    var buffer = new Memory<byte>(new byte[10]);

    // Act
    int bytesRead = await _fileSystem.ReadFileAsync(filePath, offset, buffer);

    // Assert
    Assert.Equal(0, bytesRead);
  }

  [Fact]
  public async Task ReadFileAsync_ReadBeyondEndOfFile_ReturnsZeroBytes()
  {
    // Arrange
    var filePath = GetTestFilePath("read_beyond_eof.bin");
    await CreateTestFileAsync(filePath, _sampleData);
    long offset = _sampleData.Length + 100; // Start reading past EOF
    var buffer = new Memory<byte>(new byte[10]);

    // Act
    int bytesRead = await _fileSystem.ReadFileAsync(filePath, offset, buffer);

    // Assert
    Assert.Equal(0, bytesRead);
  }

  [Fact]
  public async Task ReadFileAsync_ReadZeroBytes_ReturnsZeroBytes()
  {
    // Arrange
    var filePath = GetTestFilePath("read_zero_bytes.bin");
    await CreateTestFileAsync(filePath, _sampleData);
    long offset = 10;
    var buffer = Memory<byte>.Empty; // Request to read into empty buffer

    // Act
    int bytesRead = await _fileSystem.ReadFileAsync(filePath, offset, buffer);

    // Assert
    Assert.Equal(0, bytesRead);
  }

  [Fact]
  public async Task ReadFileAsync_NegativeOffset_ThrowsArgumentOutOfRangeException()
  {
    // Arrange
    var filePath = GetTestFilePath("negative_offset_read.bin");
    await CreateTestFileAsync(filePath, _sampleData);
    long offset = -1;
    var buffer = new Memory<byte>(new byte[10]);

    // Act & Assert
    await Assert.ThrowsAsync<ArgumentOutOfRangeException>("fileOffset", () =>
        _fileSystem.ReadFileAsync(filePath, offset, buffer));
  }

  [Fact]
  public async Task ReadFileAsync_FileDoesNotExist_ThrowsFileNotFoundException()
  {
    // Arrange
    var filePath = GetTestFilePath("not_found_read.bin");
    long offset = 0;
    var buffer = new Memory<byte>(new byte[10]);
    Assert.False(File.Exists(filePath)); // Ensure file doesn't exist

    // Act & Assert
    await Assert.ThrowsAsync<FileNotFoundException>(() =>
       _fileSystem.ReadFileAsync(filePath, offset, buffer));
  }

  [Fact]
  public async Task ReadFileAsync_PathIsDirectory_ThrowsUnauthorizedAccessExceptionOrIOException()
  {
    // Arrange
    var dirPath = GetTestFilePath("a_read_directory");
    Directory.CreateDirectory(dirPath); // Create a directory
    long offset = 0;
    var buffer = new Memory<byte>(new byte[10]);

    // Act & Assert
    // Reading a directory usually fails with UnauthorizedAccessException or IOException
    await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() =>
       _fileSystem.ReadFileAsync(dirPath, offset, buffer));
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

  private string GetTestFilePath(string fileName)
  {
    return Path.Combine(_testDirectory, fileName);
  }

  private async Task CreateTestFileAsync(string path, byte[] content)
  {
    // Use standard BCL File operations for reliable test setup
    await File.WriteAllBytesAsync(path, content);
  }
}