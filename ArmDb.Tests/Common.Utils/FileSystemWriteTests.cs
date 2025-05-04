using System.Text;

namespace ArmDb.IntegrationTests.Common.Utils;

public partial class FileSystemTests
{
  private readonly byte[] _sampleData1 = Encoding.UTF8.GetBytes("Hello World!");
  private readonly byte[] _sampleData2 = Encoding.UTF8.GetBytes("OVERWRITE");

  [Fact]
  public async Task WriteFileAsync_WriteToNewFile_OffsetZero_CreatesAndWrites()
  {
    // Arrange
    var filePath = GetTestFilePath("new_zero.bin");
    Assert.False(File.Exists(filePath)); // Ensure doesn't exist

    // Act
    await _fileSystem.WriteFileAsync(filePath, 0, _sampleData1.AsMemory());

    // Assert
    Assert.True(File.Exists(filePath));
    var writtenData = await ReadTestFileBytesAsync(filePath);
    var fileInfo = new FileInfo(filePath);
    Assert.Equal(_sampleData1.Length, fileInfo.Length);
    Assert.Equal(_sampleData1, writtenData);
  }

  [Fact]
  public async Task WriteFileAsync_WriteToNewFile_NonZeroOffset_CreatesPaddedFile()
  {
    // Arrange
    var filePath = GetTestFilePath("new_offset.bin");
    long offset = 100;
    long expectedLength = offset + _sampleData1.Length;
    Assert.False(File.Exists(filePath));

    // Act
    await _fileSystem.WriteFileAsync(filePath, offset, _sampleData1.AsMemory());

    // Assert
    Assert.True(File.Exists(filePath));
    var writtenData = await ReadTestFileBytesAsync(filePath);
    var fileInfo = new FileInfo(filePath);
    Assert.Equal(expectedLength, fileInfo.Length); // Check length
                                                   // Check padding is zeros
    Assert.All(writtenData.Take((int)offset), b => Assert.Equal(0, b));
    // Check data written at offset
    Assert.True(writtenData.AsSpan((int)offset).SequenceEqual(_sampleData1));
  }

  [Fact]
  public async Task WriteFileAsync_OverwriteExistingFile_OffsetZero_ReplacesContent()
  {
    // Arrange
    var filePath = GetTestFilePath("overwrite_zero.bin");
    var initialContent = Encoding.UTF8.GetBytes("Initial Data"); // 12 bytes
    await File.WriteAllBytesAsync(filePath, initialContent); // Use helper if available

    var dataToWrite = _sampleData2; // 8 bytes
    long expectedLength = initialContent.Length; // Expect length to REMAIN 12

    // Act
    await _fileSystem.WriteFileAsync(filePath, 0, dataToWrite.AsMemory());

    // Assert
    var finalData = await File.ReadAllBytesAsync(filePath); // Read result for verification
    var fileInfo = new FileInfo(filePath);
    fileInfo.Refresh();

    // 1. Verify length did NOT change
    Assert.Equal(expectedLength, fileInfo.Length);
    Assert.Equal(expectedLength, finalData.Length);

    // 2. Verify the beginning was overwritten
    Assert.True(finalData.AsSpan(0, dataToWrite.Length).SequenceEqual(dataToWrite));

    // 3. Verify the end bytes were NOT truncated and remain from the original content
    var remainingOriginalBytes = initialContent.AsSpan(dataToWrite.Length);
    Assert.True(finalData.AsSpan(dataToWrite.Length).SequenceEqual(remainingOriginalBytes));
  }

  [Fact]
  public async Task WriteFileAsync_OverwritePartialMiddle_ReplacesCorrectSegment()
  {
    // Arrange
    var filePath = GetTestFilePath("overwrite_partial.bin");
    var initialData = Enumerable.Repeat((byte)'A', 100).ToArray();
    await CreateTestFileAsync(filePath, initialData); // File with 100 'A's
    int offset = 40;
    var dataToWrite = Encoding.UTF8.GetBytes("REPLACED"); // 8 bytes

    // Act
    await _fileSystem.WriteFileAsync(filePath, offset, dataToWrite.AsMemory());

    // Assert
    var finalData = await ReadTestFileBytesAsync(filePath);
    var fileInfo = new FileInfo(filePath);
    Assert.Equal(initialData.Length, fileInfo.Length); // Length shouldn't change
                                                       // Check beginning untouched
    Assert.True(finalData.AsSpan(0, offset).SequenceEqual(initialData.AsSpan(0, offset)));
    // Check overwritten section
    Assert.True(finalData.AsSpan(offset, dataToWrite.Length).SequenceEqual(dataToWrite));
    // Check end untouched
    int endOffset = offset + dataToWrite.Length;
    Assert.True(finalData.AsSpan(endOffset).SequenceEqual(initialData.AsSpan(endOffset)));
  }

  [Fact]
  public async Task WriteFileAsync_AppendToExistingFile_ExtendsFile()
  {
    // Arrange
    var filePath = GetTestFilePath("append.bin");
    await CreateTestFileAsync(filePath, _sampleData1);
    long offset = _sampleData1.Length; // Offset is current EOF
    var dataToAppend = _sampleData2;
    var expectedData = _sampleData1.Concat(dataToAppend).ToArray();
    long expectedLength = expectedData.Length;

    // Act
    await _fileSystem.WriteFileAsync(filePath, offset, dataToAppend.AsMemory());

    // Assert
    var finalData = await ReadTestFileBytesAsync(filePath);
    var fileInfo = new FileInfo(filePath);
    Assert.Equal(expectedLength, fileInfo.Length);
    Assert.Equal(expectedData, finalData);
  }

  [Fact]
  public async Task WriteFileAsync_WriteBeyondEndOfFile_ExtendsAndPadsFile()
  {
    // Arrange
    var filePath = GetTestFilePath("extend_gap.bin");
    await CreateTestFileAsync(filePath, _sampleData1);
    long initialLength = _sampleData1.Length;
    long offset = initialLength + 50; // Write 50 bytes past current EOF
    var dataToWrite = _sampleData2;
    long expectedLength = offset + dataToWrite.Length;

    // Act
    await _fileSystem.WriteFileAsync(filePath, offset, dataToWrite.AsMemory());

    // Assert
    var finalData = await ReadTestFileBytesAsync(filePath);
    var fileInfo = new FileInfo(filePath);
    Assert.Equal(expectedLength, fileInfo.Length);
    // Check original data
    Assert.True(finalData.AsSpan(0, (int)initialLength).SequenceEqual(_sampleData1));
    // Check padding (gap) is zeros
    Assert.All(finalData.AsSpan((int)initialLength, (int)(offset - initialLength)).ToArray(), b => Assert.Equal(0, b));
    // Check newly written data
    Assert.True(finalData.AsSpan((int)offset).SequenceEqual(dataToWrite));
  }

  [Fact]
  public async Task WriteFileAsync_WriteEmptySpan_DoesNotChangeFile()
  {
    // Arrange
    var filePath = GetTestFilePath("write_empty.bin");
    await CreateTestFileAsync(filePath, _sampleData1);
    long initialLength = _sampleData1.Length;
    var initialData = _sampleData1.ToArray(); // Copy before act

    // Act
    await _fileSystem.WriteFileAsync(filePath, 10, ReadOnlyMemory<byte>.Empty); // Write empty at offset 10
    await _fileSystem.WriteFileAsync(filePath, initialLength, ReadOnlyMemory<byte>.Empty); // Write empty at EOF

    // Assert
    var finalData = await ReadTestFileBytesAsync(filePath);
    var fileInfo = new FileInfo(filePath);
    Assert.Equal(initialLength, fileInfo.Length); // Length unchanged
    Assert.Equal(initialData, finalData); // Content unchanged
  }

  [Fact]
  public async Task WriteFileAsync_WriteEmptySpanToNewFile_CreatesEmptyFile()
  {
    // Arrange
    var filePath = GetTestFilePath("write_empty_new.bin");
    Assert.False(File.Exists(filePath));

    // Act
    await _fileSystem.WriteFileAsync(filePath, 0, ReadOnlyMemory<byte>.Empty);

    // Assert
    Assert.True(File.Exists(filePath));
    var fileInfo = new FileInfo(filePath);
    Assert.Equal(0, fileInfo.Length);
  }

  [Fact]
  public async Task WriteFileAsync_NegativeOffset_ThrowsArgumentOutOfRangeException()
  {
    // Arrange
    var filePath = GetTestFilePath("negative_offset_write.bin");
    // File doesn't need to exist as offset check should happen first

    // Act & Assert
    await Assert.ThrowsAsync<ArgumentOutOfRangeException>("fileOffset", () =>
        _fileSystem.WriteFileAsync(filePath, -1, _sampleData1.AsMemory()));
  }

  [Fact]
  public async Task WriteFileAsync_PathIsDirectory_ThrowsUnauthorizedAccessExceptionOrIOException()
  {
    // Arrange
    var dirPath = GetTestFilePath("a_write_directory");
    Directory.CreateDirectory(dirPath); // Create a directory

    // Act & Assert
    // Writing to a directory usually fails with UnauthorizedAccessException or IOException
    await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() =>
       _fileSystem.WriteFileAsync(dirPath, 0, _sampleData1.AsMemory()));
  }
}