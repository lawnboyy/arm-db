using ArmDb.Storage;
using static ArmDb.UnitTests.StorageEngine.StorageEngineTestHelper;

namespace ArmDb.UnitTests.StorageEngine; // Your test project's namespace

// Note: These are integration tests hitting the real file system.
public partial class DiskManagerTests : IDisposable
{
  [Fact]
  public async Task WriteDiskPageAsync_WritePageZeroToNewFile_CreatesFileAndWritesData()
  {
    // Arrange
    int tableId = 101;
    var pageId = new PageId(tableId, 0);
    var filePath = GetExpectedTablePath(tableId);
    var bufferToWrite = CreateTestBuffer(0xA1);
    Assert.False(_fileSystem.FileExists(filePath));

    // Act
    await _diskManager.WriteDiskPageAsync(pageId, bufferToWrite.AsMemory());

    // Assert
    Assert.True(_fileSystem.FileExists(filePath));
    Assert.Equal(PageSize, await _fileSystem.GetFileLengthAsync(filePath));
    var dataRead = await ReadPageDirectlyAsync(filePath, 0);
    Assert.True(bufferToWrite.SequenceEqual(dataRead));
  }

  [Fact]
  public async Task WriteDiskPageAsync_WritePageOneToNewFile_CreatesFileWithGap()
  {
    // Arrange
    int tableId = 102;
    var pageId = new PageId(tableId, 1); // Write the *second* page (index 1)
    var filePath = GetExpectedTablePath(tableId);
    var bufferToWrite = CreateTestBuffer(0xB2);
    var expectedZeroPage = new byte[PageSize]; // Expect page 0 to be zero-padded
    Assert.False(_fileSystem.FileExists(filePath));

    // Act
    await _diskManager.WriteDiskPageAsync(pageId, bufferToWrite.AsMemory());

    // Assert
    Assert.True(_fileSystem.FileExists(filePath));
    Assert.Equal(PageSize * 2, await _fileSystem.GetFileLengthAsync(filePath)); // File extended to end of page 1

    var page0Data = await ReadPageDirectlyAsync(filePath, 0);
    Assert.True(expectedZeroPage.SequenceEqual(page0Data), "Page 0 (gap) should be zero-filled");

    var page1Data = await ReadPageDirectlyAsync(filePath, 1);
    Assert.True(bufferToWrite.SequenceEqual(page1Data), "Page 1 content mismatch");
  }

  [Fact]
  public async Task WriteDiskPageAsync_OverwriteExistingPageZero_UpdatesContent()
  {
    // Arrange
    int tableId = 103;
    var pageId = new PageId(tableId, 0);
    var filePath = GetExpectedTablePath(tableId);
    var initialBuffer = CreateTestBuffer(0xC3);
    var bufferToWrite = CreateTestBuffer(0xD3);
    // Create file with initial data using the method under test or a helper
    await _diskManager.WriteDiskPageAsync(pageId, initialBuffer.AsMemory());
    Assert.Equal(PageSize, await _fileSystem.GetFileLengthAsync(filePath));

    // Act
    await _diskManager.WriteDiskPageAsync(pageId, bufferToWrite.AsMemory()); // Overwrite

    // Assert
    Assert.Equal(PageSize, await _fileSystem.GetFileLengthAsync(filePath)); // Length should remain same
    var dataRead = await ReadPageDirectlyAsync(filePath, 0);
    Assert.True(bufferToWrite.SequenceEqual(dataRead)); // Content should be the new data
  }

  [Fact]
  public async Task WriteDiskPageAsync_OverwriteExistingMiddlePage_UpdatesContentAffectsOnlyThatPage()
  {
    // Arrange
    int tableId = 104;
    var pageId0 = new PageId(tableId, 0);
    var pageId1 = new PageId(tableId, 1); // Modify Page 1
    var pageId2 = new PageId(tableId, 2);
    var filePath = GetExpectedTablePath(tableId);
    var page0Buffer = CreateTestBuffer(0xA4);
    var initialPage1Buffer = CreateTestBuffer(0xB4);
    var page2Buffer = CreateTestBuffer(0xC4);
    var bufferToWrite = CreateTestBuffer(0xD4); // New data for Page 1

    // Create 3-page file
    await _diskManager.WriteDiskPageAsync(pageId0, page0Buffer.AsMemory());
    await _diskManager.WriteDiskPageAsync(pageId1, initialPage1Buffer.AsMemory());
    await _diskManager.WriteDiskPageAsync(pageId2, page2Buffer.AsMemory());
    Assert.Equal(PageSize * 3, await _fileSystem.GetFileLengthAsync(filePath));

    // Act
    await _diskManager.WriteDiskPageAsync(pageId1, bufferToWrite.AsMemory()); // Overwrite page 1

    // Assert
    Assert.Equal(PageSize * 3, await _fileSystem.GetFileLengthAsync(filePath)); // Length unchanged

    var dataPage0 = await ReadPageDirectlyAsync(filePath, 0);
    var dataPage1 = await ReadPageDirectlyAsync(filePath, 1);
    var dataPage2 = await ReadPageDirectlyAsync(filePath, 2);

    Assert.True(page0Buffer.SequenceEqual(dataPage0), "Page 0 mismatch");
    Assert.True(bufferToWrite.SequenceEqual(dataPage1), "Page 1 mismatch");
    Assert.True(page2Buffer.SequenceEqual(dataPage2), "Page 2 mismatch");
  }

  [Fact]
  public async Task WriteDiskPageAsync_AppendNewPage_ExtendsFileCorrectly()
  {
    // Arrange
    int tableId = 105;
    var initialPageId = new PageId(tableId, 0);
    var newPageId = new PageId(tableId, 1); // Append Page 1
    var filePath = GetExpectedTablePath(tableId);
    var page0Buffer = CreateTestBuffer(0xE5);
    var page1Buffer = CreateTestBuffer(0xF5);

    // Write initial page 0
    await _diskManager.WriteDiskPageAsync(initialPageId, page0Buffer.AsMemory());
    Assert.Equal(PageSize, await _fileSystem.GetFileLengthAsync(filePath));

    // Act
    await _diskManager.WriteDiskPageAsync(newPageId, page1Buffer.AsMemory()); // Write page 1

    // Assert
    Assert.Equal(PageSize * 2, await _fileSystem.GetFileLengthAsync(filePath)); // Length should be 2 pages
    var dataPage0 = await ReadPageDirectlyAsync(filePath, 0);
    var dataPage1 = await ReadPageDirectlyAsync(filePath, 1);
    Assert.True(page0Buffer.SequenceEqual(dataPage0));
    Assert.True(page1Buffer.SequenceEqual(dataPage1));
  }


  [Fact]
  public async Task WriteDiskPageAsync_BufferNotPageSize_ThrowsArgumentException()
  {
    // Arrange
    int tableId = 106;
    var pageId = new PageId(tableId, 0);
    var filePath = GetExpectedTablePath(tableId);
    // Ensure file exists for write attempt, content doesn't matter
    await _diskManager.CreateTableFileAsync(tableId); // Use method to ensure file exists

    var smallBuffer = new ReadOnlyMemory<byte>(new byte[PageSize - 1]);
    var largeBuffer = new ReadOnlyMemory<byte>(new byte[PageSize + 1]);

    // Act & Assert
    await Assert.ThrowsAsync<ArgumentException>("buffer", () => _diskManager.WriteDiskPageAsync(pageId, smallBuffer));
    await Assert.ThrowsAsync<ArgumentException>("buffer", () => _diskManager.WriteDiskPageAsync(pageId, largeBuffer));
  }

  [Fact]
  public async Task WriteDiskPageAsync_PathIsDirectory_ThrowsUnauthorizedAccessExceptionOrIOException()
  {
    // Arrange
    int tableId = 107;
    var pageId = new PageId(tableId, 0);
    var dirPath = GetExpectedTablePath(tableId); // Use path where file would be
    Directory.CreateDirectory(dirPath); // Create directory instead of file
    var bufferToWrite = CreateTestBuffer(0x17);

    // Act & Assert
    // Trying to open/write to a directory usually fails this way
    await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() =>
       _diskManager.WriteDiskPageAsync(pageId, bufferToWrite.AsMemory()));
  }
}