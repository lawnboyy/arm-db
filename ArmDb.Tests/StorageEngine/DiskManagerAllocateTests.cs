using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class DiskManagerTests : IDisposable
{
  [Fact]
  public async Task AllocateNewDiskPageAsync_ForNewTable_ReturnsPageIndexZeroAndExtendsFile()
  {
    // Arrange
    int tableId = 201;
    var filePath = GetExpectedTablePath(tableId);
    Assert.False(_fileSystem.FileExists(filePath)); // Ensure file doesn't exist initially

    // Act
    PageId newPageId = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    Assert.Equal(tableId, newPageId.TableId);
    Assert.Equal(0, newPageId.PageIndex); // First page should be index 0

    // Verify file was created and extended to hold one page
    Assert.True(_fileSystem.FileExists(filePath));
    Assert.Equal(PageSize, await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_ForExistingOnePageTable_ReturnsPageIndexOneAndExtendsFile()
  {
    // Arrange
    int tableId = 202;
    // Create a file with exactly one page
    await CreateTestTableFileAsync(tableId, 1, p => (byte)p);
    var filePath = GetExpectedTablePath(tableId);
    Assert.Equal(PageSize, await _fileSystem.GetFileLengthAsync(filePath));

    // Act
    PageId newPageId = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    Assert.Equal(tableId, newPageId.TableId);
    Assert.Equal(1, newPageId.PageIndex); // Next page should be index 1

    // Verify file was extended to hold two pages
    Assert.Equal(PageSize * 2, await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_ForExistingMultiPageTable_ReturnsNextPageIndexAndExtendsFile()
  {
    // Arrange
    int tableId = 203;
    int initialPages = 5;
    await CreateTestTableFileAsync(tableId, initialPages, p => (byte)p);
    var filePath = GetExpectedTablePath(tableId);
    Assert.Equal((long)PageSize * initialPages, await _fileSystem.GetFileLengthAsync(filePath));

    // Act
    PageId newPageId = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    Assert.Equal(tableId, newPageId.TableId);
    Assert.Equal(initialPages, newPageId.PageIndex); // Next page index should be initial count

    // Verify file was extended to hold one more page
    Assert.Equal((long)PageSize * (initialPages + 1), await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_ForNonPageAlignedFile_ReturnsNextPageIndexAndExtendsFileCorrectly()
  {
    // Arrange
    int tableId = 204;
    var filePath = GetExpectedTablePath(tableId);
    // Create a file with non-standard length (e.g., 1.5 pages)
    long initialLength = (long)(PageSize * 1.5);
    await using (var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
    {
      fs.SetLength(initialLength);
    }
    Assert.Equal(initialLength, await _fileSystem.GetFileLengthAsync(filePath));

    // Act
    PageId newPageId = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    // Integer division of length / PageSize gives the index of the *last full page*
    // So the next page index should be (int)(initialLength / PageSize) + 1 ? No, just int division.
    // Example: 12288 / 8192 = 1. The next page index is 1.
    // Example: 8193 / 8192 = 1. The next page index is 1.
    int expectedNextIndex = (int)(initialLength / PageSize);
    Assert.Equal(tableId, newPageId.TableId);
    Assert.Equal(expectedNextIndex, newPageId.PageIndex); // Next index based on integer division

    // Verify file was extended to hold the allocated page fully
    long expectedFinalLength = (long)(expectedNextIndex + 1) * PageSize;
    Assert.Equal(expectedFinalLength, await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_CalledTwice_ReturnsIncrementingIndices()
  {
    // Arrange
    int tableId = 205;
    var filePath = GetExpectedTablePath(tableId);
    Assert.False(_fileSystem.FileExists(filePath));

    // Act
    PageId pageId0 = await _diskManager.AllocateNewDiskPageAsync(tableId);
    PageId pageId1 = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    Assert.Equal(0, pageId0.PageIndex);
    Assert.Equal(1, pageId1.PageIndex);
    Assert.Equal(tableId, pageId0.TableId);
    Assert.Equal(tableId, pageId1.TableId);
    Assert.Equal(PageSize * 2, await _fileSystem.GetFileLengthAsync(filePath));
  }

  // Note: Testing concurrent calls would require more complex setup with multiple threads/tasks
  // calling AllocateNewDiskPageAsync simultaneously and verifying atomicity, which might
  // need locking mechanisms within DiskManager (currently deferred).

  // --- IDisposable Implementation (If class is not partial and needs cleanup) ---
  // ... (Keep the Dispose/Finalizer logic as defined previously) ...
}