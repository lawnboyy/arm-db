using ArmDb.Common.Abstractions;
using ArmDb.Common.Utils;
using ArmDb.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace ArmDb.UnitTests.StorageEngine;

public partial class DiskManagerTests : IDisposable
{
  [Fact]
  public async Task ReadDiskPageAsync_ReadsExistingPageZero_ReturnsCorrectData()
  {
    // Arrange
    int tableId = 1;
    var pageId = new PageId(tableId, 0);
    byte pageFillValue = 0xAA;
    await CreateTestTableFileAsync(tableId, 1, p => pageFillValue);
    var buffer = new Memory<byte>(new byte[PageSize]);

    // Act
    int bytesRead = await _diskManager.ReadDiskPageAsync(pageId, buffer);

    // Assert
    Assert.Equal(PageSize, bytesRead);
    Assert.True(buffer.Span.ToArray().All(b => b == pageFillValue), "Buffer content mismatch");
  }

  [Fact]
  public async Task ReadDiskPageAsync_ReadsExistingMiddlePage_ReturnsCorrectData()
  {
    // Arrange
    int tableId = 2;
    int targetPageIndex = 2;
    var pageId = new PageId(tableId, targetPageIndex);
    await CreateTestTableFileAsync(tableId, 5, p => (byte)(p + 1));
    var buffer = new Memory<byte>(new byte[PageSize]);
    byte expectedFillValue = (byte)(targetPageIndex + 1);

    // Act
    int bytesRead = await _diskManager.ReadDiskPageAsync(pageId, buffer);

    // Assert
    Assert.Equal(PageSize, bytesRead);
    Assert.True(buffer.Span.ToArray().All(b => b == expectedFillValue), $"Buffer not filled with {expectedFillValue}");
  }

  [Fact]
  public async Task ReadDiskPageAsync_BufferNotPageSize_ThrowsArgumentException()
  {
    // Arrange
    int tableId = 3;
    var pageId = new PageId(tableId, 0);
    await CreateTestTableFileAsync(tableId, 1, p => 0xCC);
    var smallBuffer = new Memory<byte>(new byte[PageSize - 10]);
    var largeBuffer = new Memory<byte>(new byte[PageSize + 10]);

    // Act & Assert
    await Assert.ThrowsAsync<ArgumentException>("buffer", () => _diskManager.ReadDiskPageAsync(pageId, smallBuffer));
    await Assert.ThrowsAsync<ArgumentException>("buffer", () => _diskManager.ReadDiskPageAsync(pageId, largeBuffer));
  }

  [Fact]
  public async Task ReadDiskPageAsync_TableFileNotFound_ThrowsFileNotFoundException()
  {
    // Arrange
    int tableId = 99;
    var pageId = new PageId(tableId, 0);
    var buffer = new Memory<byte>(new byte[PageSize]);
    Assert.False(_fileSystem.FileExists(GetExpectedTablePath(tableId)));

    // Act & Assert
    await Assert.ThrowsAsync<FileNotFoundException>(() =>
        _diskManager.ReadDiskPageAsync(pageId, buffer));
  }

  [Fact]
  public async Task ReadDiskPageAsync_PageIndexOutOfBounds_ThrowsIOExceptionDueToShortRead()
  {
    // Arrange
    int tableId = 5;
    await CreateTestTableFileAsync(tableId, 2, p => (byte)(p + 1));
    var pageId = new PageId(tableId, 2); // Request page index 2 (valid indices 0, 1)
    var buffer = new Memory<byte>(new byte[PageSize]);

    // Act & Assert
    await Assert.ThrowsAsync<IOException>(() =>
        _diskManager.ReadDiskPageAsync(pageId, buffer));
  }
}