using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BufferPoolManagerTests : IDisposable
{
  [Fact]
  public async Task CreatePageAsync_WhenFreeFramesAvailable_ReturnsNewPinnedDirtyPage()
  {
    // Arrange
    // We use the default _bpm, which has 100 frames (so plenty of free ones)
    // We'll use the _diskManager which uses the real FileSystem.
    int tableId = 5;
    var expectedNewPageId = new PageId(tableId, 0); // The first page for this table
    string expectedFilePath = Path.Combine(_baseTestDir, $"{tableId}{DiskManager.TableFileExtension}");

    // Act
    // Call the method to be implemented
    Page newPage = await _bpm.CreatePageAsync(tableId);

    // Assert
    Assert.NotNull(newPage);

    // 1. Verify the returned Page object is correct
    Assert.Equal(expectedNewPageId, newPage.Id);

    // 2. Verify the physical file was created and extended on disk
    Assert.True(_fileSystem.FileExists(expectedFilePath));
    Assert.Equal(Page.Size, await _fileSystem.GetFileLengthAsync(expectedFilePath));

    // 3. Verify the page's state in the buffer pool (using test hook)
#if DEBUG
    var frame = _bpm.GetFrameByPageId_TestOnly(expectedNewPageId);
    Assert.NotNull(frame);
    Assert.True(frame.IsDirty, "New page frame should be marked dirty.");
    Assert.Equal(1, frame.PinCount); // New page should be returned pinned
#endif
  }
}