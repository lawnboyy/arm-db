using ArmDb.StorageEngine;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

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

  [Fact]
  public async Task CreatePageAsync_WhenPoolFull_EvictsLRUVictimAndReturnsNewPage()
  {
    // Arrange
    // 1. Use a pool size of 3 to have multiple candidates for eviction
    var options = new BufferPoolManagerOptions { PoolSizeInPages = 3 };
    var localBpm = new BufferPoolManager(Options.Create(options), _diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 8;
    string filePath = Path.Combine(_baseTestDir, $"{tableId}{DiskManager.TableFileExtension}");

    // 2. Fill the pool with 3 pages (P0, P1, P2).
    // We create them sequentially.
    Page p0 = await localBpm.CreatePageAsync(tableId);
    Page p1 = await localBpm.CreatePageAsync(tableId);
    Page p2 = await localBpm.CreatePageAsync(tableId);

    // 3. Make P0 dirty and the LRU victim.
    p0.Data.Span[0] = 0xFE; // Mark P0 with known data
    await localBpm.UnpinPageAsync(p0.Id, isDirty: true);

    // Unpin the others as clean. They will be MRU relative to P0.
    await localBpm.UnpinPageAsync(p1.Id, isDirty: false);
    await localBpm.UnpinPageAsync(p2.Id, isDirty: false);

    // Current expected state: Pool=[P0, P1, P2], LRU=[P0, P1, P2] (MRU)

    // Act
    // 4. Create a 4th page (P3). This MUST evict P0 (the LRU page).
    Page p3 = await localBpm.CreatePageAsync(tableId);

    // Assert
    Assert.NotNull(p3);
    Assert.Equal(new PageId(tableId, 3), p3.Id); // Should be the next logical page index

    // 5. Verify P0 was evicted and flushed to disk
#if DEBUG
    Assert.Null(localBpm.GetFrameByPageId_TestOnly(p0.Id));
    // Verify P1 and P2 are STILL in the pool
    Assert.NotNull(localBpm.GetFrameByPageId_TestOnly(p1.Id));
    Assert.NotNull(localBpm.GetFrameByPageId_TestOnly(p2.Id));
#endif

    var p0DataOnDisk = new byte[Page.Size];
    // Read P0's data from disk (offset 0)
    using (var handle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
    {
      await RandomAccess.ReadAsync(handle, p0DataOnDisk.AsMemory(), 0);
    }
    Assert.Equal(0xFE, p0DataOnDisk[0]); // Verify dirty data was flushed

    await localBpm.DisposeAsync();
  }
}