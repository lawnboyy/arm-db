using ArmDb.StorageEngine;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using static ArmDb.UnitTests.StorageEngine.StorageEngineTestHelper;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BufferPoolManagerTests
{
  [Fact]
  public async Task FetchPageAsync_PoolFull_EvictsLeastRecentlyUsedPage()
  {
    // Arrange
    var options = new BufferPoolManagerOptions { PoolSizeInPages = 3 };
    // Use a local BPM instance to control pool size precisely
    var localBpm = new BufferPoolManager(Options.Create(options), _diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 801;
    var p0 = new PageId(tableId, 0);
    var p1 = new PageId(tableId, 1);
    var p2 = new PageId(tableId, 2);
    var p3 = new PageId(tableId, 3); // The new page that will force eviction

    // Create data on disk for all pages
    await CreateTestPageFileWithDataAsync(tableId, 0, CreateTestBuffer(0xA0));
    await CreateTestPageFileWithDataAsync(tableId, 1, CreateTestBuffer(0xA1));
    await CreateTestPageFileWithDataAsync(tableId, 2, CreateTestBuffer(0xA2));
    await CreateTestPageFileWithDataAsync(tableId, 3, CreateTestBuffer(0xA3));

    // 1. Fill the pool. Fetch order: P0, P1, P2.
    // Expected LRU order (head to tail): [P0, P1, P2] (MRU)
    await localBpm.FetchPageAsync(p0);
    await localBpm.FetchPageAsync(p1);
    await localBpm.FetchPageAsync(p2);

    // 2. Unpin all pages so they are eligible for eviction.
    // Unpinning shouldn't change their LRU order based on our current design (updates on fetch).
    await localBpm.UnpinPageAsync(p0, false);
    await localBpm.UnpinPageAsync(p1, false);
    await localBpm.UnpinPageAsync(p2, false);

    // Act
    // 3. Fetch P3. This requires eviction.
    // P0 should be the LRU victim.
    Page? fetchedP3 = await localBpm.FetchPageAsync(p3);

    // Assert
    Assert.NotNull(fetchedP3);
    Assert.Equal(p3, fetchedP3.Id);

    // 4. Verify P0 was evicted (should not be in pool)
    // We can test this by trying to unpin it (should throw if not in pool)
    // or by using our test hook if available.
#if DEBUG
    Assert.Null(localBpm.GetFrameByPageId_TestOnly(p0)); // P0 should be gone
    Assert.NotNull(localBpm.GetFrameByPageId_TestOnly(p1)); // P1 should still be there
    Assert.NotNull(localBpm.GetFrameByPageId_TestOnly(p2)); // P2 should still be there
    Assert.NotNull(localBpm.GetFrameByPageId_TestOnly(p3)); // P3 should be there
#else
        // Without test hook, we can try to fetch P0 again.
        // If it was evicted, it will be a cache miss (disk read).
        // We'd need a way to detect that, e.g., via ControllableFileSystem call counts.
        // For now, assuming DEBUG is available for deep inspection is easiest.
#endif

    await localBpm.DisposeAsync();
  }

  [Fact]
  public async Task FetchPageAsync_CacheHit_UpdatesLruPosition()
  {
    // Arrange
    var options = new BufferPoolManagerOptions { PoolSizeInPages = 3 };
    var localBpm = new BufferPoolManager(Options.Create(options), _diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 802;
    var p0 = new PageId(tableId, 0);
    var p1 = new PageId(tableId, 1);
    var p2 = new PageId(tableId, 2);
    var p3 = new PageId(tableId, 3);

    await CreateTestPageFileWithDataAsync(tableId, 0, CreateTestBuffer(0xB0));
    await CreateTestPageFileWithDataAsync(tableId, 1, CreateTestBuffer(0xB1));
    await CreateTestPageFileWithDataAsync(tableId, 2, CreateTestBuffer(0xB2));
    await CreateTestPageFileWithDataAsync(tableId, 3, CreateTestBuffer(0xB3));

    // 1. Fill pool: [P0, P1, P2] (MRU)
    await localBpm.FetchPageAsync(p0);
    await localBpm.FetchPageAsync(p1);
    await localBpm.FetchPageAsync(p2);
    await localBpm.UnpinPageAsync(p0, false);
    await localBpm.UnpinPageAsync(p1, false);
    await localBpm.UnpinPageAsync(p2, false);

    // Act
    // 2. Access P0 again. It should move from LRU position to MRU position.
    // New expected order: [P1, P2, P0] (MRU)
    await localBpm.FetchPageAsync(p0);
    await localBpm.UnpinPageAsync(p0, false);

    // 3. Fetch P3, forcing eviction. P1 should now be the victim.
    await localBpm.FetchPageAsync(p3);

    // Assert
#if DEBUG
    // P1 should be evicted (it became LRU after P0 was accessed)
    Assert.Null(localBpm.GetFrameByPageId_TestOnly(p1));

    // P0 should still be in the pool because it was recently accessed
    Assert.NotNull(localBpm.GetFrameByPageId_TestOnly(p0));

    // P2 and P3 should also be present
    Assert.NotNull(localBpm.GetFrameByPageId_TestOnly(p2));
    Assert.NotNull(localBpm.GetFrameByPageId_TestOnly(p3));
#endif
    await localBpm.DisposeAsync();
  }
}