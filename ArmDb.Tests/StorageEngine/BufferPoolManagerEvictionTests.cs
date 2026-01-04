using ArmDb.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using static ArmDb.UnitTests.Storage.StorageEngineTestHelper;

namespace ArmDb.UnitTests.Storage;

public partial class BufferPoolManagerTests
{
  [Fact]
  public async Task FetchPageAsync_PoolFull_EvictsCleanLruPage_AndLoadsNewPage()
  {
    // Arrange
    var options = new BufferPoolManagerOptions { PoolSizeInPages = 2 }; // Small pool for controlled eviction
    // Create a local BPM for this test to control its state precisely
    var localBpm = new BufferPoolManager(Options.Create(options), _diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 401;
    var pageId0 = new PageId(tableId, 0); // Will be LRU, clean
    var pageId1 = new PageId(tableId, 1); // Will be MRU, clean
    var pageId2 = new PageId(tableId, 2); // New page to fetch, causing eviction

    // Create initial data on "disk" for these pages
    byte[] page0InitialData = CreateTestBuffer(0xAA); // Unique content for page 0
    byte[] page1InitialData = CreateTestBuffer(0xBB); // Unique content for page 1
    byte[] page2InitialData = CreateTestBuffer(0xCC); // Unique content for page 2

    await CreateTestPageFileWithDataAsync(tableId, 0, page0InitialData);
    await CreateTestPageFileWithDataAsync(tableId, 1, page1InitialData);
    await CreateTestPageFileWithDataAsync(tableId, 2, page2InitialData);

    // 1. Fetch P0: pool=[P0*], LRU=[P0] (* indicates pinned)
    Page? p0_instance1 = await localBpm.FetchPageAsync(pageId0);
    Assert.NotNull(p0_instance1);

    // 2. Fetch P1: pool=[P0*, P1*], LRU=[P0, P1]
    Page? p1_instance1 = await localBpm.FetchPageAsync(pageId1);
    Assert.NotNull(p1_instance1);

    // 3. Unpin P0 (isDirty: false): pool=[P0, P1*], LRU=[P0(clean,LRU), P1(MRU,pinned)]
    localBpm.UnpinPage(pageId0, false);

    // 4. Unpin P1 (isDirty: false): pool=[P0, P1], LRU=[P0(clean,LRU), P1(clean,MRU)]
    localBpm.UnpinPage(pageId1, false);
    // At this point, P0 is the LRU candidate for eviction.

    // Act: Fetch P2. This should cause P0 (clean) to be evicted.
    Page? p2_instance = await localBpm.FetchPageAsync(pageId2);

    // Assert
    // 1. P2 should be fetched correctly
    Assert.NotNull(p2_instance);
    Assert.Equal(pageId2, p2_instance.Id);
    Assert.True(p2_instance.Data.Span.SequenceEqual(page2InitialData), "Content of newly fetched P2 is incorrect.");

    // 2. Verify P0 (clean) was evicted and its content on disk remains original (wasn't re-written)
    byte[] p0_diskContentAfterEviction = await ReadPageDirectlyAsync(GetExpectedTablePath(tableId), 0);
    Assert.True(page0InitialData.SequenceEqual(p0_diskContentAfterEviction), "Clean page P0 should not have been written back to disk.");

    // 3. Verify P1 is still in the cache (it was MRU among the original two)
    //    Fetching it again should be a cache hit. We can verify its content from memory.
    Page? p1_instance2 = await localBpm.FetchPageAsync(pageId1);
    Assert.NotNull(p1_instance2);
    Assert.True(p1_instance2.Data.Span.SequenceEqual(page1InitialData), "P1 content mismatch after P0 eviction; implies P1 might have been wrongly evicted or its data corrupted.");
    localBpm.UnpinPage(pageId1, false); // Unpin for cleanup

    // Cleanup local BPM
    await localBpm.DisposeAsync();
  }

  [Fact]
  public async Task FetchPageAsync_PoolFull_EvictsDirtyLruPage_FlushesAndLoadsNewPage()
  {
    // Arrange
    var options = new BufferPoolManagerOptions { PoolSizeInPages = 2 }; // Small pool
    var localBpm = new BufferPoolManager(Options.Create(options), _diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 402;
    var pageId0 = new PageId(tableId, 0); // Will be LRU and dirty
    var pageId1 = new PageId(tableId, 1); // Will be MRU and clean
    var pageId2 = new PageId(tableId, 2); // New page to fetch

    byte[] page0InitialData = CreateTestBuffer(0xAA);
    byte[] page0ModifiedData = CreateTestBuffer(0xA1); // P0 will be changed to this in memory
    byte[] page1InitialData = CreateTestBuffer(0xBB);
    byte[] page2InitialData = CreateTestBuffer(0xCC);

    await CreateTestPageFileWithDataAsync(tableId, 0, page0InitialData);
    await CreateTestPageFileWithDataAsync(tableId, 1, page1InitialData);
    await CreateTestPageFileWithDataAsync(tableId, 2, page2InitialData);

    // 1. Fetch P0, caches page 0 in memory
    Page? p0_instance1 = await localBpm.FetchPageAsync(pageId0);
    Assert.NotNull(p0_instance1);

    // 2. Fetch P1, caches page 1 in memory
    Page? p1_instance1 = await localBpm.FetchPageAsync(pageId1);
    Assert.NotNull(p1_instance1);

    // 3. Modify P0 in memory
    page0ModifiedData.CopyTo(p0_instance1.Data); // page0_instance1.Data is Memory<byte>

    // 4. Unpin P0 (isDirty: true). P0 is now dirty and an LRU candidate.
    localBpm.UnpinPage(pageId0, true);

    // 5. Unpin P1 (isDirty: false). P1 is now MRU. P0 is LRU.
    localBpm.UnpinPage(pageId1, false);

    // Act: Fetch P2. This should cause P0 (dirty) to be flushed and evicted.
    Page? p2_instance = await localBpm.FetchPageAsync(pageId2);

    // Assert
    // 1. P2 should be fetched correctly
    Assert.NotNull(p2_instance);
    Assert.Equal(pageId2, p2_instance.Id);
    Assert.True(p2_instance.Data.Span.SequenceEqual(page2InitialData), "Content of newly fetched P2 is incorrect.");

    // 2. Verify P0 (dirty) was flushed to disk with its MODIFIED content
    byte[] p0_diskContentAfterEviction = await ReadPageDirectlyAsync(GetExpectedTablePath(tableId), 0);
    Assert.True(page0ModifiedData.SequenceEqual(p0_diskContentAfterEviction), "Dirty page P0 was not flushed correctly with modifications.");

    // 3. Verify P1 is still in the cache
    Page? p1_instance2 = await localBpm.FetchPageAsync(pageId1);
    Assert.NotNull(p1_instance2);
    Assert.True(p1_instance2.Data.Span.SequenceEqual(page1InitialData), "P1 content mismatch after P0 eviction.");
    localBpm.UnpinPage(pageId1, false);

    // Cleanup local BPM
    await localBpm.DisposeAsync();
  }
}