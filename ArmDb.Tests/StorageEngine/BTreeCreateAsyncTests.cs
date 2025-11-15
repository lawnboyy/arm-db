using ArmDb.StorageEngine;
using ArmDb.StorageEngine.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeTests : IDisposable
{
  [Fact]
  public async Task CreateAsync_InitializesEmptyRootLeafPageCorrectly()
  {
    // Arrange
    // (Setup is done in the constructor)
    // We need to know the TableId to check the PageId
    // Assuming TableDefinition will have a way to get this.
    // For now, let's assume the test table 'TestTable' has a known ID, e.g., 1
    // We'll pass this ID to our helper
    var tableDef = CreateIntPKTable(1);
    int expectedTableId = tableDef.TableId;

    // Act
    // Call the static factory method
    var btree = await BTree.CreateAsync(_bpm, tableDef);

    // Assert
    Assert.NotNull(btree);

#if DEBUG
    // 1. Get the root PageId from the BTree
    var rootPageId = btree.GetRootPageIdForTest(); // Requires internal test hook on BTree

    // 2. Verify it's the first page for this table
    Assert.Equal(expectedTableId, rootPageId.TableId);
    Assert.Equal(0, rootPageId.PageIndex);

    // 3. Get the corresponding frame from the BPM to check its state
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootPageId);
    Assert.NotNull(rootFrame);

    // 4. Verify the page was left in the correct state in the buffer pool
    Assert.True(rootFrame.IsDirty, "The new root page should be marked dirty.");
    Assert.Equal(0, rootFrame.PinCount); // CreateAsync should unpin the page

    // 5. Verify the page content was correctly formatted
    var page = new Page(rootFrame.CurrentPageId, rootFrame.PageData);
    var header = new PageHeader(page);
    Assert.Equal(PageType.LeafNode, header.PageType);
    Assert.Equal(0, header.ItemCount);
    Assert.Equal(PageHeader.INVALID_PAGE_INDEX, header.NextPageIndex);
    Assert.Equal(PageHeader.INVALID_PAGE_INDEX, header.ParentPageIndex);
#endif
  }

  [Fact]
  public async Task CreateAsync_WhenPoolFullOfPinnedPages_ThrowsBufferPoolFullException()
  {
    // Arrange
    // 1. Create a *local* BPM with a very small pool size
    var smallPoolOptions = new BufferPoolManagerOptions { PoolSizeInPages = 2 };
    var localBpm = new BufferPoolManager(Options.Create(smallPoolOptions), _diskManager, NullLogger<BufferPoolManager>.Instance);

    // 2. Fill the pool with pinned pages
    //    We must use a different tableId for each so CreateAsync can succeed
    var tableDef1 = CreateIntPKTable(101);
    var tableDef2 = CreateIntPKTable(102);

    await BTree.CreateAsync(localBpm, tableDef1); // Creates page, leaves it unpinned
    await BTree.CreateAsync(localBpm, tableDef2); // Creates page, leaves it unpinned

    // Now, manually fetch and *pin* the pages to fill the pool
    var page0 = await localBpm.FetchPageAsync(new PageId(tableDef1.TableId, 0));
    var page1 = await localBpm.FetchPageAsync(new PageId(tableDef2.TableId, 0));
    Assert.NotNull(page0); // Ensure they were fetched
    Assert.NotNull(page1);

    // At this point, the pool of size 2 is full of pinned pages (p0 and p1)

    // 3. Define the table for the creation attempt that should fail
    var tableDef3 = CreateIntPKTable(103);

    // Act & Assert
    // 4. The call to CreateAsync should fail because its internal call to
    //    _bpm.CreatePageAsync() will find no evictable frames.
    await Assert.ThrowsAsync<BufferPoolFullException>(() =>
        BTree.CreateAsync(localBpm, tableDef3)
    );

    // Cleanup
    await localBpm.DisposeAsync();
  }
}