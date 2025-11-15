using ArmDb.StorageEngine;

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
}