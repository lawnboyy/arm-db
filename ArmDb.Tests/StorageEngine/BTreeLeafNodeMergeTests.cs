using ArmDb.DataModel;
using ArmDb.StorageEngine;
using ArmDb.StorageEngine.Exceptions;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeLeafNodeTests // Using partial to extend the existing class
{
  [Fact]
  public void MergeInto_WithTwoNodes_MovesAllRecordsAndUpdatesLinks()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var leftPage = CreateTestPage(new PageId(1, 10)); // The left node (merging into)
    var rightPage = CreateTestPage(new PageId(1, 11)); // The right sibling (merging from, 'this')

    // Format pages and link them: left <-> right
    SlottedPage.Initialize(leftPage, PageType.LeafNode);
    SlottedPage.Initialize(rightPage, PageType.LeafNode);

    var leftHeader = new PageHeader(leftPage);
    var rightHeader = new PageHeader(rightPage);
    leftHeader.NextPageIndex = rightPage.Id.PageIndex;
    rightHeader.PrevPageIndex = leftPage.Id.PageIndex;
    // rightPage is the last node
    rightHeader.NextPageIndex = PageHeader.INVALID_PAGE_INDEX;

    var leftNode = new BTreeLeafNode(leftPage, tableDef);
    var rightNode = new BTreeLeafNode(rightPage, tableDef);

    // Populate leftNode (left)
    var row10 = new Record(DataValue.CreateInteger(10), DataValue.CreateString("Data 10"));
    var row20 = new Record(DataValue.CreateInteger(20), DataValue.CreateString("Data 20"));
    leftNode.TryInsert(row10);
    leftNode.TryInsert(row20);

    // Populate rightNode (right)
    var row30 = new Record(DataValue.CreateInteger(30), DataValue.CreateString("Data 30"));
    var row40 = new Record(DataValue.CreateInteger(40), DataValue.CreateString("Data 40"));
    rightNode.TryInsert(row30);
    rightNode.TryInsert(row40);

    // Act
    // Merge rightNode's content INTO leftNode
    rightNode.MergeLeft(leftNode);

    // Assert
    // 1. Verify leftNode's item count is correct
    Assert.Equal(4, leftNode.ItemCount);

    // 2. Verify rightNode has been cleared (implementation should wipe it)
    Assert.Equal(0, rightNode.ItemCount);

    // 3. Verify leftNode now contains all records in the correct order
    Assert.Equal(row10, leftNode.Search(new Key([DataValue.CreateInteger(10)])));
    Assert.Equal(row20, leftNode.Search(new Key([DataValue.CreateInteger(20)])));
    Assert.Equal(row30, leftNode.Search(new Key([DataValue.CreateInteger(30)])));
    Assert.Equal(row40, leftNode.Search(new Key([DataValue.CreateInteger(40)])));

    // 4. Verify leftNode's NextPageIndex is now -1 (copied from rightNode)
    var leftHeader_after = new PageHeader(leftPage);
    Assert.Equal(PageHeader.INVALID_PAGE_INDEX, leftHeader_after.NextPageIndex);
  }

  [Fact]
  public void MergeInto_InMiddleOfList_CorrectlyUpdatesAllSiblingPointers()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var leftPage = CreateTestPage(10);
    var middlePage = CreateTestPage(11); // The node to be merged (this)
    var rightPage = CreateTestPage(12); // The far-right sibling

    // Format pages and link them: left <-> middle <-> right
    SlottedPage.Initialize(leftPage, PageType.LeafNode);
    SlottedPage.Initialize(middlePage, PageType.LeafNode);
    SlottedPage.Initialize(rightPage, PageType.LeafNode);

    var leftHeader = new PageHeader(leftPage);
    var middleHeader = new PageHeader(middlePage);
    var rightHeader = new PageHeader(rightPage);

    leftHeader.NextPageIndex = middlePage.Id.PageIndex;

    middleHeader.PrevPageIndex = leftPage.Id.PageIndex;
    middleHeader.NextPageIndex = rightPage.Id.PageIndex;

    rightHeader.PrevPageIndex = middlePage.Id.PageIndex;
    rightHeader.NextPageIndex = PageHeader.INVALID_PAGE_INDEX;

    var leftNode = new BTreeLeafNode(leftPage, tableDef);
    var middleNode = new BTreeLeafNode(middlePage, tableDef);
    var rightNode = new BTreeLeafNode(rightPage, tableDef);

    // Populate nodes
    leftNode.TryInsert(new Record(DataValue.CreateInteger(10), DataValue.CreateString("Data 10")));
    middleNode.TryInsert(new Record(DataValue.CreateInteger(20), DataValue.CreateString("Data 20")));
    rightNode.TryInsert(new Record(DataValue.CreateInteger(30), DataValue.CreateString("Data 30")));

    // Act
    // Merge middleNode INTO leftNode, passing rightNode as the far sibling
    middleNode.MergeLeft(leftNode, rightNode);

    // Assert
    // 1. Verify data moved correctly
    Assert.Equal(2, leftNode.ItemCount); // Should now have 10 and 20
    Assert.Equal(0, middleNode.ItemCount); // Should be wiped
    Assert.NotNull(leftNode.Search(new Key([DataValue.CreateInteger(20)]))); // Verify data moved

    // 2. Verify linked-list pointers are updated
    // We need fresh header views to read the modified state
    var headerLeft_after = new PageHeader(leftPage);
    var headerRight_after = new PageHeader(rightPage);

    // leftNode (10) should now point to rightNode (12)
    Assert.Equal(rightPage.Id.PageIndex, headerLeft_after.NextPageIndex);

    // rightNode (12) should now point back to leftNode (10)
    Assert.Equal(leftPage.Id.PageIndex, headerRight_after.PrevPageIndex);
  }

  [Fact]
  public void MergeInto_WithNullLeftSibling_ThrowsArgumentNullException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage(11);
    SlottedPage.Initialize(page, PageType.LeafNode);
    var nodeToMerge = new BTreeLeafNode(page, tableDef);
    BTreeLeafNode? leftSibling = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("leftSibling", () =>
        nodeToMerge.MergeLeft(leftSibling!) // Pass null for farRightSibling
    );
  }

  [Fact]
  public void MergeInto_WhenLeftSiblingIsFull_ThrowsInvalidOperationException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var leftPage = CreateTestPage(10);
    var rightPage = CreateTestPage(11);
    SlottedPage.Initialize(leftPage, PageType.LeafNode);
    SlottedPage.Initialize(rightPage, PageType.LeafNode);

    var leftNode = new BTreeLeafNode(leftPage, tableDef);
    var rightNode = new BTreeLeafNode(rightPage, tableDef);

    // 1. Fill the left node so it has almost no space left
    // Calculate size needed to leave only 1 byte of free space
    int freeSpace = SlottedPage.GetFreeSpace(leftPage);
    int nullBitmapSize = (tableDef.Columns.Count + 7) / 8; // 1 byte
    int fixedDataSize = tableDef.Columns[0].DataType.GetFixedSize(); // 4 bytes
    int varLengthPrefixSize = sizeof(int); // 4 bytes
    int otherOverhead = Slot.Size + nullBitmapSize + fixedDataSize + varLengthPrefixSize; // 8 + 1 + 4 + 4 = 17 bytes

    int dataSize = freeSpace - otherOverhead - 1; // Leaves 1 byte of free space

    var largeRecord = new Record(DataValue.CreateInteger(1), DataValue.CreateString(new string('A', dataSize)));

    Assert.True(leftNode.TryInsert(largeRecord)); // This should fill the page

    // 2. Add a record to the right node (the one to be merged)
    var recordToMerge = new Record(DataValue.CreateInteger(100), DataValue.CreateString("merge me"));
    rightNode.TryInsert(recordToMerge);

    // Act & Assert
    // The attempt to merge (which calls leftNode.AppendRawRecord) should fail
    // because leftNode does not have enough space for the new record.
    // This is a violation of the MergeLeft contract, which assumes space was pre-checked.
    Assert.Throws<InvalidOperationException>(() =>
        rightNode.MergeLeft(leftNode, null)
    );
  }

  [Fact]
  public void MergeInto_WhenMergingNodeIsEmpty_CorrectlyUpdatesLinks()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var pageLeft = CreateTestPage(10);
    var pageMiddle = CreateTestPage(11); // This node is empty
    var pageRight = CreateTestPage(12);

    // Link them: Left <-> Middle <-> Right
    SlottedPage.Initialize(pageLeft, PageType.LeafNode);
    SlottedPage.Initialize(pageMiddle, PageType.LeafNode);
    SlottedPage.Initialize(pageRight, PageType.LeafNode);

    var headerLeft = new PageHeader(pageLeft);
    var headerMiddle = new PageHeader(pageMiddle);
    var headerRight = new PageHeader(pageRight);

    headerLeft.NextPageIndex = pageMiddle.Id.PageIndex;
    headerMiddle.PrevPageIndex = pageLeft.Id.PageIndex;
    headerMiddle.NextPageIndex = pageRight.Id.PageIndex;
    headerRight.PrevPageIndex = pageMiddle.Id.PageIndex;
    headerRight.NextPageIndex = PageHeader.INVALID_PAGE_INDEX;

    var leftNode = new BTreeLeafNode(pageLeft, tableDef);
    var middleNode = new BTreeLeafNode(pageMiddle, tableDef);
    var rightNode = new BTreeLeafNode(pageRight, tableDef);

    // Add some data to prove nodes are not wiped
    leftNode.TryInsert(new Record(DataValue.CreateInteger(10), DataValue.CreateString("Data 10")));
    rightNode.TryInsert(new Record(DataValue.CreateInteger(30), DataValue.CreateString("Data 30")));

    // Verify setup
    Assert.Equal(1, leftNode.ItemCount);
    Assert.Equal(0, middleNode.ItemCount);
    Assert.Equal(1, rightNode.ItemCount);

    // Act
    // Merge the empty middleNode INTO leftNode, passing rightNode
    middleNode.MergeLeft(leftNode, rightNode);

    // Assert
    // 1. Verify item counts are unchanged
    Assert.Equal(1, leftNode.ItemCount); // Should still be 1
    Assert.Equal(0, middleNode.ItemCount); // Should still be 0 (or reset)

    // 2. Verify the pointers are "spliced" correctly: Left <-> Right
    var headerLeft_after = new PageHeader(pageLeft);
    var headerRight_after = new PageHeader(pageRight);

    // leftNode (10) should now point to rightNode (12)
    Assert.Equal(pageRight.Id.PageIndex, headerLeft_after.NextPageIndex);

    // rightNode (12) should now point back to leftNode (10)
    Assert.Equal(pageLeft.Id.PageIndex, headerRight_after.PrevPageIndex);
  }
}