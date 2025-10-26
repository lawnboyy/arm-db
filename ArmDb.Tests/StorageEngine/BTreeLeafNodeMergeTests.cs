using ArmDb.DataModel;
using ArmDb.StorageEngine;
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

}