using ArmDb.DataModel;
using ArmDb.Storage;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.Storage.BTreeTests;

public partial class BTreeLeafNodeTests
{
  [Fact]
  public void SplitAndInsert_WhenNewRecordIsMedian_CorrectlyDistributesRecords()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var pageA = CreateTestPage(new PageId(1, 10)); // The page to be split
    var pageB = CreateTestPage(new PageId(1, 11)); // The new sibling page
    SlottedPage.Initialize(pageA, PageType.LeafNode);
    SlottedPage.Initialize(pageB, PageType.LeafNode);

    var nodeA = new BTreeLeafNode(pageA, tableDef);
    var nodeB = new BTreeLeafNode(pageB, tableDef);

    // Populate nodeA with an even number of records
    var initialRows = new[]
    {
            new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10")),
            new ArmDb.DataModel.Record(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20")),
            new ArmDb.DataModel.Record(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30")),
            new ArmDb.DataModel.Record(DataValue.CreateInteger(50), DataValue.CreateString("Data for 50")),
            new ArmDb.DataModel.Record(DataValue.CreateInteger(60), DataValue.CreateString("Data for 60")),
            new ArmDb.DataModel.Record(DataValue.CreateInteger(70), DataValue.CreateString("Data for 70"))
        };

    foreach (var row in initialRows)
    {
      nodeA.TryInsert(row);
    }
    Assert.Equal(6, nodeA.ItemCount); // Verify setup

    // The new row to insert, which will be the median of the combined 7 records
    var newRowToInsert = new ArmDb.DataModel.Record(DataValue.CreateInteger(40), DataValue.CreateString("Data for 40"));
    var expectedSeparatorKey = new Key([DataValue.CreateInteger(40)]);

    // Act
    // The SplitAndInsert method will perform the split and insert the new row.
    Key separatorKey = nodeA.SplitAndInsert(newRowToInsert, nodeB);

    // Assert
    // 1. Verify the correct separator key was returned (the new median key)
    Assert.Equal(expectedSeparatorKey, separatorKey);

    // 2. Verify item counts are correct after the split and redistribution
    // The original 6 records + 1 new record = 7 total.
    // Midpoint of 7 is index 3.
    // Node A should get records at indices 0, 1, 2 (3 records).
    // Node B should get records at indices 3, 4, 5, 6 (4 records).
    Assert.Equal(3, nodeA.ItemCount);
    Assert.Equal(4, nodeB.ItemCount);

    // 3. Verify the content of both nodes
    // Node A should contain the first part of the sorted list
    Assert.NotNull(nodeA.Search(new Key([DataValue.CreateInteger(10)])));
    Assert.NotNull(nodeA.Search(new Key([DataValue.CreateInteger(20)])));
    Assert.NotNull(nodeA.Search(new Key([DataValue.CreateInteger(30)])));
    Assert.Null(nodeA.Search(new Key([DataValue.CreateInteger(40)]))); // 40 should be in Node B

    // Node B should contain the second part of the sorted list
    Assert.NotNull(nodeB.Search(new Key([DataValue.CreateInteger(40)]))); // The newly inserted row
    Assert.NotNull(nodeB.Search(new Key([DataValue.CreateInteger(50)])));
    Assert.NotNull(nodeB.Search(new Key([DataValue.CreateInteger(60)])));
    Assert.NotNull(nodeB.Search(new Key([DataValue.CreateInteger(70)])));
    Assert.Null(nodeB.Search(new Key([DataValue.CreateInteger(30)]))); // 30 should be in Node A

    // 4. Verify sibling pointers are linked correctly (A -> B)
    var headerA = new PageHeader(pageA);
    var headerB = new PageHeader(pageB);
    Assert.Equal(pageB.Id.PageIndex, headerA.NextPageIndex);
    Assert.Equal(pageA.Id.PageIndex, headerB.PrevPageIndex);
  }

  [Fact]
  public void SplitAndInsert_WithOddNumberOfRecords_CorrectlyDistributesRecords()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var pageA = CreateTestPage(new PageId(1, 20));
    var pageB = CreateTestPage(new PageId(1, 21));
    SlottedPage.Initialize(pageA, PageType.LeafNode);
    SlottedPage.Initialize(pageB, PageType.LeafNode);

    var nodeA = new BTreeLeafNode(pageA, tableDef);
    var nodeB = new BTreeLeafNode(pageB, tableDef);

    // Populate with an odd number of records
    var initialRows = Enumerable.Range(0, 5).Select(i =>
        new Record(DataValue.CreateInteger(i * 10), DataValue.CreateString($"Data for {i * 10}"))
    ).ToList(); // 0, 10, 20, 30, 40

    foreach (var row in initialRows)
    {
      nodeA.TryInsert(row);
    }
    Assert.Equal(5, nodeA.ItemCount);

    var newRowToInsert = new ArmDb.DataModel.Record(DataValue.CreateInteger(25), DataValue.CreateString("Data for 25"));
    var expectedSeparatorKey = new Key([DataValue.CreateInteger(25)]);

    // Act
    Key separatorKey = nodeA.SplitAndInsert(newRowToInsert, nodeB);

    // Assert
    Assert.Equal(expectedSeparatorKey, separatorKey);

    // Total 6 records. Midpoint is 3. Node A gets 3, Node B gets 3.
    Assert.Equal(3, nodeA.ItemCount);
    Assert.Equal(3, nodeB.ItemCount);

    // Check content
    Assert.NotNull(nodeA.Search(new Key([DataValue.CreateInteger(0)])));
    Assert.NotNull(nodeA.Search(new Key([DataValue.CreateInteger(10)])));
    Assert.NotNull(nodeA.Search(new Key([DataValue.CreateInteger(20)])));

    Assert.NotNull(nodeB.Search(new Key([DataValue.CreateInteger(25)]))); // New record
    Assert.NotNull(nodeB.Search(new Key([DataValue.CreateInteger(30)])));
    Assert.NotNull(nodeB.Search(new Key([DataValue.CreateInteger(40)])));
  }

  [Fact]
  public void SplitAndInsert_InMiddleOfList_CorrectlyUpdatesAllSiblingPointers()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    // Setup a linked list of pages: P1 <-> P2 <-> P3
    var leftSiblingPage = CreateTestPage(new PageId(1, 101));
    var pageToSplit = CreateTestPage(new PageId(1, 102)); // This is the page we will split
    var rightSiblingPage = CreateTestPage(new PageId(1, 103));
    var newPageForSplit = CreateTestPage(new PageId(1, 104)); // This is the new sibling for P2

    SlottedPage.Initialize(leftSiblingPage, PageType.LeafNode);
    SlottedPage.Initialize(pageToSplit, PageType.LeafNode);
    SlottedPage.Initialize(rightSiblingPage, PageType.LeafNode);
    SlottedPage.Initialize(newPageForSplit, PageType.LeafNode);

    // Link them manually: P1 -> P2 -> P3
    var leftSiblingHeader = new PageHeader(leftSiblingPage);
    var pageToSplitHeader = new PageHeader(pageToSplit);
    var rightSiblingHeader = new PageHeader(rightSiblingPage);
    leftSiblingHeader.NextPageIndex = pageToSplit.Id.PageIndex;
    pageToSplitHeader.PrevPageIndex = leftSiblingPage.Id.PageIndex;
    pageToSplitHeader.NextPageIndex = rightSiblingPage.Id.PageIndex;
    rightSiblingHeader.PrevPageIndex = pageToSplit.Id.PageIndex;

    var leafToSplit = new BTreeLeafNode(pageToSplit, tableDef);
    var newLeafForSplit = new BTreeLeafNode(newPageForSplit, tableDef);
    var rightSiblingLeaf = new BTreeLeafNode(rightSiblingPage, tableDef);

    // Populate P2 with some data
    for (int i = 0; i < 4; i++)
    {
      leafToSplit.TryInsert(new ArmDb.DataModel.Record(DataValue.CreateInteger(i * 10), DataValue.CreateString($"Data for {i * 10}")));
    }

    var newRowToInsert = new ArmDb.DataModel.Record(DataValue.CreateInteger(25), DataValue.CreateString($"Data for {25}"));

    // Act
    // Split P2. The new page P4 should be inserted between P2 and P3.
    leafToSplit.SplitAndInsert(newRowToInsert, newLeafForSplit, rightSiblingLeaf);

    // Assert
    // The final linked list order must be: P1 <-> P2 <-> P4 <-> P3
    var updatedLeftSiblingHeader = new PageHeader(leftSiblingPage);
    var updatedSplitPageHeader = new PageHeader(pageToSplit);
    var updatedRightSiblingHeader = new PageHeader(rightSiblingPage);
    var updatedNewPageHeader = new PageHeader(newPageForSplit);

    // Check P2's forward pointer
    Assert.Equal(newPageForSplit.Id.PageIndex, updatedSplitPageHeader.NextPageIndex);
    // Check P4's pointers
    Assert.Equal(pageToSplit.Id.PageIndex, updatedNewPageHeader.PrevPageIndex);
    Assert.Equal(rightSiblingPage.Id.PageIndex, updatedNewPageHeader.NextPageIndex);
    // Check P3's backward pointer (this is the critical one that often gets missed)
    Assert.Equal(newPageForSplit.Id.PageIndex, updatedRightSiblingHeader.PrevPageIndex);
    // P1's forward pointer should not have changed during this split
    Assert.Equal(pageToSplit.Id.PageIndex, updatedLeftSiblingHeader.NextPageIndex);
  }
}
