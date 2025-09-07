using ArmDb.DataModel;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

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
            new DataRow(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10")),
            new DataRow(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20")),
            new DataRow(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30")),
            new DataRow(DataValue.CreateInteger(50), DataValue.CreateString("Data for 50")),
            new DataRow(DataValue.CreateInteger(60), DataValue.CreateString("Data for 60")),
            new DataRow(DataValue.CreateInteger(70), DataValue.CreateString("Data for 70"))
        };

    foreach (var row in initialRows)
    {
      nodeA.TryInsert(row);
    }
    Assert.Equal(6, nodeA.ItemCount); // Verify setup

    // The new row to insert, which will be the median of the combined 7 records
    var newRowToInsert = new DataRow(DataValue.CreateInteger(40), DataValue.CreateString("Data for 40"));
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
}
