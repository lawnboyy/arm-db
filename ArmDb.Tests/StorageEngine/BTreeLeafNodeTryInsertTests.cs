using ArmDb.DataModel;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeLeafNodeTests
{
  [Fact]
  public void TryInsert_WhenSpaceAvailable_InsertsRecordInCorrectOrder()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Pre-populate the page with some records, leaving a gap
    var row10 = new DataRow(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row30 = new DataRow(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30"));

    // Use SlottedPage.TryAddItem for test setup to avoid dependency on the method under test
    SlottedPage.TryAddItem(page, RecordSerializer.Serialize(tableDef, row10), 0);
    SlottedPage.TryAddItem(page, RecordSerializer.Serialize(tableDef, row30), 1);

    // The new row to insert
    var row20 = new DataRow(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));

    // Act
    bool success = leafNode.TryInsert(row20);

    // Assert
    Assert.True(success);

    // 1. Verify the item count in the header has increased
    var header = new PageHeader(page);
    Assert.Equal(3, header.ItemCount);

    // 2. Verify the records are now in the correct logical order by searching for their keys
    Assert.Equal(row10, leafNode.Search(new Key([DataValue.CreateInteger(10)])));
    Assert.Equal(row20, leafNode.Search(new Key([DataValue.CreateInteger(20)])));
    Assert.Equal(row30, leafNode.Search(new Key([DataValue.CreateInteger(30)])));
  }
}