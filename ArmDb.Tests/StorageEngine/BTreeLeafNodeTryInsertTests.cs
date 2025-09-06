using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
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

  [Fact]
  public void TryInsert_WhenKeyAlreadyExists_ThrowsDuplicateKeyException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Pre-populate the page with a record
    var originalRow = new DataRow(DataValue.CreateInteger(100), DataValue.CreateString("Original Data"));
    leafNode.TryInsert(originalRow);

    // Create a new row with the SAME primary key but different data
    var duplicateKeyRow = new DataRow(DataValue.CreateInteger(100), DataValue.CreateString("Duplicate Data"));
    var expectedKey = new Key([DataValue.CreateInteger(100)]);

    var headerBefore = new PageHeader(page);
    int initialItemCount = headerBefore.ItemCount;

    // Act & Assert
    // The attempt to insert the duplicate should throw a specific exception
    var ex = Assert.Throws<DuplicateKeyException>(() => leafNode.TryInsert(duplicateKeyRow));

    // Optional: Assert that the exception contains useful information
    Assert.Contains($"The key '{expectedKey}' already exists", ex.Message);

    // Finally, assert that the page state has not changed
    var headerAfter = new PageHeader(page);
    Assert.Equal(initialItemCount, headerAfter.ItemCount); // No new items should be added

    // The original record should still be present and unchanged
    var recordInPage = leafNode.Search(new Key([DataValue.CreateInteger(100)]));
    Assert.Equal(originalRow, recordInPage);
  }

  [Fact]
  public void TryInsert_WhenCompositeKeyAlreadyExists_ThrowsDuplicateKeyException()
  {
    // Arrange
    var tableDef = CreateCompositePKTableWithIsActive();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    var originalRow = new DataRow(
        DataValue.CreateString("Sales"),
        DataValue.CreateInteger(901),
        DataValue.CreateBoolean(true)
    );
    leafNode.TryInsert(originalRow);

    // Create a new row with the SAME composite key but different non-key data
    var duplicateKeyRow = new DataRow(
        DataValue.CreateString("Sales"),
        DataValue.CreateInteger(901),
        DataValue.CreateBoolean(false) // Different non-key value
    );

    var expectedKey = new Key(new[]
    {
            DataValue.CreateString("Sales"),
            DataValue.CreateInteger(901)
        });

    var headerBefore = new PageHeader(page);
    int initialItemCount = headerBefore.ItemCount;

    // Act & Assert
    var ex = Assert.Throws<DuplicateKeyException>(() => leafNode.TryInsert(duplicateKeyRow));

    Assert.Contains($"The key '{expectedKey}' already exists", ex.Message);

    // Finally, assert that the page state has not changed
    var headerAfter = new PageHeader(page);
    Assert.Equal(initialItemCount, headerAfter.ItemCount);

    var recordInPage = leafNode.Search(expectedKey);
    Assert.Equal(originalRow, recordInPage); // Verify original data is untouched
  }
}