using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
using ArmDb.Storage;

namespace ArmDb.Tests.Unit.Storage.BTreeTests;

public partial class BTreeLeafNodeTests
{
  [Fact]
  public void TryInsert_OnEmptyPage_Succeeds()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    var firstRow = new ArmDb.DataModel.Record(DataValue.CreateInteger(100), DataValue.CreateString("First Record"));

    // Act
    bool success = leafNode.TryInsert(firstRow);

    // Assert
    Assert.True(success);

    // Verify header is updated
    var header = new PageHeader(page);
    Assert.Equal(1, header.ItemCount);

    // Verify the record can be found and is correct
    var searchKey = new Key([DataValue.CreateInteger(100)]);
    var recordInPage = leafNode.Search(searchKey);
    Assert.NotNull(recordInPage);
    Assert.Equal(firstRow, recordInPage);
  }

  [Fact]
  public void TryInsert_WithNewSmallestKey_InsertsAtBeginning()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Pre-populate with existing records
    var row20 = new ArmDb.DataModel.Record(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));
    var row30 = new ArmDb.DataModel.Record(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30"));
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row20), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row30), 1);

    // The new row to insert has the smallest key
    var row10 = new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));

    // Act
    bool success = leafNode.TryInsert(row10);

    // Assert
    Assert.True(success);

    var header = new PageHeader(page);
    Assert.Equal(3, header.ItemCount);

    // Verify the new logical order: 10, 20, 30
    Assert.Equal(row10, leafNode.Search(new Key([DataValue.CreateInteger(10)])));
    Assert.Equal(row20, leafNode.Search(new Key([DataValue.CreateInteger(20)])));
    Assert.Equal(row30, leafNode.Search(new Key([DataValue.CreateInteger(30)])));
  }

  [Fact]
  public void TryInsert_WithNewLargestKey_AppendsToEnd()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Pre-populate with existing records
    var row10 = new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row20 = new ArmDb.DataModel.Record(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row10), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row20), 1);

    // The new row to insert has the largest key
    var row30 = new ArmDb.DataModel.Record(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30"));

    // Act
    bool success = leafNode.TryInsert(row30);

    // Assert
    Assert.True(success);

    var header = new PageHeader(page);
    Assert.Equal(3, header.ItemCount);

    // Verify the new logical order: 10, 20, 30
    Assert.Equal(row10, leafNode.Search(new Key([DataValue.CreateInteger(10)])));
    Assert.Equal(row20, leafNode.Search(new Key([DataValue.CreateInteger(20)])));
    Assert.Equal(row30, leafNode.Search(new Key([DataValue.CreateInteger(30)])));
  }

  [Fact]
  public void TryInsert_WhenSpaceAvailable_InsertsRecordInCorrectOrder()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Pre-populate the page with some records, leaving a gap
    var row10 = new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row30 = new ArmDb.DataModel.Record(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30"));

    // Use SlottedPage.TryAddItem for test setup to avoid dependency on the method under test
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row10), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row30), 1);

    // The new row to insert
    var row20 = new ArmDb.DataModel.Record(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));

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
    var originalRow = new ArmDb.DataModel.Record(DataValue.CreateInteger(100), DataValue.CreateString("Original Data"));
    leafNode.TryInsert(originalRow);

    // Create a new row with the SAME primary key but different data
    var duplicateKeyRow = new ArmDb.DataModel.Record(DataValue.CreateInteger(100), DataValue.CreateString("Duplicate Data"));
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

    var originalRow = new ArmDb.DataModel.Record(
        DataValue.CreateString("Sales"),
        DataValue.CreateInteger(901),
        DataValue.CreateBoolean(true)
    );
    leafNode.TryInsert(originalRow);

    // Create a new row with the SAME composite key but different non-key data
    var duplicateKeyRow = new ArmDb.DataModel.Record(
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

  [Fact]
  public void TryInsert_WhenPageIsFull_ReturnsFalseAndDoesNotModifyPage()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Manually fill the page to a known "almost full" state using lower-level helpers
    int availableSpace = SlottedPage.GetFreeSpace(page);
    // Calculate the maximum possible size for the variable part of our test row.
    // We need to account for all overhead: the slot, the null bitmap, the fixed-size 'Id' column,
    // and the length prefix for the 'Data' column. We want to leave 1 byte free.
    int overhead = Slot.Size + 1 /* null bitmap */ + sizeof(int) /* Id column */ + sizeof(int) /* Data length prefix */;
    int largeRecordDataSize = availableSpace - overhead - 1; // Leave 1 byte free

    var largeRow = new ArmDb.DataModel.Record(DataValue.CreateInteger(1), DataValue.CreateString(new string('x', largeRecordDataSize)));

    // Use SlottedPage.TryAddItem, which is already tested, for setup
    var serializedRow = RecordSerializer.Serialize(tableDef.Columns, largeRow);
    Assert.True(SlottedPage.TryAddRecord(page, serializedRow, 0));

    // At this point, GetFreeSpace() should be exactly 1 byte.
    Assert.Equal(1, SlottedPage.GetFreeSpace(page));

    // Capture a snapshot of the full page's state
    var pageStateBefore = page.Data.ToArray();

    // Create a new row that requires more than 1 byte of free space to insert
    var rowThatWontFit = new ArmDb.DataModel.Record(DataValue.CreateInteger(2), DataValue.CreateString("A"));

    // Act
    bool success = leafNode.TryInsert(rowThatWontFit);

    // Assert
    Assert.False(success, "TryInsert should return false when the page is full.");

    // Verify the page was not modified by the failed insert attempt
    var pageStateAfter = page.Data.ToArray();
    Assert.True(pageStateBefore.SequenceEqual(pageStateAfter), "Page content should not change after a failed insert.");
  }
}