using System.Text;
using ArmDb.DataModel;
using ArmDb.StorageEngine;
using ArmDb.StorageEngine.Exceptions;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeLeafNodeTests
{
  [Fact]
  public void TryUpdate_WhenNewDataIsSmaller_SucceedsInPlace()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // 1. Add an initial record with a relatively long string value
    var originalKey = new Key([DataValue.CreateInteger(100)]);
    var originalRow = new DataRow(originalKey.Values[0], DataValue.CreateString("Original Long Data"));
    Assert.True(leafNode.TryInsert(originalRow));

    // 2. Create the new row with the SAME key but SHORTER data
    var rowWithUpdate = new DataRow(originalKey.Values[0], DataValue.CreateString("New Short"));

    // 3. Capture page state before the update for comparison
    var headerBefore = new PageHeader(page);
    int initialItemCount = headerBefore.ItemCount;
    int initialDataStartOffset = headerBefore.DataStartOffset;

    // Act
    bool success = leafNode.TryUpdate(rowWithUpdate);

    // Assert
    Assert.True(success, "TryUpdate should succeed for an in-place update.");

    var headerAfter = new PageHeader(page);
    // 4. Verify page metadata hasn't changed for an in-place update
    Assert.Equal(initialItemCount, headerAfter.ItemCount);
    Assert.Equal(initialDataStartOffset, headerAfter.DataStartOffset);

    // 5. Verify the record was actually updated by searching for it
    DataRow? updatedRow = leafNode.Search(originalKey);
    Assert.NotNull(updatedRow);
    Assert.Equal(rowWithUpdate, updatedRow); // The new row should be what's stored
  }

  [Fact]
  public void TryUpdate_WhenNewDataIsLarger_WithEnoughSpace_SucceedsOutOfPlace()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // 1. Add an initial record with a short string value
    var originalKey = new Key([DataValue.CreateInteger(100)]);
    var originalRow = new DataRow(originalKey.Values[0], DataValue.CreateString("Small"));
    Assert.True(leafNode.TryInsert(originalRow));

    // 2. Create the new row with the SAME key but LARGER data
    var rowWithUpdate = new DataRow(originalKey.Values[0], DataValue.CreateString("This is now much larger data"));

    // 3. Capture the initial state values
    var headerBeforeView = new PageHeader(page);
    int initialItemCount = headerBeforeView.ItemCount;
    int initialDataStartOffset = headerBeforeView.DataStartOffset;

    // Act
    bool success = leafNode.TryUpdate(rowWithUpdate);

    // Assert
    Assert.True(success, "TryUpdate should succeed for an out-of-place update with enough space.");

    var headerAfter = new PageHeader(page);
    // 4. Verify page metadata has changed correctly
    Assert.Equal(initialItemCount, headerAfter.ItemCount); // Item count should not change
    Assert.True(headerAfter.DataStartOffset < initialDataStartOffset, "DataStartOffset should have decreased (heap grew).");

    // 5. Verify the record was updated by searching for it
    DataRow? updatedRow = leafNode.Search(originalKey);
    Assert.NotNull(updatedRow);
    Assert.Equal(rowWithUpdate, updatedRow);
  }

  [Fact]
  public void TryUpdate_WhenNewDataIsLarger_WithoutEnoughSpace_ReturnsFalse()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // 1. Fill the page almost completely, leaving a small amount of free space
    var smallRow = new DataRow(DataValue.CreateInteger(100), DataValue.CreateString("small"));
    var largeData = new byte[SlottedPage.GetFreeSpace(page) - RecordSerializer.Serialize(tableDef, smallRow).Length - Slot.Size - 50]; // Leave 50 bytes free
    var largeRow = new DataRow(DataValue.CreateInteger(200), DataValue.CreateString(Encoding.UTF8.GetString(largeData)));
    Assert.True(leafNode.TryInsert(smallRow));
    Assert.True(leafNode.TryInsert(largeRow));

    // 2. Create an update for the small row that is larger than the available free space
    var rowWithUpdate = new DataRow(DataValue.CreateInteger(100), DataValue.CreateString("This update is way too large to fit in the 50 bytes of free space"));

    // 3. Snapshot the page state before the failed update attempt
    var pageStateBefore = page.Data.ToArray();

    // Act
    bool success = leafNode.TryUpdate(rowWithUpdate);

    // Assert
    Assert.False(success, "Update should fail due to lack of space.");

    // Verify the page was not modified at all
    var pageStateAfter = page.Data.ToArray();
    Assert.True(pageStateBefore.SequenceEqual(pageStateAfter), "Page content should not be modified on a failed update.");
  }

  [Fact]
  public void TryUpdate_WhenRecordNotFound_ThrowsRecordNotFoundException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Add a record so the page isn't empty
    var existingRow = new DataRow(DataValue.CreateInteger(100), DataValue.CreateString("Existing Data"));
    Assert.True(leafNode.TryInsert(existingRow));

    // Create an update for a key that does not exist
    var rowWithUpdate = new DataRow(DataValue.CreateInteger(999), DataValue.CreateString("Non-existent record"));

    // Act & Assert
    Assert.Throws<RecordNotFoundException>(() => leafNode.TryUpdate(rowWithUpdate));
  }
}