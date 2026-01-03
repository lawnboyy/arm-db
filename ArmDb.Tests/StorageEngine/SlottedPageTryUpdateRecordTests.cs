using System.Text;
using ArmDb.Storage;

namespace ArmDb.UnitTests.StorageEngine;

public partial class SlottedPageTests
{
  [Fact]
  public void TryUpdateRecord_WhenNewDataIsSmaller_SucceedsInPlaceAndUpdatesSlotLength()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var originalData = Encoding.UTF8.GetBytes("Original Long Data"); // 18 bytes
    var newData = Encoding.UTF8.GetBytes("New Short"); // 9 bytes

    Assert.True(SlottedPage.TryAddRecord(page, originalData, 0)); // Add the initial record

    var headerBefore = new PageHeader(page);
    var slotBefore = ReadSlot(page, 0);

    // Act
    bool success = SlottedPage.TryUpdateRecord(page, 0, newData.AsSpan());

    // Assert
    Assert.True(success);

    var headerAfter = new PageHeader(page);
    var slotAfter = ReadSlot(page, 0);

    // 1. Header should be unchanged (no new items, no data heap movement)
    Assert.Equal(headerBefore.ItemCount, headerAfter.ItemCount);
    Assert.Equal(headerBefore.DataStartOffset, headerAfter.DataStartOffset);

    // 2. Slot offset should be the same, but length should be updated
    Assert.Equal(slotBefore.RecordOffset, slotAfter.RecordOffset);
    Assert.Equal(newData.Length, slotAfter.RecordLength);

    // 3. The new data should be at the original location
    var writtenData = page.GetReadOnlySpan(slotAfter.RecordOffset, slotAfter.RecordLength);
    Assert.True(newData.AsSpan().SequenceEqual(writtenData));
  }

  [Fact]
  public void TryUpdateRecord_WhenNewDataIsLarger_WithEnoughSpace_SucceedsOutOfPlace()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var originalData = Encoding.UTF8.GetBytes("Small"); // 5 bytes
    var newData = Encoding.UTF8.GetBytes("This is much larger data"); // 24 bytes

    Assert.True(SlottedPage.TryAddRecord(page, originalData, 0));

    var pageHeader = new PageHeader(page);
    var originalItemCount = pageHeader.ItemCount;
    var originalDataStartOffset = pageHeader.DataStartOffset;
    var slotBefore = ReadSlot(page, 0);

    // Act
    bool success = SlottedPage.TryUpdateRecord(page, 0, newData.AsSpan());

    // Assert
    Assert.True(success);

    var updatedItemCount = pageHeader.ItemCount;
    var updatedDataStartOffset = pageHeader.DataStartOffset;
    var slotAfter = ReadSlot(page, 0);

    // 1. Item count should be the same
    Assert.Equal(originalItemCount, updatedItemCount);
    // 2. Data heap should have grown (DataStartOffset is smaller)
    Assert.True(updatedDataStartOffset < originalDataStartOffset);
    Assert.Equal(originalDataStartOffset - newData.Length, updatedDataStartOffset);

    // 3. Slot offset should now point to a *new* location
    Assert.NotEqual(slotBefore.RecordOffset, slotAfter.RecordOffset);
    Assert.Equal(updatedDataStartOffset, slotAfter.RecordOffset); // Points to the new end of data heap
    Assert.Equal(newData.Length, slotAfter.RecordLength);

    // 4. The new data should be readable from the new location
    var writtenData = page.GetReadOnlySpan(slotAfter.RecordOffset, slotAfter.RecordLength);
    Assert.True(newData.AsSpan().SequenceEqual(writtenData));

    // 5. The old data location is now "garbage" but physically still there
    var garbageData = page.GetReadOnlySpan(slotBefore.RecordOffset, slotBefore.RecordLength);
    Assert.True(originalData.AsSpan().SequenceEqual(garbageData));
  }

  [Fact]
  public void TryUpdateRecord_WhenNewDataIsLarger_WithoutEnoughSpace_ReturnsFalse()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    // Fill the page so there's not enough room for the update to grow
    var item1 = new byte[4000];
    var item2 = new byte[4000];
    SlottedPage.TryAddRecord(page, item1, 0);
    SlottedPage.TryAddRecord(page, item2, 1);

    var newData = new byte[item1.Length + 100]; // 100 bytes larger than original
    var pageStateBefore = page.Data.ToArray(); // Snapshot the page state

    // Act
    bool success = SlottedPage.TryUpdateRecord(page, 0, newData.AsSpan());

    // Assert
    Assert.False(success, "Update should fail due to lack of space.");

    // Verify the page was not modified at all
    var pageStateAfter = page.Data.ToArray();
    Assert.True(pageStateBefore.SequenceEqual(pageStateAfter));
  }

  [Fact]
  public void TryUpdateRecord_OnDeletedSlot_ThrowsInvalidOperationException()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    SlottedPage.TryAddRecord(page, new byte[] { 1, 2, 3 }, 0);
    SlottedPage.DeleteRecord(page, 0); // Delete the record

    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>(() =>
        SlottedPage.TryUpdateRecord(page, 0, new byte[] { 4, 5, 6 }));
  }

  [Theory]
  [InlineData(-1)] // Negative index
  [InlineData(1)]  // Index equal to item count (out of bounds)
  public void TryUpdateRecord_WithInvalidIndex_ThrowsArgumentOutOfRangeException(int invalidIndex)
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    SlottedPage.TryAddRecord(page, new byte[] { 1, 2, 3 }, 0); // Page has one item at index 0

    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("slotIndex", () =>
        SlottedPage.TryUpdateRecord(page, invalidIndex, new byte[] { 4, 5, 6 }));
  }
}