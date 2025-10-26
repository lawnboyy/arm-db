using System.Text;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class SlottedPageTests
{
  [Fact]
  public void GetRecord_WithValidIndex_ReturnsCorrectDataSpan()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var item1 = Encoding.UTF8.GetBytes("Item One");
    var item2 = Encoding.UTF8.GetBytes("Item Two is Longer");
    var item3 = Encoding.UTF8.GetBytes("Item 3");

    // Use TryAddItem to populate the page
    SlottedPage.TryAddRecord(page, item1, 0);
    SlottedPage.TryAddRecord(page, item2, 1);
    SlottedPage.TryAddRecord(page, item3, 2);

    // Act
    ReadOnlySpan<byte> record1Span = SlottedPage.GetRawRecord(page, 0);
    ReadOnlySpan<byte> record2Span = SlottedPage.GetRawRecord(page, 1);
    ReadOnlySpan<byte> record3Span = SlottedPage.GetRawRecord(page, 2);

    // Assert
    Assert.True(item1.AsSpan().SequenceEqual(record1Span));
    Assert.True(item2.AsSpan().SequenceEqual(record2Span));
    Assert.True(item3.AsSpan().SequenceEqual(record3Span));
  }

  [Fact]
  public void GetRecord_ForDeletedSlot_ReturnsEmptySpan()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var item1 = Encoding.UTF8.GetBytes("I will be deleted");
    var item2 = Encoding.UTF8.GetBytes("I will remain");
    SlottedPage.TryAddRecord(page, item1, 0);
    SlottedPage.TryAddRecord(page, item2, 1);

    // Manually "delete" the first record by getting its slot's physical offset
    // and writing a new length of 0.
    int slot0_physical_offset = PageHeader.HEADER_SIZE + (0 * Slot.Size);
    var original_slot0 = ReadSlot(page, 0); // Read the original slot to get its offset

    // Write a new slot with the same offset but a length of 0
    page.WriteInt32(slot0_physical_offset, original_slot0.RecordOffset);
    page.WriteInt32(slot0_physical_offset + sizeof(int), 0); // Set length to 0

    // Act
    ReadOnlySpan<byte> deletedRecordSpan = SlottedPage.GetRawRecord(page, 0);
    ReadOnlySpan<byte> existingRecordSpan = SlottedPage.GetRawRecord(page, 1);

    // Assert
    Assert.True(deletedRecordSpan.IsEmpty);
    // Also ensure the other record is still readable and correct
    Assert.True(item2.AsSpan().SequenceEqual(existingRecordSpan));
  }

  [Theory]
  [InlineData(-1)] // Negative index
  [InlineData(2)]  // Index equal to item count
  [InlineData(3)]  // Index greater than item count
  public void GetRecord_WithInvalidIndex_ThrowsArgumentOutOfRangeException(int invalidIndex)
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    SlottedPage.TryAddRecord(page, new byte[] { 1, 2 }, 0);
    SlottedPage.TryAddRecord(page, new byte[] { 3, 4 }, 1); // Page has 2 items (indices 0, 1)

    // Act & Assert
    var ex = Assert.Throws<ArgumentOutOfRangeException>("slotIndex", () =>
        SlottedPage.GetRawRecord(page, invalidIndex)
    );
    // Verify the exception message is helpful
    Assert.Contains($"Slot index {invalidIndex} is out of range.", ex.Message);
  }

  [Fact]
  public void GetRecord_OnEmptyPage_ThrowsArgumentOutOfRangeException()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode); // Page is empty, ItemCount is 0

    // Act & Assert
    var ex = Assert.Throws<ArgumentOutOfRangeException>("slotIndex", () =>
        SlottedPage.GetRawRecord(page, 0)
    );
    // Verify the specific message for an empty page
    Assert.Contains("The page is empty.", ex.Message);
  }

  [Fact]
  public void GetRecord_WithNullPage_ThrowsArgumentNullException()
  {
    // Arrange
    Page? nullPage = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("page", () =>
        // Use null-forgiving operator (!) as we are intentionally testing the null case
        SlottedPage.GetRawRecord(nullPage!, 0)
    );
  }
}