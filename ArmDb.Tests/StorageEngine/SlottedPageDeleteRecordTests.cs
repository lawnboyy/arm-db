using System;
using System.Linq; // For SequenceEqual
using System.Text;
using Xunit;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class SlottedPageTests // Use partial to extend the existing SlottedPageTests class
{
  [Fact]
  public void DeleteRecord_WithValidIndex_NullifiesSlotAndKeepsData()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var item1 = Encoding.UTF8.GetBytes("Item One");
    var item2 = Encoding.UTF8.GetBytes("Item Two"); // The item to be deleted
    var item3 = Encoding.UTF8.GetBytes("Item Three");
    SlottedPage.TryAddItem(page, item1, 0);
    SlottedPage.TryAddItem(page, item2, 1);
    SlottedPage.TryAddItem(page, item3, 2);

    var headerBefore = new PageHeader(page);
    int initialItemCount = headerBefore.ItemCount;
    int initialDataStart = headerBefore.DataStartOffset;
    var slotToDeleteInfo = ReadSlot(page, 1); // Get info for slot 1 before deletion

    // Act
    SlottedPage.DeleteRecord(page, 1); // Delete the middle item

    // Assert
    var headerAfter = new PageHeader(page);
    // 1. Header metadata should NOT change on a simple delete (no compaction)
    Assert.Equal(initialItemCount, headerAfter.ItemCount);
    Assert.Equal(initialDataStart, headerAfter.DataStartOffset);

    // 2. Verify the targeted slot is now empty/invalid
    var deletedSlot = ReadSlot(page, 1);
    Assert.Equal(0, deletedSlot.RecordOffset);
    Assert.Equal(0, deletedSlot.RecordLength);

    // 3. Verify GetRecord now returns an empty span for the deleted slot
    Assert.True(SlottedPage.GetRecord(page, 1).IsEmpty);

    // 4. Verify other slots and their data are untouched
    var slot0 = ReadSlot(page, 0);
    Assert.Equal(item1.Length, slot0.RecordLength);
    Assert.True(item1.AsSpan().SequenceEqual(page.GetReadOnlySpan(slot0.RecordOffset, slot0.RecordLength)));

    var slot2 = ReadSlot(page, 2);
    Assert.Equal(item3.Length, slot2.RecordLength);
    Assert.True(item3.AsSpan().SequenceEqual(page.GetReadOnlySpan(slot2.RecordOffset, slot2.RecordLength)));

    // 5. Verify the underlying data bytes of the deleted record are still physically present (now "garbage")
    var originalDataSpan = page.GetReadOnlySpan(slotToDeleteInfo.RecordOffset, slotToDeleteInfo.RecordLength);
    Assert.True(item2.AsSpan().SequenceEqual(originalDataSpan));
  }

  [Theory]
  [InlineData(-1)] // Negative index
  [InlineData(2)]  // Index equal to item count
  [InlineData(3)]  // Index greater than item count
  public void DeleteRecord_WithInvalidIndex_ThrowsArgumentOutOfRangeException(int invalidIndex)
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    SlottedPage.TryAddItem(page, new byte[] { 1, 2 }, 0);
    SlottedPage.TryAddItem(page, new byte[] { 3, 4 }, 1); // Page has 2 items (indices 0, 1)

    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("slotIndex", () =>
        SlottedPage.DeleteRecord(page, invalidIndex)
    );
  }

  [Fact]
  public void DeleteRecord_WithNullPage_ThrowsArgumentNullException()
  {
    // Arrange
    Page? nullPage = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("page", () =>
        // Use null-forgiving operator (!) as we are intentionally testing the null case
        SlottedPage.DeleteRecord(nullPage!, 0)
    );
  }
}