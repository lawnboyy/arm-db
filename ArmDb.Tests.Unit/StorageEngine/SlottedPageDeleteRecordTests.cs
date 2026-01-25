using System.Text;
using ArmDb.Storage;

namespace ArmDb.Tests.Unit.Storage;

public partial class SlottedPageTests
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
    SlottedPage.TryAddRecord(page, item1, 0);
    SlottedPage.TryAddRecord(page, item2, 1);
    SlottedPage.TryAddRecord(page, item3, 2);

    var headerBefore = new PageHeader(page);
    int initialItemCount = headerBefore.ItemCount;
    int initialDataStart = headerBefore.DataStartOffset;
    var slotToDeleteInfo = ReadSlot(page, 1); // Get info for slot 1 before deletion

    var slot3Before = ReadSlot(page, 2);

    // Act
    SlottedPage.DeleteRecord(page, 1); // Delete the middle item

    // Assert
    var headerAfter = new PageHeader(page);
    // 1. Header metadata SHOULD change on a simple delete (slots are compacted)
    Assert.Equal(initialItemCount - 1, headerAfter.ItemCount);
    Assert.Equal(initialDataStart, headerAfter.DataStartOffset);

    // 2. Verify the slot is shifted left
    var slot2After = ReadSlot(page, 1);
    Assert.Equal(slot3Before, slot2After);

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
    SlottedPage.TryAddRecord(page, [1, 2], 0);
    SlottedPage.TryAddRecord(page, [3, 4], 1); // Page has 2 items (indices 0, 1)

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

  [Fact]
  public void DeleteRecord_WhenCalled_CompactsSlotArrayAndDecrementsItemCount()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);

    var item1 = Encoding.UTF8.GetBytes("Item One");
    var item2 = Encoding.UTF8.GetBytes("Item Two (to be deleted)");
    var item3 = Encoding.UTF8.GetBytes("Item Three");
    var item4 = Encoding.UTF8.GetBytes("Item Four");

    SlottedPage.TryAddRecord(page, item1, 0);
    SlottedPage.TryAddRecord(page, item2, 1);
    SlottedPage.TryAddRecord(page, item3, 2);
    SlottedPage.TryAddRecord(page, item4, 3);

    // Capture state before deletion
    var headerBefore = new PageHeader(page);
    Assert.Equal(4, headerBefore.ItemCount); // Verify setup
                                             // Get the slot info for the items that should be shifted
    var slot2Before = ReadSlot(page, 2);
    var slot3Before = ReadSlot(page, 3);

    // Act
    SlottedPage.DeleteRecord(page, 1); // Delete the middle item, forcing slots 2 and 3 to shift left

    // Assert
    var headerAfter = new PageHeader(page);

    // 1. Verify the ItemCount in the header has been decremented
    Assert.Equal(3, headerAfter.ItemCount);

    // 2. Verify the slot array was compacted:
    //    The slot that was at index 2 should now be at index 1.
    var slot1After = ReadSlot(page, 1);
    Assert.Equal(slot2Before, slot1After);

    //    The slot that was at index 3 should now be at index 2.
    var slot2After = ReadSlot(page, 2);
    Assert.Equal(slot3Before, slot2After);

    // 3. Verify the data for the shifted slots is still correct and accessible at their new slot indices
    var recordAtSlot1After = SlottedPage.GetRawRecord(page, 1);
    Assert.True(item3.AsSpan().SequenceEqual(recordAtSlot1After));
    var recordAtSlot2After = SlottedPage.GetRawRecord(page, 2);
    Assert.True(item4.AsSpan().SequenceEqual(recordAtSlot2After));


    // 4. Verify the original first record is untouched
    var recordAtSlot0After = SlottedPage.GetRawRecord(page, 0);
    Assert.True(item1.AsSpan().SequenceEqual(recordAtSlot0After));

    // 5. Verify that accessing the old, now out-of-bounds slot index throws an exception
    Assert.Throws<ArgumentOutOfRangeException>("slotIndex", () => SlottedPage.GetRawRecord(page, 3));
  }
}