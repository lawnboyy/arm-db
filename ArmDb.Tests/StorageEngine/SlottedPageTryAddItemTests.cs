using System;
using System.Text; // For Encoding
using System.Linq;
using Xunit;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class SlottedPageTests // Use partial to extend the existing SlottedPageTests class
{
  [Fact]
  public void TryAddItem_OnEmptyPage_Succeeds()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var itemData = Encoding.UTF8.GetBytes("Hello, World!");

    // Act
    bool success = SlottedPage.TryAddItem(page, itemData.AsSpan(), 0);

    // Assert
    Assert.True(success);

    var header = new PageHeader(page);
    Assert.Equal(1, header.ItemCount); // Item count should be 1
    Assert.Equal(Page.Size - itemData.Length, header.DataStartOffset); // DataStart should move

    // Verify the slot was written correctly
    var slot = ReadSlot(page, 0);
    Assert.Equal(header.DataStartOffset, slot.RecordOffset); // Slot offset should point to new data start
    Assert.Equal(itemData.Length, slot.RecordLength); // Slot length should match data length

    // Verify the data was written correctly
    var writtenData = page.GetReadOnlySpan(slot.RecordOffset, slot.RecordLength);
    Assert.True(itemData.AsSpan().SequenceEqual(writtenData));
  }

  [Fact]
  public void TryAddItem_AppendToEndOfSlots_Succeeds()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var item1Data = Encoding.UTF8.GetBytes("Item 1");
    var item2Data = Encoding.UTF8.GetBytes("The Second Item");

    SlottedPage.TryAddItem(page, item1Data, 0); // Add first item

    // Act: Append second item to the end (index 1)
    bool success = SlottedPage.TryAddItem(page, item2Data, 1);

    // Assert
    Assert.True(success);
    var header = new PageHeader(page);
    Assert.Equal(2, header.ItemCount);

    // Verify slot 1 (the new one)
    var slot1 = ReadSlot(page, 1);
    Assert.Equal(item2Data.Length, slot1.RecordLength);
    var writtenData2 = page.GetReadOnlySpan(slot1.RecordOffset, slot1.RecordLength);
    Assert.True(item2Data.AsSpan().SequenceEqual(writtenData2));

    // Verify slot 0 (the original one) is untouched
    var slot0 = ReadSlot(page, 0);
    Assert.Equal(item1Data.Length, slot0.RecordLength);
    var writtenData1 = page.GetReadOnlySpan(slot0.RecordOffset, slot0.RecordLength);
    Assert.True(item1Data.AsSpan().SequenceEqual(writtenData1));
  }

  [Fact]
  public void TryAddItem_InsertInMiddle_ShiftsSlotsAndSucceeds()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var item1Data = Encoding.UTF8.GetBytes("First");   // Will be at slot 0
    var item3Data = Encoding.UTF8.GetBytes("Third");   // Will be at slot 1 initially
    var item2Data = Encoding.UTF8.GetBytes("Second (Inserted)"); // Will be inserted at slot 1

    SlottedPage.TryAddItem(page, item1Data, 0);
    SlottedPage.TryAddItem(page, item3Data, 1); // item3 is at index 1

    var headerBefore = new PageHeader(page);
    Assert.Equal(2, headerBefore.ItemCount);
    int originalItem3Offset = ReadSlot(page, 1).RecordOffset;

    // Act: Insert item2 into the middle (at index 1)
    bool success = SlottedPage.TryAddItem(page, item2Data, 1);

    // Assert
    Assert.True(success);
    var headerAfter = new PageHeader(page);
    Assert.Equal(3, headerAfter.ItemCount);

    // Verify the new item at slot 1
    var slot1_new = ReadSlot(page, 1);
    Assert.Equal(item2Data.Length, slot1_new.RecordLength);
    Assert.True(item2Data.AsSpan().SequenceEqual(page.GetReadOnlySpan(slot1_new.RecordOffset, slot1_new.RecordLength)));

    // Verify the original item from slot 1 is now at slot 2 and its pointer is correct
    var slot2_moved = ReadSlot(page, 2);
    Assert.Equal(originalItem3Offset, slot2_moved.RecordOffset); // The offset pointer itself should be the same
    Assert.Equal(item3Data.Length, slot2_moved.RecordLength);
    Assert.True(item3Data.AsSpan().SequenceEqual(page.GetReadOnlySpan(slot2_moved.RecordOffset, slot2_moved.RecordLength)));
  }

  [Fact]
  public void TryAddItem_WhenNotEnoughSpace_ReturnsFalse()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    // Create an item that is almost the size of the whole page
    var largeItem = new byte[Page.Size - PageHeader.HEADER_SIZE - Slot.Size];
    var anotherItem = new byte[1]; // There won't be space for this + another slot
    Assert.True(SlottedPage.TryAddItem(page, largeItem, 0)); // This should succeed and fill the page

    var pageStateBefore = page.Data.ToArray(); // Snapshot the state

    // Act
    bool success = SlottedPage.TryAddItem(page, anotherItem, 1);

    // Assert
    Assert.False(success); // Should fail due to lack of space
    var pageStateAfter = page.Data.ToArray();
    Assert.True(pageStateBefore.SequenceEqual(pageStateAfter)); // Page should not have been modified
  }

  [Theory]
  [InlineData(-1)] // Index too low
  [InlineData(2)]  // Index too high (current count is 1)
  public void TryAddItem_InvalidIndex_ThrowsArgumentOutOfRangeException(int invalidIndex)
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    SlottedPage.TryAddItem(page, new byte[] { 1, 2, 3 }, 0); // Page now has 1 item (at index 0)

    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("indexToInsertAt", () =>
        SlottedPage.TryAddItem(page, new byte[] { 4, 5, 6 }, invalidIndex));
  }

  [Fact]
  public void TryAddItem_WithEmptyData_ThrowsArgumentException()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);

    // Act & Assert
    Assert.Throws<ArgumentException>("itemData", () =>
        SlottedPage.TryAddItem(page, ReadOnlySpan<byte>.Empty, 0));
  }
}