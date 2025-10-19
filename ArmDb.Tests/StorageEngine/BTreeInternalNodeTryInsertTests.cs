using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeInternalNodeTests
{
  [Fact]
  public void TryInsert_WhenSpaceAvailable_InsertsEntryInCorrectOrder()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Pre-populate the node with two entries: (Key:100 -> Ptr:10) and (Key:300 -> Ptr:30)
    // This sets up the structure: [ Ptr(10) | Key(100) | Ptr(30) | Key(300) | RightmostPtr(40) ]
    // conceptually, where Ptr(10) is stored with Key(100)'s entry, etc.
    internalNode.InsertEntryForTest(new Key([DataValue.CreateInteger(100)]), new PageId(1, 10));
    internalNode.InsertEntryForTest(new Key([DataValue.CreateInteger(300)]), new PageId(1, 30));
    var header = new PageHeader(page);
    header.RightmostChildPageIndex = 40;

    // The new entry to insert, which should go between the existing two.
    var keyToInsert = new Key([DataValue.CreateInteger(200)]);
    var pageIdToInsert = new PageId(1, 20);

    // Act
    bool success = internalNode.TryInsert(keyToInsert, pageIdToInsert);

    // Assert
    Assert.True(success, "TryInsert should succeed when space is available.");

    // 1. Verify the item count in the header has increased
    header = new PageHeader(page); // Re-create header view to get latest state
    Assert.Equal(3, header.ItemCount);

    // 2. Verify the entries are now in the correct logical order: 100, 200, 300
    //    This requires a way to read back entries for verification.
    var entry0 = internalNode.GetEntryForTest(0);
    var entry1 = internalNode.GetEntryForTest(1);
    var entry2 = internalNode.GetEntryForTest(2);

    Assert.Equal(new Key([DataValue.CreateInteger(100)]), entry0.key);
    Assert.Equal(new PageId(1, 10), entry0.childPageId);

    Assert.Equal(keyToInsert, entry1.key); // The new entry should be at index 1
    Assert.Equal(pageIdToInsert, entry1.childPageId);

    Assert.Equal(new Key([DataValue.CreateInteger(300)]), entry2.key); // The old entry from index 1 should be at index 2
    Assert.Equal(new PageId(1, 30), entry2.childPageId);

    // 3. Verify the RightmostChildPageIndex is unchanged
    Assert.Equal(40, header.RightmostChildPageIndex);
  }

  [Fact]
  public void TryInsert_WhenPageIsFull_ReturnsFalseAndDoesNotModifyPage()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Calculate how many entries will fit on a page
    var sampleKey = new Key([DataValue.CreateInteger(0)]);
    var samplePageId = new PageId(0, 0);
    var entryBytes = BTreeInternalNode.SerializeRecord(sampleKey, samplePageId, tableDef);
    int spacePerEntry = entryBytes.Length + Slot.Size;
    int maxEntries = (Page.Size - PageHeader.HEADER_SIZE) / spacePerEntry;

    // Fill the page completely with dummy entries
    for (int i = 0; i < maxEntries; i++)
    {
      var key = new Key([DataValue.CreateInteger(i * 10)]);
      var pageId = new PageId(1, i);
      // Use test helper for setup, assuming it inserts at the end
      internalNode.InsertEntryForTest(key, pageId);
    }

    // At this point, GetFreeSpace should be less than spacePerEntry
    Assert.True(SlottedPage.GetFreeSpace(page) < spacePerEntry);

    var pageStateBefore = page.Data.ToArray(); // Snapshot the page state

    // The entry that will fail to insert
    var keyToInsert = new Key([DataValue.CreateInteger(999)]);
    var pageIdToInsert = new PageId(1, 999);

    // Act
    bool success = internalNode.TryInsert(keyToInsert, pageIdToInsert);

    // Assert
    Assert.False(success, "TryInsert should return false when the page is full.");

    // Verify the page content was not modified by the failed insert
    var pageStateAfter = page.Data.ToArray();
    Assert.True(pageStateBefore.SequenceEqual(pageStateAfter), "Page content should not be modified on a failed insert.");
  }

  [Fact]
  public void TryInsert_WhenKeyAlreadyExists_ThrowsDuplicateKeyException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    var existingKey = new Key([DataValue.CreateInteger(100)]);
    var existingPageId = new PageId(1, 10);
    internalNode.InsertEntryForTest(existingKey, existingPageId);

    var pageStateBefore = page.Data.ToArray(); // Snapshot state

    var newPageIdForDuplicateKey = new PageId(1, 20);

    // Act & Assert
    var ex = Assert.Throws<DuplicateKeyException>(() =>
        internalNode.TryInsert(existingKey, newPageIdForDuplicateKey)
    );

    Assert.Contains($"The key '{existingKey}' already exists", ex.Message);

    // Verify the page was not modified
    var pageStateAfter = page.Data.ToArray();
    Assert.True(pageStateBefore.SequenceEqual(pageStateAfter), "Page should not be modified on a duplicate key error.");
  }

  [Fact]
  public void TryInsert_WithNewSmallestKey_InsertsAtBeginning()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Pre-populate with existing entries
    internalNode.InsertEntryForTest(new Key([DataValue.CreateInteger(100)]), new PageId(1, 10));
    internalNode.InsertEntryForTest(new Key([DataValue.CreateInteger(200)]), new PageId(1, 20));

    // The new entry has the smallest key
    var keyToInsert = new Key([DataValue.CreateInteger(50)]);
    var pageIdToInsert = new PageId(1, 5);

    // Act
    bool success = internalNode.TryInsert(keyToInsert, pageIdToInsert);

    // Assert
    Assert.True(success);
    var header = new PageHeader(page);
    Assert.Equal(3, header.ItemCount);

    // Verify the new entry is now at slot 0
    var entry0 = internalNode.GetEntryForTest(0);
    Assert.Equal(keyToInsert, entry0.key);
    Assert.Equal(pageIdToInsert, entry0.childPageId);

    // Verify the other entries were shifted right
    var entry1 = internalNode.GetEntryForTest(1);
    Assert.Equal(new Key([DataValue.CreateInteger(100)]), entry1.key);
  }

  [Fact]
  public void TryInsert_WithNewLargestKey_InsertsAtEnd()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Pre-populate with existing entries
    internalNode.InsertEntryForTest(new Key([DataValue.CreateInteger(100)]), new PageId(1, 10));
    internalNode.InsertEntryForTest(new Key([DataValue.CreateInteger(200)]), new PageId(1, 20));

    // The new entry has the largest key
    var keyToInsert = new Key([DataValue.CreateInteger(300)]);
    var pageIdToInsert = new PageId(1, 30);

    // Act
    bool success = internalNode.TryInsert(keyToInsert, pageIdToInsert);

    // Assert
    Assert.True(success);
    var header = new PageHeader(page);
    Assert.Equal(3, header.ItemCount);

    // Verify the new entry is now at the last slot (index 2)
    var entry2 = internalNode.GetEntryForTest(2);
    Assert.Equal(keyToInsert, entry2.key);
    Assert.Equal(pageIdToInsert, entry2.childPageId);
  }
}