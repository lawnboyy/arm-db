using ArmDb.DataModel;
using ArmDb.Storage;

namespace ArmDb.UnitTests.StorageEngine.BTreeTests;

public partial class BTreeInternalNodeTests
{
  [Fact]
  public void Repopulate_WithValidData_CorrectlyWipesAndReloadsPage()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // 1. Add "original" data to be wiped
    var originalKey = new Key([DataValue.CreateInteger(50)]);
    var originalPageId = new PageId(1, 5);
    internalNode.InsertEntryForTest(originalKey, originalPageId);
    Assert.Equal(1, internalNode.ItemCount); // Verify setup

    // 2. Prepare the new, sorted list of entries
    var newKey100 = new Key([DataValue.CreateInteger(100)]);
    var newPageId10 = new PageId(1, 10);
    var newKey200 = new Key([DataValue.CreateInteger(200)]);
    var newPageId20 = new PageId(1, 20);

    var newRawEntries = new List<byte[]>
    {
      BTreeInternalNode.SerializeRecord(newKey100, newPageId10, tableDef),
      BTreeInternalNode.SerializeRecord(newKey200, newPageId20, tableDef)
    };

    // Act
    // This is the method you will implement
    internalNode.Repopulate(newRawEntries);

    // Assert
    // 1. Verify the item count is correct
    Assert.Equal(2, internalNode.ItemCount);

    // 2. Verify the old data is gone (by checking keys)
    var entries = internalNode.GetAllRawEntriesForTest(); // Need a helper for this
    Assert.DoesNotContain(entries, entry => entry.Key.Equals(originalKey));

    // 3. Verify the new data is present and in the correct order
    Assert.Equal(newKey100, entries[0].Key);
    Assert.Equal(newPageId10, entries[0].PageId);
    Assert.Equal(newKey200, entries[1].Key);
    Assert.Equal(newPageId20, entries[1].PageId);
  }

  [Fact]
  public void Repopulate_WithEmptyList_CorrectlyWipesPageAndSetsItemCountToZero()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // 1. Add "original" data to be wiped
    var originalKey = new Key([DataValue.CreateInteger(50)]);
    var originalPageId = new PageId(1, 5);
    internalNode.InsertEntryForTest(originalKey, originalPageId);
    Assert.Equal(1, internalNode.ItemCount);

    // 2. Prepare an empty list
    var emptyRawEntries = new List<byte[]>();

    // Act
    internalNode.Repopulate(emptyRawEntries);

    // Assert
    // 1. Verify the item count is now zero
    Assert.Equal(0, internalNode.ItemCount);

    // 2. Verify GetAllRawEntries returns an empty list
    Assert.Empty(internalNode.GetAllRawEntriesForTest());

    // 3. Verify the header is correctly reset
    var header = new PageHeader(page);
    Assert.Equal(0, header.ItemCount);
    Assert.Equal(Page.Size, header.DataStartOffset); // Data offset reset to end of page
  }

  [Fact]
  public void Repopulate_WhenDataIsTooLarge_FailsUpFrontAndDoesNotModifyPage()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // 1. Add "original" data to the page
    var originalKey = new Key([DataValue.CreateInteger(50)]);
    var originalPageId = new PageId(1, 5);
    internalNode.InsertEntryForTest(originalKey, originalPageId);

    // 2. Create a list of new entries that is too large to fit
    var largeString = new string('A', 1000); // Key data
    var newRawEntries = new List<byte[]>();
    int numEntries = (Page.Size / 1000) + 1; // Guarantee overflow

    for (int i = 0; i < numEntries; i++)
    {
      var key = new Key([DataValue.CreateInteger(i), DataValue.CreateString(largeString)]);
      // We need a complex table def for this, let's simplify.
      // Let's just create many simple entries.
    }

    // Re-arrange: Create a list of simple entries that is too large
    var simpleKey = new Key([DataValue.CreateInteger(1)]);
    var simplePageId = new PageId(1, 1);
    var simpleEntryBytes = BTreeInternalNode.SerializeRecord(simpleKey, simplePageId, tableDef);

    // Calculate max entries: (PageSize - HeaderSize) / (EntrySize + SlotSize)
    int maxEntries = (Page.Size - PageHeader.HEADER_SIZE) / (simpleEntryBytes.Length + Slot.Size);

    newRawEntries.Clear();
    for (int i = 0; i < maxEntries + 1; i++) // One more than can fit
    {
      newRawEntries.Add(simpleEntryBytes);
    }

    // 3. Snapshot the original page state
    var pageStateBefore = page.Data.ToArray();

    // Act & Assert
    // 4. Verify the method throws an exception
    var ex = Assert.Throws<InvalidOperationException>(() =>
        internalNode.Repopulate(newRawEntries)
    );

    Assert.Contains("Data for repopulating is too large to fit on a single page.", ex.Message, StringComparison.OrdinalIgnoreCase);

    // 5. CRUCIAL: Verify the page was not modified
    var pageStateAfter = page.Data.ToArray();
    Assert.True(pageStateBefore.SequenceEqual(pageStateAfter), "Page was modified despite data being too large.");

    // 6. Verify the original data is still present
    Assert.Equal(1, internalNode.ItemCount);
    Assert.Equal(originalKey, internalNode.GetAllRawEntriesForTest()[0].Key);
  }

  [Fact]
  public void Repopulate_WithNullList_ThrowsArgumentNullException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);
    List<byte[]>? nullRawEntries = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("sortedRawRecords", () =>
        internalNode.Repopulate(nullRawEntries!)
    );
  }

  [Fact]
  public void Repopulate_WhenCalled_PreservesParentPageIndex()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    int expectedParentIndex = 50; // A non-default parent index

    // Initialize the page with the specific parent index
    SlottedPage.Initialize(page, PageType.InternalNode, expectedParentIndex);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // 1. Add "original" data
    internalNode.InsertEntryForTest(new Key([DataValue.CreateInteger(10)]), new PageId(1, 1));
    Assert.Equal(expectedParentIndex, new PageHeader(page).ParentPageIndex); // Verify setup

    // 2. Prepare a new, valid list of records for repopulation
    var newKey = new Key([DataValue.CreateInteger(100)]);
    var newPageId = new PageId(1, 10);
    var newRawEntries = new List<byte[]>
        {
            BTreeInternalNode.SerializeRecord(newKey, newPageId, tableDef)
        };

    // Act
    internalNode.Repopulate(newRawEntries);

    // Assert
    // 1. Verify the data was repopulated
    Assert.Equal(1, internalNode.ItemCount);
    Assert.Equal(newKey, internalNode.GetAllRawEntriesForTest()[0].Key);

    // 2. CRUCIAL: Verify the parent page index was preserved
    var headerAfter = new PageHeader(page);
    Assert.Equal(expectedParentIndex, headerAfter.ParentPageIndex);
  }
}