using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeTests
{
  [Fact]
  public async Task SearchAsync_OnEmptyTree_ReturnsNull()
  {
    // Arrange
    var btree = await BTree.CreateAsync(_bpm, _tableDef);
    var searchKey = new Key([DataValue.CreateInteger(100)]);

    // Act
    var result = await btree.SearchAsync(searchKey);

    // Assert
    Assert.Null(result);
  }

  [Fact]
  public async Task SearchAsync_OnTreeWithOneRecord_FindsRecord()
  {
    // Arrange
    // 1. Create a new, empty BTree.
    var btree = await BTree.CreateAsync(_bpm, _tableDef);

    // 2. Manually prepare and insert the record to bypass InsertAsync
    var recordToInsert = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(100),
        DataValue.CreateString("Hello World")
    );
    var keyToInsert = recordToInsert.GetPrimaryKey(_tableDef);
    var recordBytes = RecordSerializer.Serialize(_tableDef.Columns, recordToInsert);

#if DEBUG
    // 2a. Fetch the root page (which was created by CreateAsync)
    var rootPageId = btree.GetRootPageIdForTest();
    var rootPage = await _bpm.FetchPageAsync(rootPageId);
    Assert.NotNull(rootPage);

    // 2b. Manually insert the data onto the page
    // (This simulates what InsertAsync would do)
    bool added = SlottedPage.TryAddRecord(rootPage, recordBytes, 0);
    Assert.True(added, "SlottedPage.TryAddRecord failed during test setup.");

    // 2c. Unpin the page, marking it as dirty
    await _bpm.UnpinPageAsync(rootPageId, isDirty: true);
#else
        // This test cannot run without the test hooks
        Assert.True(false, "This test requires DEBUG build with test hooks (GetRootPageIdForTest, GetFrameByPageId_TestOnly).");
#endif

    // Act
    // 3. Search for the key we just inserted.
    var result = await btree.SearchAsync(keyToInsert);

    // Assert
    // 4. Verify the correct record was returned.
    Assert.NotNull(result);
    Assert.Equal(recordToInsert, result);
  }

  [Fact]
  public async Task SearchAsync_OnNonEmptyTree_WhenKeyDoesNotExist_ReturnsNull()
  {
    // Arrange
    // 1. Create a new, empty BTree.
    var btree = await BTree.CreateAsync(_bpm, _tableDef);

    // 2. Manually prepare and insert a record
    var existingRecord = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(100),
        DataValue.CreateString("Hello World")
    );
    var recordBytes = RecordSerializer.Serialize(_tableDef.Columns, existingRecord);

#if DEBUG
    var rootPageId = btree.GetRootPageIdForTest();
    var rootPage = await _bpm.FetchPageAsync(rootPageId);
    Assert.NotNull(rootPage);
    bool added = SlottedPage.TryAddRecord(rootPage, recordBytes, 0);
    Assert.True(added, "Test setup failed to add record.");
    await _bpm.UnpinPageAsync(rootPageId, isDirty: true);
#else
        // This test requires the ability to manually insert data to set up the state
        // without relying on a working InsertAsync.
        Assert.True(false, "This test requires DEBUG build with test hooks (GetRootPageIdForTest, GetFrameByPageId_TestOnly).");
#endif

    // 3. Define a key that does NOT exist
    var searchKey = new Key([DataValue.CreateInteger(999)]);

    // Act
    // 4. Search for the non-existent key
    var result = await btree.SearchAsync(searchKey);

    // Assert
    // 5. Verify the result is null
    Assert.Null(result);
  }

  [Fact]
  public async Task SearchAsync_OnInvalidPageType_ThrowsInvalidDataException()
  {
    // Arrange
    // 1. Create a new BTree, which creates a valid LeafNode root page
    var btree = await BTree.CreateAsync(_bpm, _tableDef);
    var searchKey = new Key([DataValue.CreateInteger(100)]);

#if DEBUG
    // 2. Manually fetch the root page and corrupt its type
    var rootPageId = btree.GetRootPageIdForTest();
    var rootPage = await _bpm.FetchPageAsync(rootPageId);
    Assert.NotNull(rootPage);

    // 3. Corrupt the header to be an invalid type
    var header = new PageHeader(rootPage);
    header.PageType = PageType.Invalid; // Set to 0

    // 4. Unpin the corrupted page, marking it dirty
    await _bpm.UnpinPageAsync(rootPageId, isDirty: true);
#else
        Assert.True(false, "This test requires DEBUG build with test hooks.");
#endif

    // Act & Assert
    // 5. SearchAsync should now fail when it reads the corrupted header
    await Assert.ThrowsAsync<InvalidDataException>(() =>
        btree.SearchAsync(searchKey)
    );
  }

  [Fact]
  public async Task SearchAsync_OnTwoLevelTree_FindsRecordInLeaf()
  {
    // Create, initialize, and unpin the leaf pages
    var leafPage1 = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(leafPage1, PageType.LeafNode);
    await _bpm.UnpinPageAsync(leafPage1.Id, true);
    // Create 2 records for the 1st Page
    var page1Record1 = new ArmDb.DataModel.Record([DataValue.CreateInteger(10), DataValue.CreateString("Data 10")]);
    var page1Record2 = new ArmDb.DataModel.Record([DataValue.CreateInteger(20), DataValue.CreateString("Data 20")]);
    // Add the records to the leaf node
    var page1LeafNode = new BTreeLeafNode(leafPage1, _tableDef);
    page1LeafNode.TryInsert(page1Record1);
    page1LeafNode.TryInsert(page1Record2);


    var leafPage2 = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(leafPage2, PageType.LeafNode);
    await _bpm.UnpinPageAsync(leafPage2.Id, true);
    // Create 2 records for the second leaf page
    var page2Record1 = new ArmDb.DataModel.Record([DataValue.CreateInteger(30), DataValue.CreateString("Data 30")]);
    var page2Record2 = new ArmDb.DataModel.Record([DataValue.CreateInteger(40), DataValue.CreateString("Data 40")]);
    // Add the records to the leaf node
    var page2LeafNode = new BTreeLeafNode(leafPage2, _tableDef);
    page2LeafNode.TryInsert(page2Record1);
    page2LeafNode.TryInsert(page2Record2);

    // Create the root page
    var rootPage = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(rootPage, PageType.InternalNode);

    // Define the separator key
    var separatorKey = new Key([DataValue.CreateInteger(30)]);
    var childPageId = leafPage1.Id;
    var separatorKeyRecordBytes = BTreeInternalNode.SerializeRecord(separatorKey, childPageId, _tableDef);
    SlottedPage.TryAddRecord(rootPage, separatorKeyRecordBytes, 0);

    // Get page header for the root page, so we can set the rightmost pointer.
    var rootPageHeader = new PageHeader(rootPage);
    rootPageHeader.RightmostChildPageIndex = leafPage2.Id.PageIndex;

    // Now create the B-Tree
    var bTree = await BTree.CreateAsync(_bpm, _tableDef, rootPage.Id);

    // Key to search for...
    var searchKey = new Key([DataValue.CreateInteger(40)]);
    var expectedRecord = new ArmDb.DataModel.Record(DataValue.CreateInteger(40), DataValue.CreateString("Data 40"));

    // Now search the tree for the key and verify that we get back the expected record.
    var result = await bTree.SearchAsync(searchKey);

    Assert.NotNull(result);
    Assert.Equal(expectedRecord, result);
  }

  [Fact]
  public async Task SearchAsync_OnThreeLevelTree_FindsRecordsSuccessfully()
  {
    // Arrange
    // 1. Setup Leaves (Level 2)
    // We will create 9 leaf pages, numbered 0-8 logically.
    // Leaf 0: [10, 11]
    // Leaf 1: [20, 21]
    // ...
    // Leaf 8: [90, 91]
    var leafIds = new PageId[9];
    for (int i = 0; i < 9; i++)
    {
      var startKey = (i + 1) * 10;
      leafIds[i] = await CreateAndPopulateLeafPageAsync(
          new[] { startKey, startKey + 1 }
      );
    }

    // 2. Setup Internal Nodes (Level 1)
    // We will create 3 internal nodes, each covering 3 leaves.

    // Internal A: Points to Leaves 0, 1, 2. 
    // Separators: Key(20) -> Leaf 0, Key(30) -> Leaf 1. Rightmost -> Leaf 2.
    var internalA = await CreateInternalPageAsync(
        new[] { (20, leafIds[0]), (30, leafIds[1]) },
        rightmostChild: leafIds[2]
    );

    // Internal B: Points to Leaves 3, 4, 5.
    // Separators: Key(50) -> Leaf 3, Key(60) -> Leaf 4. Rightmost -> Leaf 5.
    var internalB = await CreateInternalPageAsync(
        new[] { (50, leafIds[3]), (60, leafIds[4]) },
        rightmostChild: leafIds[5]
    );

    // Internal C: Points to Leaves 6, 7, 8.
    // Separators: Key(80) -> Leaf 6, Key(90) -> Leaf 7. Rightmost -> Leaf 8.
    var internalC = await CreateInternalPageAsync(
        new[] { (80, leafIds[6]), (90, leafIds[7]) },
        rightmostChild: leafIds[8]
    );

    // 3. Setup Root Node (Level 0)
    // Points to Internal A, B, C.
    // Separators: Key(40) -> Internal A, Key(70) -> Internal B. Rightmost -> Internal C.
    // Note: Key 40 is chosen because it's > all keys in Internal A (max 31) and <= all keys in Internal B (min 40)
    // Actually, strict B+Tree logic: separator is the smallest key in the right-hand child.
    // Leaf 3 starts with 40. Leaf 6 starts with 70.
    var rootPageId = await CreateInternalPageAsync(
        new[] { (40, internalA), (70, internalB) },
        rightmostChild: internalC
    );

    // 4. Create BTree instance
    // Assuming you added an overload or internal constructor to take an existing rootPageId
    var btree = await BTree.CreateAsync(_bpm, _tableDef, rootPageId);

    // Act & Assert

    // Case 1: Left-most search (Leaf 0, Val 10)
    var result1 = await btree.SearchAsync(new Key([DataValue.CreateInteger(10)]));
    Assert.NotNull(result1);
    Assert.Equal("Data 10", result1.Values[1].GetAs<string>());

    // Case 2: Middle search (Leaf 4, Val 51)
    var result2 = await btree.SearchAsync(new Key([DataValue.CreateInteger(51)]));
    Assert.NotNull(result2);
    Assert.Equal("Data 51", result2.Values[1].GetAs<string>());

    // Case 3: Right-most search (Leaf 8, Val 91)
    var result3 = await btree.SearchAsync(new Key([DataValue.CreateInteger(91)]));
    Assert.NotNull(result3);
    Assert.Equal("Data 91", result3.Values[1].GetAs<string>());

    // Case 4: Not found (Range that would be in Leaf 1 but isn't there)
    var result4 = await btree.SearchAsync(new Key([DataValue.CreateInteger(25)]));
    Assert.Null(result4);
  }

  private async Task<PageId> CreateAndPopulateLeafPageAsync(int[] keys)
  {
    var page = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(page, PageType.LeafNode);

    var leafNode = new BTreeLeafNode(page, _tableDef);
    foreach (var k in keys)
    {
      var rec = new Record(DataValue.CreateInteger(k), DataValue.CreateString($"Data {k}"));
      leafNode.TryInsert(rec);
    }

    await _bpm.UnpinPageAsync(page.Id, true);
    return page.Id;
  }

  private async Task<PageId> CreateInternalPageAsync((int key, PageId ptr)[] entries, PageId rightmostChild)
  {
    var page = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(page, PageType.InternalNode);

    // We can use the BTreeInternalNode wrapper if we made InsertEntryForTest internal/public
    // Or use the raw SlottedPage.TryAddRecord logic you used in your test.
    // Using the raw logic to ensure we don't depend on BTreeInternalNode.Insert logic yet:

    foreach (var entry in entries)
    {
      var key = new Key([DataValue.CreateInteger(entry.key)]);
      var recordBytes = BTreeInternalNode.SerializeRecord(key, entry.ptr, _tableDef);

      // For test setup, we can just append to slot 0, 1, 2... since we are adding in order
      int nextSlot = new PageHeader(page).ItemCount;
      SlottedPage.TryAddRecord(page, recordBytes, nextSlot);
    }

    var header = new PageHeader(page);
    header.RightmostChildPageIndex = rightmostChild.PageIndex;

    await _bpm.UnpinPageAsync(page.Id, true);
    return page.Id;
  }
}