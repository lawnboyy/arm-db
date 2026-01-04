using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.Storage;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.Storage.BTreeTests;

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
    _bpm.UnpinPage(rootPageId, isDirty: true);
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
    _bpm.UnpinPage(rootPageId, isDirty: true);
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
    _bpm.UnpinPage(rootPageId, isDirty: true);
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
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // Create, initialize, and unpin the leaf pages
    var leafPage1 = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(leafPage1, PageType.LeafNode);
    _bpm.UnpinPage(leafPage1.Id, true);
    // Create 2 records for the 1st Page
    var page1Record1 = new ArmDb.DataModel.Record([DataValue.CreateInteger(10), DataValue.CreateString("Data 10")]);
    var page1Record2 = new ArmDb.DataModel.Record([DataValue.CreateInteger(20), DataValue.CreateString("Data 20")]);
    // Add the records to the leaf node
    var page1LeafNode = new BTreeLeafNode(leafPage1, _tableDef);
    page1LeafNode.TryInsert(page1Record1);
    page1LeafNode.TryInsert(page1Record2);


    var leafPage2 = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(leafPage2, PageType.LeafNode);
    _bpm.UnpinPage(leafPage2.Id, true);
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
    // Set the root page index
    new PageHeader(tableHeaderPage).RootPageIndex = rootPage.Id.PageIndex;

    // Define the separator key
    var separatorKey = new Key([DataValue.CreateInteger(30)]);
    var childPageId = leafPage1.Id;
    var separatorKeyRecordBytes = BTreeInternalNode.SerializeRecord(separatorKey, childPageId, _tableDef);
    SlottedPage.TryAddRecord(rootPage, separatorKeyRecordBytes, 0);

    // Get page header for the root page, so we can set the rightmost pointer.
    var rootPageHeader = new PageHeader(rootPage);
    rootPageHeader.RightmostChildPageIndex = leafPage2.Id.PageIndex;

    // Now create the B-Tree
    var bTree = await BTree.CreateAsync(_bpm, _tableDef, rootPage.Id, tableHeaderPage);

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
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

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

    // Set the root page index
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;

    // 4. Create BTree instance
    // Assuming you added an overload or internal constructor to take an existing rootPageId
    var btree = await BTree.CreateAsync(_bpm, _tableDef, rootPageId, tableHeaderPage);

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

  [Fact]
  public async Task SearchAsync_OnFourLevelTree_FindsRecordInDeepLeaf()
  {
    // --- Construct a 4-Level Tree Bottom-Up ---
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // 1. Level 3: Leaf Nodes (8 Pages)
    // We create leaves covering ranges: [10..19], [20..29], ... [80..89]
    var leafIds = new PageId[8];
    for (int i = 0; i < 8; i++)
    {
      int startKey = (i + 1) * 10;
      // Example: Leaf 0 has keys 10, 15. Leaf 6 has keys 70, 75.
      leafIds[i] = await CreateAndPopulateLeafPageAsync(_tableDef, new[] { startKey, startKey + 5 });
    }

    // 2. Level 2: Internal Nodes (4 Pages)
    // Each L2 node points to 2 leaves.
    // L2_0: Covers < 30. (Sep 20 -> Leaf 0). Rightmost -> Leaf 1 (>= 20).
    var l2_0 = await CreateInternalPageAsync(_tableDef, new[] { (20, leafIds[0]) }, leafIds[1]);

    // L2_1: Covers >= 30, < 50. (Sep 40 -> Leaf 2). Rightmost -> Leaf 3 (>= 40).
    var l2_1 = await CreateInternalPageAsync(_tableDef, new[] { (40, leafIds[2]) }, leafIds[3]);

    // L2_2: Covers >= 50, < 70. (Sep 60 -> Leaf 4). Rightmost -> Leaf 5 (>= 60).
    var l2_2 = await CreateInternalPageAsync(_tableDef, new[] { (60, leafIds[4]) }, leafIds[5]);

    // L2_3: Covers >= 70. (Sep 80 -> Leaf 6). Rightmost -> Leaf 7 (>= 80).
    var l2_3 = await CreateInternalPageAsync(_tableDef, new[] { (80, leafIds[6]) }, leafIds[7]);

    // 3. Level 1: Internal Nodes (2 Pages)
    // Each L1 node points to 2 L2 nodes.
    // L1_0: Covers < 50. (Sep 30 -> L2_0). Rightmost -> L2_1 (>= 30).
    // Note: Separator 30 is chosen because L2_0 handles <30, L2_1 starts at 30.
    var l1_0 = await CreateInternalPageAsync(_tableDef, new[] { (30, l2_0) }, l2_1);

    // L1_1: Covers >= 50. (Sep 70 -> L2_2). Rightmost -> L2_3 (>= 70).
    var l1_1 = await CreateInternalPageAsync(_tableDef, new[] { (70, l2_2) }, l2_3);

    // 4. Level 0: Root Node (1 Page)
    // Root points to 2 L1 nodes.
    // Root: (Sep 50 -> L1_0). Rightmost -> L1_1 (>= 50).
    var rootPageId = await CreateInternalPageAsync(_tableDef, new[] { (50, l1_0) }, l1_1);
    // Set the root page index
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;

    // --- Instantiate BTree ---
    // Use the constructor that takes an existing root page
    // (Assuming you added `internal BTree(BufferPoolManager bpm, TableDefinition tableDef, PageId rootPageId)` constructor)
    var btree = await BTree.CreateAsync(_bpm, _tableDef, rootPageId, tableHeaderPage);

    // --- Search Target ---
    // We want to find key 75.
    // Path should be:
    // Root (75 >= 50) -> Rightmost (L1_1)
    // L1_1 (75 >= 70) -> Rightmost (L2_3)
    // L2_3 (75 < 80)  -> Left Ptr (Leaf 6)
    // Leaf 6 contains 70, 75. Match!
    var searchKey = new Key([DataValue.CreateInteger(75)]);
    var expectedRecord = new Record(DataValue.CreateInteger(75), DataValue.CreateString("Data_75"));

    // Act
    var result = await btree.SearchAsync(searchKey);

    // Assert
    Assert.NotNull(result);
    Assert.Equal(expectedRecord, result);
  }

  [Fact]
  public async Task SearchAsync_OnMultiLevelTree_BoundaryAndMissingKeys_ReturnsNull()
  {
    // Arrange
    // --- Construct a 4-Level Tree Bottom-Up ---
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(_tableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // 1. Level 3: Leaf Nodes (8 Pages)
    // We create leaves covering ranges: [10..19], [20..29], ... [80..89]
    var leafIds = new PageId[8];
    for (int i = 0; i < 8; i++)
    {
      int startKey = (i + 1) * 10;
      // Example: Leaf 0 has keys 10, 15. Leaf 6 has keys 70, 75.
      leafIds[i] = await CreateAndPopulateLeafPageAsync(_tableDef, new[] { startKey, startKey + 5 });
    }

    // 2. Level 2: Internal Nodes (4 Pages)
    // Each L2 node points to 2 leaves.
    // L2_0: Covers < 30. (Sep 20 -> Leaf 0). Rightmost -> Leaf 1 (>= 20).
    var l2_0 = await CreateInternalPageAsync(_tableDef, new[] { (20, leafIds[0]) }, leafIds[1]);

    // L2_1: Covers >= 30, < 50. (Sep 40 -> Leaf 2). Rightmost -> Leaf 3 (>= 40).
    var l2_1 = await CreateInternalPageAsync(_tableDef, new[] { (40, leafIds[2]) }, leafIds[3]);

    // L2_2: Covers >= 50, < 70. (Sep 60 -> Leaf 4). Rightmost -> Leaf 5 (>= 60).
    var l2_2 = await CreateInternalPageAsync(_tableDef, new[] { (60, leafIds[4]) }, leafIds[5]);

    // L2_3: Covers >= 70. (Sep 80 -> Leaf 6). Rightmost -> Leaf 7 (>= 80).
    var l2_3 = await CreateInternalPageAsync(_tableDef, new[] { (80, leafIds[6]) }, leafIds[7]);

    // 3. Level 1: Internal Nodes (2 Pages)
    // Each L1 node points to 2 L2 nodes.
    // L1_0: Covers < 50. (Sep 30 -> L2_0). Rightmost -> L2_1 (>= 30).
    // Note: Separator 30 is chosen because L2_0 handles <30, L2_1 starts at 30.
    var l1_0 = await CreateInternalPageAsync(_tableDef, new[] { (30, l2_0) }, l2_1);

    // L1_1: Covers >= 50. (Sep 70 -> L2_2). Rightmost -> L2_3 (>= 70).
    var l1_1 = await CreateInternalPageAsync(_tableDef, new[] { (70, l2_2) }, l2_3);

    // 4. Level 0: Root Node (1 Page)
    // Root points to 2 L1 nodes.
    // Root: (Sep 50 -> L1_0). Rightmost -> L1_1 (>= 50).
    var rootPageId = await CreateInternalPageAsync(_tableDef, new[] { (50, l1_0) }, l1_1);
    // Set the root page index
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;

    var btree = await BTree.CreateAsync(_bpm, _tableDef, rootPageId, tableHeaderPage);

    // Act & Assert

    // 1. Left-most boundary (Key < 10)
    var resultLow = await btree.SearchAsync(new Key([DataValue.CreateInteger(0)]));
    Assert.Null(resultLow);

    // 2. Right-most boundary (Key > 85)
    var resultHigh = await btree.SearchAsync(new Key([DataValue.CreateInteger(100)]));
    Assert.Null(resultHigh);

    // 3. Middle gap (e.g., 12 is not in Leaf 0 [10, 15])
    var resultGap = await btree.SearchAsync(new Key([DataValue.CreateInteger(12)]));
    Assert.Null(resultGap);
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

    _bpm.UnpinPage(page.Id, true);
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

    _bpm.UnpinPage(page.Id, true);
    return page.Id;
  }

  private async Task<PageId> CreateAndPopulateLeafPageAsync(TableDefinition tableDef, int[] keys)
  {
    // 1. Create page
    var page = await _bpm.CreatePageAsync(tableDef.TableId);

    // 2. Format as Leaf
    SlottedPage.Initialize(page, PageType.LeafNode);

    // 3. Insert Data
    var leafNode = new BTreeLeafNode(page, tableDef);
    foreach (var key in keys)
    {
      var record = new Record(DataValue.CreateInteger(key), DataValue.CreateString($"Data_{key}"));
      leafNode.TryInsert(record);
    }

    // 4. Unpin (mark dirty)
    _bpm.UnpinPage(page.Id, isDirty: true);
    return page.Id;
  }

  private async Task<PageId> CreateInternalPageAsync(TableDefinition tableDef, (int key, PageId ptr)[] entries, PageId rightmostChild)
  {
    // 1. Create page
    var page = await _bpm.CreatePageAsync(tableDef.TableId);

    // 2. Format as Internal
    SlottedPage.Initialize(page, PageType.InternalNode);

    // 3. Insert Entries
    // We use the lower-level SlottedPage.TryAddRecord to avoid needing the BTreeInternalNode.Insert logic here,
    // ensuring we test SearchAsync in isolation.
    int slotIndex = 0;
    foreach (var entry in entries)
    {
      var key = new Key([DataValue.CreateInteger(entry.key)]);
      // Assuming SerializeEntry is internal static on BTreeInternalNode
      var recordBytes = BTreeInternalNode.SerializeRecord(key, entry.ptr, tableDef);
      SlottedPage.TryAddRecord(page, recordBytes, slotIndex++);
    }

    // 4. Set Rightmost Pointer
    new PageHeader(page).RightmostChildPageIndex = rightmostChild.PageIndex;

    // 5. Unpin
    _bpm.UnpinPage(page.Id, isDirty: true);
    return page.Id;
  }
}