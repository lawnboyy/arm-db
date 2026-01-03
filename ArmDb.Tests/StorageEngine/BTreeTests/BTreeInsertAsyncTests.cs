using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
using ArmDb.SchemaDefinition;
using ArmDb.Storage;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.StorageEngine.BTreeTests;

public partial class BTreeTests
{
  [Fact]
  public async Task InsertAsync_OnEmptyTree_SucceedsAndRecordIsFound()
  {
    // Arrange
    // 1. Create a new, empty BTree.
    var btree = await BTree.CreateAsync(_bpm, _tableDef);

    // 2. The record to insert
    var recordToInsert = new Record(
        DataValue.CreateInteger(100),
        DataValue.CreateString("Hello World")
    );
    var keyToInsert = recordToInsert.GetPrimaryKey(_tableDef);

    // Act
    // 3. Insert the record. This should fit in the root leaf node without splitting.
    await btree.InsertAsync(recordToInsert);

    // Assert
    // 4. Verify the insertion by searching for it.
    var result = await btree.SearchAsync(keyToInsert);

    Assert.NotNull(result);
    Assert.Equal(recordToInsert, result);
  }

  [Fact]
  public async Task InsertAsync_WhenRootIsFull_SplitsRootAndGrowsTree()
  {
    // Arrange
    // 1. Define a table with a large column to force splits quickly
    //    (ID INT PK, LargeData VARCHAR(4000))
    var largeTableDef = new TableDefinition("LargeTable");
    largeTableDef.AddColumn(new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), false));
    largeTableDef.AddColumn(new ColumnDefinition("LargeData", new DataTypeInfo(PrimitiveDataType.Varchar, 4000), false));
    largeTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Large", new[] { "ID" }));

    var btree = await BTree.CreateAsync(_bpm, largeTableDef);
    var initialRootId = btree.GetRootPageIdForTest();

    // 2. Create records. 
    //    Page size is 8192. Header is ~32. Usable ~8160.
    //    We want to fill it. Let's use ~2500 bytes per record.
    //    Record overhead: Slot(8) + NullMap(1) + ID(4) + Len(4) = 17 bytes.
    //    Total per record = 2500 + 17 = 2517.
    //    3 records = 7551 bytes. (Fits)
    //    4 records = 10068 bytes. (Splits)
    string largeString = new string('A', 2500);

    var r1 = new Record(DataValue.CreateInteger(10), DataValue.CreateString(largeString));
    var r2 = new Record(DataValue.CreateInteger(20), DataValue.CreateString(largeString));
    var r3 = new Record(DataValue.CreateInteger(30), DataValue.CreateString(largeString));
    var r4 = new Record(DataValue.CreateInteger(40), DataValue.CreateString(largeString));

    // Act
    // Insert enough to fill the root but not split yet
    await btree.InsertAsync(r1);
    await btree.InsertAsync(r2);
    await btree.InsertAsync(r3);

    // Verify we haven't split yet
    Assert.Equal(initialRootId, btree.GetRootPageIdForTest());

    // INSERT #4: This should trigger the Root Split
    await btree.InsertAsync(r4);

    // Assert
    // 1. Verify the root page ID has CHANGED
    var newRootId = btree.GetRootPageIdForTest();
    Assert.NotEqual(initialRootId, newRootId);

    // 2. Verify the new root is an Internal Node
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(newRootId);
    Assert.NotNull(rootFrame);
    var rootHeader = new PageHeader(new Page(rootFrame.CurrentPageId, rootFrame.PageData));
    Assert.Equal(PageType.InternalNode, rootHeader.PageType);
    Assert.Equal(1, rootHeader.ItemCount); // Should have 1 separator key

    // 3. Verify we can still find all records (traversal works)
    var found1 = await btree.SearchAsync(r1.GetPrimaryKey(largeTableDef));
    var found2 = await btree.SearchAsync(r2.GetPrimaryKey(largeTableDef));
    var found3 = await btree.SearchAsync(r3.GetPrimaryKey(largeTableDef));
    var found4 = await btree.SearchAsync(r4.GetPrimaryKey(largeTableDef));

    Assert.NotNull(found1);
    Assert.NotNull(found2);
    Assert.NotNull(found3);
    Assert.NotNull(found4);

    // 4. Fetch the Table Header Page (Page Index 0)
    var headerPageId = new PageId(_tableDef.TableId, 0);

    // We fetch the page from the buffer pool to ensure we see the latest in-memory state
    // (The DiskManager might not have flushed it yet, which is expected)
    var tableHeaderPage = await _bpm.FetchPageAsync(headerPageId);
    Assert.NotNull(tableHeaderPage);
    try
    {
      var tableMetadataPageHeader = new PageHeader(tableHeaderPage);
      // 5. Verify the header points to the NEW root      
      Assert.Equal(newRootId.PageIndex, tableMetadataPageHeader.RootPageIndex);
    }
    finally
    {
      // Always unpin manually fetched pages
      _bpm.UnpinPage(headerPageId, false);
    }
  }

  [Fact]
  public async Task InsertAsync_LeafSplit_PropagatesToParent_NoParentSplit()
  {
    // Arrange
    // 1. Define schema with large column to force small fan-out (max 2 items per page)
    var largeTableDef = new TableDefinition("SplitPropTable");
    largeTableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    largeTableDef.AddColumn(new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Varchar, 2500), false));
    largeTableDef.AddConstraint(new PrimaryKeyConstraint("PK_SplitProp", new[] { "Id" }));
    string largeString = new string('X', 3000);

    // --- Construct 3-Level Tree ---
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(largeTableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // 2. Leaf Nodes
    // Leaf Target (Full): [10, 30]. Insert 20 will go here and force split.
    var leafTargetPageId = await ManualCreateLeaf(largeTableDef, new[] { 10, 30 }, largeString);
    // Leaf Sibling: [80]. Just to have a rightmost child.
    var leafRightSiblingPageId = await ManualCreateLeaf(largeTableDef, new[] { 80 }, largeString);

    // 3. Parent Internal Node (L1)
    // Structure: [ (50, leafTarget) ] -> Rightmost: leafSibling
    // Logic: Keys < 50 go to leafTarget. Keys >= 50 go to leafSibling.
    // This node has plenty of space (can hold hundreds of keys).
    var internalParentPageId = await ManualCreateInternal(largeTableDef, new[] { (50, leafTargetPageId) }, leafRightSiblingPageId);

    // Link Leaves to Parent
    await SetParentPointer(leafTargetPageId, internalParentPageId);
    await SetParentPointer(leafRightSiblingPageId, internalParentPageId);

    // 4. Root Node (L0)
    // Structure: [] -> Rightmost: parentPageId
    // Just a pointer down to the parent. All searches go to Rightmost.
    var rootPageId = await ManualCreateInternal(largeTableDef, new (int, PageId)[0], internalParentPageId);
    // Set the root page index
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;

    // Link Parent to Root
    await SetParentPointer(internalParentPageId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, largeTableDef, rootPageId, tableHeaderPage);

    // Act
    // Insert 20.
    // 1. Traversal finds leafTarget (since 20 < 50).
    // 2. leafTarget is full (holds 10, 30).
    // 3. Split: Sorted list [10, 20, 30]. Midpoint 20.
    //    - Left (Original): [10]
    //    - Right (New): [20, 30]
    //    - Promoted Key: 20.
    // 4. Propagate to Parent (L1).
    //    - Parent needs to insert (20, leafTarget) ? 
    //    - Logic: Parent had (50, leafTarget). leafTarget split.
    //    - Parent updates existing pointer for 50 to point to NewRight.
    //    - Parent inserts (20, leafTarget).
    //    - Result Parent: [ (20, leafTarget), (50, NewRight) ].
    var recordToInsert = new Record(DataValue.CreateInteger(20), DataValue.CreateString(largeString));
    await btree.InsertAsync(recordToInsert);

    // Assert
    // 1. Root should NOT have changed (no recursive split up to root)
    Assert.Equal(rootPageId, btree.GetRootPageIdForTest());

    // 2. Parent (L1) should now have 2 items instead of 1
    var parentFrame = _bpm.GetFrameByPageId_TestOnly(internalParentPageId);
    Assert.NotNull(parentFrame);
    var parentHeader = new PageHeader(new Page(parentFrame.CurrentPageId, parentFrame.PageData));
    Assert.Equal(2, parentHeader.ItemCount);

    // 3. Verify we can find all records (ensures pointers are correct)
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateInteger(10)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateInteger(20)]))); // New
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateInteger(30)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateInteger(80)])));
  }

  [Fact]
  public async Task InsertAsync_LeafAndParentSplit_PropagatesToRoot()
  {
    // Arrange
    // 1. Define schema with a LARGE Primary Key to force Internal Nodes to split easily.
    var hugeKeyTableDef = new TableDefinition("RecursiveSplitTable_HugePK");
    hugeKeyTableDef.AddColumn(new ColumnDefinition("KeyData", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    hugeKeyTableDef.AddColumn(new ColumnDefinition("Val", new DataTypeInfo(PrimitiveDataType.Int), false));
    hugeKeyTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Huge", ["KeyData"]));

    // Helper strings for keys (Length ~3000)
    string kA = new string('A', 3000);
    string kB = new string('B', 3000); // Insert Target
    string kC = new string('C', 3000);
    string kE = new string('E', 3000); // Separator 1
    string kF = new string('F', 3000);
    string kG = new string('G', 3000); // Separator 2
    string kM = new string('M', 3000); // Root Separator

    // --- Construct Tree Bottom-Up ---
    /*
       Initial Tree Structure (Keys Only):
       * = Rightmost Child Pointer

                                 [ M ]*
                                /     
                     [ E | G ]*       
                    /    |            
            [ A | C ]  [ F ]  
    */

    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(hugeKeyTableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // 2. Leaf Nodes
    // Leaf 1 (Target - Full): [A, C]. Inserting B will split it.
    var leaf1 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kA, kC });
    // Leaf 2: [F]
    var leaf2 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kF });

    // 3. Parent Internal Node (L1 - Full)
    // We want this node to cover the range A..H (keys < M).
    // Entries: (E, leaf1), (G, leaf2). Rightmost: leaf3.
    var parentNodeId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kE, leaf1), (kG, leaf2) }, null);

    // Link leaves to parent
    await SetParentPointer(leaf1, parentNodeId);
    await SetParentPointer(leaf2, parentNodeId);

    // 5. Root Node (L0 - Not Full)
    // Entries: (M, parentNodeId). Rightmost: siblingNodeId.
    // Logic: Keys < M (A..H) -> go to parentNodeId.
    //        Keys >= M (N..R) -> go to siblingNodeId.
    var rootPageId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kM, parentNodeId) },
        null);

    // Link L1s to Root
    await SetParentPointer(parentNodeId, rootPageId);
    // Set the root page index
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId, tableHeaderPage);
    var initialRootId = rootPageId;

    // Act
    // Insert "B".
    // 1. Leaf Split (A, B, C) -> Med: B. Promotes B.
    // 2. Parent Insert (B). Parent has (E, G). Sorted: B, E, G.
    // 3. Parent Split (B, E, G) -> Med: E. Promotes E.
    //    - Left Internal (Original): (B, leaf1). Rightmost: NewSiblingFromLeafSplit.
    //    - Right Internal (New): (G, leaf2). Rightmost: leaf3.
    // 4. Root Insert (E). Root has (M). Sorted: E, M.
    //    - Root should accept E.
    var recordToInsert = new Record(DataValue.CreateString(kB), DataValue.CreateInteger(1));
    await btree.InsertAsync(recordToInsert);

    /*
       Resulting Tree Structure (Keys Only):
       * = Rightmost Child Pointer

                                 [ E | M ]*
                                /    |    \
                               /     |     \
              .---------------'      |      '---------------.
              |                      |                      |
          [ B ]*                   [ G ]*                  
         /      \                
   [ A ]      [ B | C ]      
  (Leaf1)    (LeafNew)      
    */

    // Assert
    // 1. Root should NOT have changed (it had space)
    Assert.Equal(initialRootId, btree.GetRootPageIdForTest());

    // 2. Root should now have 2 entries: (E, ...) and (M, ...)
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(initialRootId);
    Assert.NotNull(rootFrame);
    var rootHeader = new PageHeader(new Page(rootFrame.CurrentPageId, rootFrame.PageData));
    Assert.Equal(2, rootHeader.ItemCount);

    // 3. Verify we can find the new record (B)
    var foundB = await btree.SearchAsync(new Key([DataValue.CreateString(kB)]));
    Assert.NotNull(foundB);
    Assert.Equal(1, foundB.Values[1].GetAs<int>());

    // 4. Verify neighbors
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kA)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kC)])));
  }

  [Fact]
  public async Task InsertAsync_LeafAndParentSplit_PromotesSameKeyToRoot()
  {
    // Arrange
    // 1. Define schema with a LARGE Primary Key to force Internal Nodes to split easily.
    var hugeKeyTableDef = new TableDefinition("RecursiveSplitTable_HugePK");
    hugeKeyTableDef.AddColumn(new ColumnDefinition("KeyData", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    hugeKeyTableDef.AddColumn(new ColumnDefinition("Val", new DataTypeInfo(PrimitiveDataType.Int), false));
    hugeKeyTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Huge", new[] { "KeyData" }));

    // Helper strings for keys (Length ~3000)
    string kA = new string('A', 3000);  // Separator 1
    string kB = new string('B', 3000);  // Insert Target
    string kC = new string('C', 3000);
    string kF = new string('F', 3000);  // Separator 1 (exists in leaf)
    string kG = new string('G', 3000);
    string kI = new string('I', 3000);  // Separator 2 (exists in leaf)
    string kL = new string('L', 3000);  // Root Separator
    string kN = new string('N', 3000);

    // --- Construct Tree Bottom-Up ---
    /*
       Initial Tree Structure (Keys Only):
       * = Rightmost Child Pointer

                                 [ L ]*
                                /      \
                          [F|I]*      [  ]*
                          /  |  \           \
                     [A|C] [F|G] [I]          [N]
    */

    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(hugeKeyTableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // 2. Leaf Nodes
    // Leaf Left (Target - Full): [A, C]. Inserting B will split it.
    var leafLeft = await ManualCreateLeaf(hugeKeyTableDef, new[] { kA, kC });
    // Leaf Middle: [F, G].
    var leafMiddle = await ManualCreateLeaf(hugeKeyTableDef, new[] { kF, kG });
    // Leaf Right: [I].
    var leafRight = await ManualCreateLeaf(hugeKeyTableDef, new[] { kI });
    // Sibling Leaf: [N].
    var siblingLeaf = await ManualCreateLeaf(hugeKeyTableDef, new[] { kN });

    // 3. Parent Internal Node (L1) - Full (Capacity 2)
    // Entries: (F, leafLeft), (I, leafMiddle). Rightmost: leafRight.
    // Logic:
    //   Keys < F -> leafLeft [A, C]
    //   Keys >= F, < I -> leafMiddle [F, G]
    //   Keys >= I -> leafRight [I]
    var parentNodeId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kF, leafLeft), (kI, leafMiddle) },
        leafRight);

    // Link leaves to parent
    await SetParentPointer(leafLeft, parentNodeId);
    await SetParentPointer(leafMiddle, parentNodeId);
    await SetParentPointer(leafRight, parentNodeId);

    // 4. Sibling Internal Node (L1) - Dummy
    var siblingNodeId = await ManualCreateInternal(hugeKeyTableDef,
        new (string, PageId)[0],
        siblingLeaf);
    await SetParentPointer(siblingLeaf, siblingNodeId);

    // 5. Root Node (L0) - Not Full
    // Entries: (L, parentNodeId). Rightmost: siblingNodeId.
    // Logic:
    //   Keys < L -> parentNodeId (covers A..I)
    //   Keys >= L -> siblingNodeId (covers N)
    var rootPageId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kL, parentNodeId) },
        siblingNodeId);

    // Set the root page index in the table header page.
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;

    await SetParentPointer(parentNodeId, rootPageId);
    await SetParentPointer(siblingNodeId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId, tableHeaderPage);
    var initialRootId = rootPageId;

    // Act
    // Insert B.
    var recordToInsert = new Record(DataValue.CreateString(kB), DataValue.CreateInteger(1));
    await btree.InsertAsync(recordToInsert);

    /*
       Resulting Tree Structure (Keys Only):
       * = Rightmost Child Pointer

                                 [ F | L ]*
                                /    |    \
                               /     |     \
              .---------------'      |      '------------.
              |                      |                   |
          [ B ]*                   [ I ]*              [   ]*
         /      \                /      \                    \
   [ A ]      [ B | C ]      [ F | G ] [ I ]               [ N ]
  (LeafL)    (LeafNew)      (LeafMid) (LeafR)             (SibLeaf)
    */

    // Assert
    // 1. Root should NOT have changed (didn't split)
    Assert.Equal(initialRootId, btree.GetRootPageIdForTest());

    // 2. Root should now have 2 entries.
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(initialRootId);
    var rootHeader = new PageHeader(new Page(rootFrame!.CurrentPageId, rootFrame.PageData));
    Assert.Equal(2, rootHeader.ItemCount);

    // 3. Verify searches find everything
    var foundB = await btree.SearchAsync(new Key([DataValue.CreateString(kB)]));
    Assert.NotNull(foundB);
    Assert.Equal(1, foundB.Values[1].GetAs<int>());

    var foundA = await btree.SearchAsync(new Key([DataValue.CreateString(kA)]));
    Assert.NotNull(foundA);

    var foundC = await btree.SearchAsync(new Key([DataValue.CreateString(kC)]));
    Assert.NotNull(foundC);

    var foundF = await btree.SearchAsync(new Key([DataValue.CreateString(kF)]));
    Assert.NotNull(foundF);

    var foundG = await btree.SearchAsync(new Key([DataValue.CreateString(kG)]));
    Assert.NotNull(foundG);

    var foundI = await btree.SearchAsync(new Key([DataValue.CreateString(kI)]));
    Assert.NotNull(foundI);

    var foundN = await btree.SearchAsync(new Key([DataValue.CreateString(kN)]));
    Assert.NotNull(foundN);
  }

  [Fact]
  public async Task InsertAsync_RecursiveSplit_Height4_PropagatesToRoot()
  {
    // Arrange
    // 1. Define schema with a LARGE Primary Key to force Internal Nodes to split easily.
    //    (Key VARCHAR(3000) PK, Data INT)
    //    Max capacity per page (Leaf or Internal) will be ~2 items.
    var hugeKeyTableDef = new TableDefinition("RecursiveSplitTable_Height4");
    hugeKeyTableDef.AddColumn(new ColumnDefinition("KeyData", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    hugeKeyTableDef.AddColumn(new ColumnDefinition("Val", new DataTypeInfo(PrimitiveDataType.Int), false));
    hugeKeyTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Huge", new[] { "KeyData" }));

    // Helper strings for keys (Length ~3000)
    string kA = new string('A', 3000);
    string kB = new string('B', 3000); // Insert Target
    string kC = new string('C', 3000);
    string kE = new string('E', 3000); // L2 Sep 1
    string kF = new string('F', 3000);
    string kG = new string('G', 3000); // L2 Sep 2
    string kH = new string('H', 3000);
    string kI = new string('I', 3000); // L1 Sep 1
    string kJ = new string('J', 3000);
    string kK = new string('K', 3000); // L1 Sep 2
    string kM = new string('M', 3000);
    string kN = new string('N', 3000); // Root Sep
    string kO = new string('O', 3000);

    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(hugeKeyTableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // --- Level 3 (Leaves) ---
    // L3_1 (Target - Full): [A, C]. Inserting B will split it.
    var leaf1 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kA, kC });
    // L3_2: [F]
    var leaf2 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kF });
    // L3_3: [H]
    var leaf3 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kH });
    // L3_4: [J]
    var leaf4 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kJ });
    // L3_5: [M]
    var leaf5 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kM });
    // L3_6: [O]
    var leaf6 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kO });

    // --- Level 2 (Internal) ---
    // L2_1 (Full): Entries (E, leaf1), (G, leaf2). Rightmost: leaf3.
    // Range: < E (A,C); >= E < G (F); >= G (H).
    var l2_1 = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kE, leaf1), (kG, leaf2) },
        leaf3);
    await SetParentPointer(leaf1, l2_1);
    await SetParentPointer(leaf2, l2_1);
    await SetParentPointer(leaf3, l2_1);

    // L2_2 (Empty-ish): Rightmost leaf4.
    // Range: < K (J).
    var l2_2 = await ManualCreateInternal(hugeKeyTableDef, new (string, PageId)[0], leaf4);
    await SetParentPointer(leaf4, l2_2);

    // L2_3 (Empty-ish): Rightmost leaf5.
    // Range: < N (M).
    var l2_3 = await ManualCreateInternal(hugeKeyTableDef, new (string, PageId)[0], leaf5);
    await SetParentPointer(leaf5, l2_3);

    // L2_4 (Empty-ish): Rightmost leaf6.
    // Range: >= N (O).
    var l2_4 = await ManualCreateInternal(hugeKeyTableDef, new (string, PageId)[0], leaf6);
    await SetParentPointer(leaf6, l2_4);

    // --- Level 1 (Internal) ---
    // L1_1 (Full): Entries (I, l2_1), (K, l2_2). Rightmost: l2_3.
    // Range: < I (A..H); >= I < K (J); >= K (M).
    var l1_1 = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kI, l2_1), (kK, l2_2) },
        l2_3);
    await SetParentPointer(l2_1, l1_1);
    await SetParentPointer(l2_2, l1_1);
    await SetParentPointer(l2_3, l1_1);

    // L1_2: Rightmost l2_4.
    // Range: >= N (O).
    var l1_2 = await ManualCreateInternal(hugeKeyTableDef, new (string, PageId)[0], l2_4);
    await SetParentPointer(l2_4, l1_2);

    // --- Root (L0) ---
    // Root (Not Full): Entries (N, l1_1). Rightmost: l1_2.
    // Range: < N (A..M); >= N (O).
    var rootPageId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kN, l1_1) },
        l1_2);
    // Set the root page index in the table header page.
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;
    await SetParentPointer(l1_1, rootPageId);
    await SetParentPointer(l1_2, rootPageId);

    /*
       Initial Tree Structure (Keys Only):
       * = Rightmost Child Pointer

                     [ N ]*             (Root L0)
                    /      \
             [ I | K ]*    [ ]*              (L1)
            /    |     \        \
       [E|G]*   [ ]*   [ ]*     [ ]*         (L2)
       / | \       \       \        \
    [AC][F][H]     [J]     [M]      [O]   (Leaves)
    */

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId, tableHeaderPage);
    var initialRootId = rootPageId;

    // Act
    // Insert B.
    // 1. Leaf Split (A, B, C) -> Med: B. Promotes B.
    // 2. L2_1 Insert: (E->NewLeaf), Insert (B, leaf1).
    //    L2_1 [ (B, leaf1), (E, NewLeaf), (G, leaf2) ]. FULL.
    //    Split L2_1: Median E. Left [(B, leaf1)], Right [(G, leaf2)]. Promoted E.
    // 3. L1_1 Insert: (I->L2_1_Right), Insert (E, L2_1_Left).
    //    L1_1 [ (E, L2_1_Left), (I, L2_1_Right), (K, L2_2) ]. FULL.
    //    Split L1_1: Median I. Left [(E, ...)], Right [(K, ...)]. Promoted I.
    // 4. Root Insert: (N->L1_1_Right), Insert (I, L1_1_Left).
    //    Root [ (I, L1_1_Left), (N, L1_1_Right) ].
    var recordToInsert = new Record(DataValue.CreateString(kB), DataValue.CreateInteger(1));
    await btree.InsertAsync(recordToInsert);

    /*
       Resulting Tree Structure (Keys Only):
       * = Rightmost Child Pointer

                        [  I |  N  ]*                 (Root L0)
                       /        |     \
                   [E]*        [K]*    [ ]*                (L1)
                   /   \      /    \       \
               [B]*    [G]*  []*  []*      []*             (L2)
              /    \   /   \   \     \        \
          [A]    [BC] [F]  [H] [J]   [M]      [O]   (Leaves L3)
    */

    // Assert
    // 1. Root should NOT have changed ID
    Assert.Equal(initialRootId, btree.GetRootPageIdForTest());

    // 2. Root should now have 2 entries: (I, N)
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(initialRootId);
    var rootHeader = new PageHeader(new Page(rootFrame!.CurrentPageId, rootFrame.PageData));
    Assert.Equal(2, rootHeader.ItemCount);

    // 3. Verify B found
    var foundB = await btree.SearchAsync(new Key([DataValue.CreateString(kB)]));
    Assert.NotNull(foundB);
    Assert.Equal(1, foundB.Values[1].GetAs<int>());

    // 4. Verify neighbors
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kA)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kC)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kF)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kH)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kJ)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kM)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kO)])));
  }

  [Fact]
  public async Task InsertAsync_LeafToRootSplit_PropagatesAndGrowsTree()
  {
    // Arrange
    // 1. Schema: Key VARCHAR(3000). Max 2 items per page.
    var hugeKeyTableDef = new TableDefinition("RecursiveSplitTable_RootSplit");
    hugeKeyTableDef.AddColumn(new ColumnDefinition("KeyData", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    hugeKeyTableDef.AddColumn(new ColumnDefinition("Val", new DataTypeInfo(PrimitiveDataType.Int), false));
    hugeKeyTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Huge", new[] { "KeyData" }));

    // Keys for the tree structure
    // Target Path: A, B, C, E, F, G
    // Siblings: H, I, J, K, L, M, N
    string kA = new string('A', 3000);
    string kB = new string('B', 3000); // Insert Target
    string kC = new string('C', 3000);
    string kE = new string('E', 3000); // L1 Sep 1
    string kF = new string('F', 3000);
    string kG = new string('G', 3000); // L1 Sep 2
    string kH = new string('H', 3000);
    string kI = new string('I', 3000); // Root Sep 1
    string kJ = new string('J', 3000);
    string kK = new string('K', 3000);
    string kL = new string('L', 3000);
    string kM = new string('M', 3000); // Root Sep 2
    string kN = new string('N', 3000);

    // --- Construct Tree Bottom-Up ---
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(hugeKeyTableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // 1. Leaf Nodes (L2)
    // L2_Target (Full): [A, C]. Insert B -> Split.
    var l2_Target = await ManualCreateLeaf(hugeKeyTableDef, new[] { kA, kC });
    var l2_F = await ManualCreateLeaf(hugeKeyTableDef, new[] { kF });
    var l2_H = await ManualCreateLeaf(hugeKeyTableDef, new[] { kH });
    var l2_J = await ManualCreateLeaf(hugeKeyTableDef, new[] { kJ });
    var l2_L = await ManualCreateLeaf(hugeKeyTableDef, new[] { kL });
    var l2_N = await ManualCreateLeaf(hugeKeyTableDef, new[] { kN });

    // 2. Internal Nodes (L1)
    // L1_Target (Full): [ (E, l2_Target), (G, l2_F) ]. Rightmost: l2_H.
    // Range: < E (A,C); E..G (F); >= G (H).
    var l1_Target = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kE, l2_Target), (kG, l2_F) },
        l2_H);
    await SetParentPointer(l2_Target, l1_Target);
    await SetParentPointer(l2_F, l1_Target);
    await SetParentPointer(l2_H, l1_Target);

    // L1_Sibling1: [ (K, l2_J) ]. Rightmost: l2_L.
    // Range: < K (J); >= K (L).
    var l1_Sibling1 = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kK, l2_J) },
        l2_L);
    await SetParentPointer(l2_J, l1_Sibling1);
    await SetParentPointer(l2_L, l1_Sibling1);

    // L1_Sibling2: [ ]. Rightmost: l2_N.
    var l1_Sibling2 = await ManualCreateInternal(hugeKeyTableDef,
        new (string, PageId)[0],
        l2_N);
    await SetParentPointer(l2_N, l1_Sibling2);


    // 3. Root Node (L0) - Full
    // Entries: [ (I, l1_Target), (M, l1_Sibling1) ]. Rightmost: l1_Sibling2.
    // Range: < I (A..H); I..M (J..L); >= M (N).
    var rootPageId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kI, l1_Target), (kM, l1_Sibling1) },
        l1_Sibling2);
    // Set the root page index in the table header page.
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;
    await SetParentPointer(l1_Target, rootPageId);
    await SetParentPointer(l1_Sibling1, rootPageId);
    await SetParentPointer(l1_Sibling2, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId, tableHeaderPage);
    var initialRootId = rootPageId;

    /*
       Initial Tree Structure:
                     [ I | M ]*         (Root L0 - Full)
                    /    |    \
         [ E | G ]*    [ K ]* [ ]*                  (L1)
        /   |    \     /   \      \
     [AC]  [F]  [H]  [J]   [L]    [N]               (L2)
    */

    // Act
    // Insert B.
    // 1. L2_Target splits [A, B, C]. Promotes B.
    // 2. L1_Target splits [B, E, G]. Promotes E.
    // 3. Root splits [E, I, M]. Promotes I.
    // 4. New Root created with I.
    var recordToInsert = new Record(DataValue.CreateString(kB), DataValue.CreateInteger(1));
    await btree.InsertAsync(recordToInsert);

    /*
       Resulting Tree Structure:
             [ I ]*  (New Root)
            /     \
       [ E ]* [ M ]*   (New L1s)
       /   \    /   \
    ...   ...    ...   ...
    */

    // Assert
    // 1. Root Page ID should have CHANGED (new root created)
    var newRootId = btree.GetRootPageIdForTest();
    Assert.NotEqual(initialRootId, newRootId);

    // 2. New Root should be Internal and have 1 item (the promoted key "I")
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(newRootId);
    var rootHeader = new PageHeader(new Page(rootFrame!.CurrentPageId, rootFrame.PageData));
    Assert.Equal(PageType.InternalNode, rootHeader.PageType);
    Assert.Equal(1, rootHeader.ItemCount);

    // 3. Verify B found (and data path is valid)
    var foundB = await btree.SearchAsync(new Key([DataValue.CreateString(kB)]));
    Assert.NotNull(foundB);

    // 4. Verify other keys to ensure tree integrity
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kA)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kN)])));

    // 5. Fetch the Table Header Page (Page Index 0) and verify the root pointer was properly updated
    var headerPageId = new PageId(_tableDef.TableId, 0);
    Assert.NotNull(tableHeaderPage);
    var tableMetadataPageHeader = new PageHeader(tableHeaderPage);
    // 5. Verify the header points to the NEW root      
    Assert.Equal(newRootId.PageIndex, tableMetadataPageHeader.RootPageIndex);
  }

  [Fact]
  public async Task InsertAsync_LeafSplit_PromotesLargestKey_ToParent()
  {
    // Arrange
    // 1. Schema: Key VARCHAR(3000). Max 2 items per page.
    var hugeKeyTableDef = new TableDefinition("RecursiveSplitTable_HugePK");
    hugeKeyTableDef.AddColumn(new ColumnDefinition("KeyData", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    hugeKeyTableDef.AddColumn(new ColumnDefinition("Val", new DataTypeInfo(PrimitiveDataType.Int), false));
    hugeKeyTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Huge", new[] { "KeyData" }));

    // Keys: A < B < E < F < G < H < I
    string kA = new string('A', 3000); // Leaf Left
    string kB = new string('B', 3000); // Parent Key 1
    string kE = new string('E', 3000); // Leaf Target
    string kF = new string('F', 3000); // Inserted -> Promoted
    string kG = new string('G', 3000); // Leaf Target
    string kH = new string('H', 3000); // Root Separator
    string kI = new string('I', 3000); // Right Sibling Leaf

    // --- Construct Tree ---
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(hugeKeyTableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // 2. Leaf Nodes
    var leafLeft = await ManualCreateLeaf(hugeKeyTableDef, new[] { kA });
    // Target Leaf: [E, G]. Full (capacity 2).
    var leafTarget = await ManualCreateLeaf(hugeKeyTableDef, new[] { kE, kG });
    // Right Sibling Leaf
    var leafRight = await ManualCreateLeaf(hugeKeyTableDef, new[] { kI });

    // 3. Parent Internal Node (L1) - Not Full (1 entry)
    // Entries: (B, leafLeft). Rightmost: leafTarget.
    // Range Logic:
    //   Keys < B -> leafLeft [A]
    //   Keys >= B -> leafTarget [E, G]
    var parentNodeId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kB, leafLeft) },
        leafTarget);

    await SetParentPointer(leafLeft, parentNodeId);
    await SetParentPointer(leafTarget, parentNodeId);

    // 4. Sibling Internal Node (L1) - Dummy for Root structure
    var siblingNodeId = await ManualCreateInternal(hugeKeyTableDef, new (string, PageId)[0], leafRight);
    await SetParentPointer(leafRight, siblingNodeId);

    // 5. Root Node (L0)
    // Entries: (H, parentNodeId). Rightmost: siblingNodeId.
    // Logic: Keys < H -> parentNodeId. Keys >= H -> siblingNodeId.
    var rootPageId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kH, parentNodeId) },
        siblingNodeId);
    // Set the root page index in the table header page.
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;

    await SetParentPointer(parentNodeId, rootPageId);
    await SetParentPointer(siblingNodeId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId, tableHeaderPage);

    /*
       Initial Tree Structure:
                     [ H ]* (Root)
                    /      \
             [ B ]*       [   ]* (Sibling)
            /      \           \
         [A]      [E|G]        [I] (Leaves)
    */

    // Act
    // Insert F.
    // 1. Traversal: Root -> Parent -> Rightmost (leafTarget).
    // 2. LeafTarget: Insert F. [E, F, G]. Split. Median F.
    //    - Left: [E]. Right: [F, G]. Promoted: F. (Note: F is first key in new right node)
    // 3. Parent Update:
    //    - Update Rightmost ptr to NewRight (containing F, G).
    //    - Insert (F, leafTarget) (containing E).
    //    - List: B, F.
    var recordToInsert = new Record(DataValue.CreateString(kF), DataValue.CreateInteger(1));
    await btree.InsertAsync(recordToInsert);

    /*
       Resulting Tree Structure:
                     [ H ]* (Root)
                    /      \
             [ B | F ]*   [   ]* (Sibling)
            /    |    \         \
         [A]    [E]  [F|G]     [I]
    */

    // Assert
    // 1. Verify Parent Node Content
    var parentFrame = _bpm.GetFrameByPageId_TestOnly(parentNodeId);
    // Re-wrap to access internal node methods
    var parentNode = new BTreeInternalNode(new Page(parentFrame!.CurrentPageId, parentFrame.PageData), hugeKeyTableDef);

    Assert.Equal(2, parentNode.ItemCount);

    // Entry 0: B -> leafLeft
    var entry0 = parentNode.GetEntryForTest(0);
    Assert.Equal(new Key([DataValue.CreateString(kB)]), entry0.key);
    Assert.Equal(leafLeft, entry0.childPageId);

    // Entry 1: F -> leafTarget (The Left half of the split leaf)
    var entry1 = parentNode.GetEntryForTest(1);
    Assert.Equal(new Key([DataValue.CreateString(kF)]), entry1.key);
    Assert.Equal(leafTarget, entry1.childPageId);

    // Rightmost: New Sibling (The Right half of the split leaf)
    // We verify this by searching for F or G, which should route through the rightmost pointer.

    // 2. Verify all records found
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kA)])));

    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kE)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kG)])));

    var foundF = await btree.SearchAsync(new Key([DataValue.CreateString(kF)]));
    Assert.NotNull(foundF);
    Assert.Equal(1, foundF.Values[1].GetAs<int>());

    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kI)])));
  }

  [Fact]
  public async Task InsertAsync_DuplicateKey_ThrowsDuplicateKeyException()
  {
    // Arrange
    var tableDef = CreateIntPKTable(999);
    var btree = await BTree.CreateAsync(_bpm, tableDef);

    var record = new Record(DataValue.CreateInteger(100), DataValue.CreateString("Original"));

    // 1. Initial Insert
    await btree.InsertAsync(record);

    // Act & Assert
    // 2. Attempt duplicate insert
    await Assert.ThrowsAsync<DuplicateKeyException>(() => btree.InsertAsync(record));
  }

  [Fact]
  public async Task InsertAsync_SequentialInserts_ForcesMultipleSplitsAndMaintainsIntegrity()
  {
    // Arrange
    // Use a schema with a large column so we trigger splits frequently (every ~3 records)
    var largeTableDef = new TableDefinition("SequentialTable");
    largeTableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    largeTableDef.AddColumn(new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Varchar, 2000), false));
    largeTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Seq", ["Id"]));

    var btree = await BTree.CreateAsync(_bpm, largeTableDef);
    string payload = new string('S', 2000);

    int recordCount = 50; // Enough to cause ~16 splits (50 / 3) and grow tree height

    // Act
    // Insert 0..49 sequentially
    for (int i = 0; i < recordCount; i++)
    {
      var rec = new Record(DataValue.CreateInteger(i), DataValue.CreateString(payload));
      await btree.InsertAsync(rec);
    }

    // Assert
    // Verify all records exist
    for (int i = 0; i < recordCount; i++)
    {
      var key = new Key([DataValue.CreateInteger(i)]);
      var result = await btree.SearchAsync(key);
      Assert.NotNull(result);
      Assert.Equal(i, result.Values[0].GetAs<int>());
    }

    // Verify root is likely an internal node now (height > 1)
    // 50 records * 2000 bytes = ~100KB data. 
    // 1 Page = 8KB. Tree must be multi-level.
    var rootId = btree.GetRootPageIdForTest();
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootId);
    Assert.NotNull(rootFrame);
    var rootHeader = new PageHeader(new Page(rootFrame.CurrentPageId, rootFrame.PageData));

    Assert.Equal(PageType.InternalNode, rootHeader.PageType);
  }

  [Fact]
  public async Task InsertAsync_ReverseSequentialInserts_MaintainsIntegrity()
  {
    // Arrange
    var largeTableDef = new TableDefinition("ReverseSequentialTable");
    largeTableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    largeTableDef.AddColumn(new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Varchar, 2000), false));
    largeTableDef.AddConstraint(new PrimaryKeyConstraint("PK_RevSeq", new[] { "Id" }));

    var btree = await BTree.CreateAsync(_bpm, largeTableDef);
    string payload = new string('R', 2000);

    int recordCount = 50;

    // Act
    // Insert 49..0 (Reverse order)
    // This stresses the "Insert at Slot 0" and "Split with New Key at Beginning" logic
    for (int i = recordCount - 1; i >= 0; i--)
    {
      var rec = new Record(DataValue.CreateInteger(i), DataValue.CreateString(payload));
      await btree.InsertAsync(rec);
    }

    // Assert
    // Verify all records exist and contain correct data
    for (int i = 0; i < recordCount; i++)
    {
      var key = new Key([DataValue.CreateInteger(i)]);
      var result = await btree.SearchAsync(key);
      Assert.NotNull(result);
      Assert.Equal(i, result.Values[0].GetAs<int>());
    }

    // Verify root is an internal node (growth occurred)
    var rootId = btree.GetRootPageIdForTest();
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootId);
    Assert.NotNull(rootFrame);
    var rootHeader = new PageHeader(new Page(rootFrame.CurrentPageId, rootFrame.PageData));

    Assert.Equal(PageType.InternalNode, rootHeader.PageType);
  }

  [Fact]
  public async Task InsertAsync_VariableLengthKeys_LopsidedSplit_ShouldDistributeBytesEvenly()
  {
    // Arrange
    // Scenario: We have a mix of very small records and very large records.
    // A standard "Median by Count" split (N/2) will result in highly unbalanced pages in terms of BYTES.
    // This test exercises the need for "Median by Size" splitting logic.

    var tableDef = new TableDefinition("LopsidedTable");
    // Use a single PK column which is also the data payload for simplicity of size calculation
    tableDef.AddColumn(new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK", new[] { "ID" }));

    var btree = await BTree.CreateAsync(_bpm, tableDef);

    // 1. Insert 10 "Small" records.
    // Each is very small (~20-50 bytes with overhead).
    // Total Size: ~500 bytes.
    for (int i = 0; i < 10; i++)
    {
      // "A00", "A01"... sorts before "B"
      var key = $"A{i:00}";
      await btree.InsertAsync(new Record(DataValue.CreateString(key)));
    }

    // 2. Insert "Large" records until we force a split.
    // Assuming PageSize is ~8192 (inferred from previous tests).
    // We create records ~2000 bytes each.
    string largePayload = new string('X', 2000);

    // Insert 4 Large records.
    // Keys: "B00"... "B03"
    // Total Data so far: 10 Small (~500B) + 4 Large (~8000B) = ~8500B.
    // This exceeds typical 8k page usable space (~8100B), forcing a split.
    for (int i = 0; i < 4; i++)
    {
      var key = $"B{i:00}" + largePayload;
      await btree.InsertAsync(new Record(DataValue.CreateString(key)));
    }

    // 3. Verify Split Occurred
    var rootId = btree.GetRootPageIdForTest();
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootId);
    Assert.NotNull(rootFrame);
    var rootHeader = new PageHeader(new Page(rootFrame.CurrentPageId, rootFrame.PageData));

    Assert.Equal(PageType.InternalNode, rootHeader.PageType); // Ensure Root is now Internal

    // 4. Inspect the Leaf Pages
    // In a standard split scenario (Root splits into Left and Right children):
    // Page 1: Original Root (now Left Child)
    // Page 2: New Sibling (now Right Child)
    // (Note: Page ID assignment depends on implementation, but usually 1 and 2 are the children).

    var leafFrame1 = _bpm.GetFrameByPageId_TestOnly(new PageId(tableDef.TableId, 1));
    var leafFrame2 = _bpm.GetFrameByPageId_TestOnly(new PageId(tableDef.TableId, 2));

    Assert.NotNull(leafFrame1);
    Assert.NotNull(leafFrame2);

    var leafHeader1 = new PageHeader(new Page(leafFrame1.CurrentPageId, leafFrame1.PageData));
    // var leafHeader2 = new PageHeader(new Page(leafFrame2.CurrentPageId, leafFrame2.PageData));

    // 5. Assert Distribution
    // Total Items = 14 (10 Small + 4 Large).

    // IF "Count-Based Split" (Midpoint = 7):
    // Left Page: 7 items (7 Small). Size ~350B. (Utilized: ~4%)
    // Right Page: 7 items (3 Small + 4 Large). Size ~8150B. (Overflows or 100% full)

    // IF "Byte-Based Split" (Balanced ~4000B each):
    // Left Page needs ~4000B. 
    // 10 Small (500B) + 1 Large (2000B) = 2500B.
    // 10 Small (500B) + 2 Large (4000B) = 4500B. -> Split point likely here.
    // Left Page: 12 items (10 Small + 2 Large).
    // Right Page: 2 items (2 Large).

    // Assertion: 
    // We expect the Left page to have taken MORE than just half the count to fill the byte space.
    // If ItemCount is <= 7, the split logic is naive and inefficient/broken for variable length keys.
    Assert.True(leafHeader1.ItemCount > 8,
        $"Left Page has only {leafHeader1.ItemCount} items. Expected > 8 (likely 12) for a byte-balanced split. " +
        "Naive count-based split detected.");
  }

  [Fact]
  public async Task InsertAsync_InternalNode_LopsidedSplit_ShouldDistributeBytesEvenly()
  {
    // Arrange
    // We want to test the SPLIT logic of an INTERNAL node.
    // To do this, we need to fill an Internal Node with a mix of Small and Large keys.
    // Internal Nodes hold the keys promoted from Leaf splits.

    var tableDef = new TableDefinition("InternalSplitTable");
    // Two columns: ID (Key), Data (Payload)
    // Constraint: Max Key Length = PageSize / 4. Assuming PageSize 8192 -> 2048 bytes.
    tableDef.AddColumn(new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Varchar, 2048), false));
    tableDef.AddColumn(new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Varchar, 5000), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK", new[] { "ID" }));

    var btree = await BTree.CreateAsync(_bpm, tableDef);
    // Page 1 is the initial Root.

    // 1. Populate Root with MANY Small Keys.
    // We do this by inserting records with Small Keys ("S_00") but Large Data.
    // This forces Leaf Splits, promoting the Small Keys to the Root.
    // Target: ~20-30 Small Keys in Root.
    string largeData = new string('x', 4500); // 4.5KB. 2 fit per leaf (9KB > 8KB). 
                                              // Actually 1 fits (4.5k), 2nd triggers split (9k). 
                                              // Each insert of 2 records = 1 Split = 1 Key in Root.
                                              // We want ~30 keys in root. So ~60 records.

    int smallKeyCount = 60;
    for (int i = 0; i < smallKeyCount; i++)
    {
      var key = $"S_{i:000}"; // Small Key (~5 bytes)
      await btree.InsertAsync(new Record(DataValue.CreateString(key), DataValue.CreateString(largeData)));
    }

    // At this point:
    // - Root (Page 1) is an Internal Node.
    // - It holds ~30 keys (all "S_...").
    // - 30 keys * ~15 bytes each = ~450 bytes used. 
    // - Root is largely EMPTY in terms of bytes, but has a decent Item Count.

    // 2. Populate Root with Large Keys until Split.
    // We insert records with Large Keys.
    // Key: "L_00" + Padding.
    // We reduce padding to 1900 to be safely under the 2048 limit (PageSize/4) while still being "Large".

    string largeKeyPadding = new string('y', 1900);

    for (int i = 0; i < 5; i++) // 5 * 1900 = 9500 bytes > 8192 page size.
    {
      var key = $"L_{i:000}" + largeKeyPadding;
      await btree.InsertAsync(new Record(DataValue.CreateString(key), DataValue.CreateString("")));
    }

    // 3. Verify Internal Split Behavior
    // The Root (Page 1) should have split.
    // A new Root (Page X) is created.
    // Page 1 remains as one of the children (typically Left, as "S" < "L" is NOT true... Wait).
    // Alphabetical: "L" comes BEFORE "S".
    // So "L..." keys are on the LEFT. "S..." keys are on the RIGHT.

    // Let's re-check Sort Order: "L" < "S".
    // So the Large keys are inserted at the BEGINNING of the sorted list.
    // The Small keys ("S_...") are at the END.

    // The Internal Node looked like: [L_00, L_01, L_02 ... S_00, S_01 ... S_29]
    // Wait, inserting "S" first means they are already there.
    // Inserting "L" puts them before "S".
    // List: [L_00...L_04, S_00...S_59]

    // Total Items: ~5 Large + ~30 Small = 35 items.
    // Total Bytes: (5 * 1900) + (30 * 20) = 9500 + 600 = ~10,100 bytes.
    // Page Capacity: ~8192.
    // Split Target: ~4096 bytes.

    // If Count-Based Split (Midpoint = 17):
    // Left Page: 5 Large + 12 Small. Size: 9500 + ... = HUGE OVERFLOW.
    // Actually, simple count split might fail if the left side doesn't fit?
    // Or if it just blindly moves them, the Left page is 150% full (corruption/crash).

    // If Byte-Based Split:
    // Accumulate from Left (Large Keys):
    // L_00 (1.9k) + L_01 (1.9k) = 3.8k. 
    // L_02 adds 1.9k -> 5.7k.
    // Split should happen after 2 or 3 items.
    // Left Page: 2 or 3 items.
    // Right Page: The rest.

    // Let's inspect Page 1. 
    // In standard B+Tree implementation, Page 1 usually stays as the "Left" or "Original" node 
    // while a new page is allocated for the split.
    // Since "L" keys (Large) are technically "before" "S" keys, they might end up in the Left Node.

    // However, to be robust, we shouldn't guess which page ID is Left/Right.
    // We should get the New Root and look at its children.

    var newRootId = btree.GetRootPageIdForTest();
    Assert.NotEqual(1, newRootId.PageIndex); // Root must have moved/changed

    // Get New Root Frame (Internal Node)
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(newRootId);
    Assert.NotNull(rootFrame);
    var rootHeader = new PageHeader(new Page(rootFrame.CurrentPageId, rootFrame.PageData));

    // The Root should have 1 item (Separator). 
    // Separator Key partitions Left and Right.
    // Left Child PageId should be implicitly accessible (e.g., in a standard B-Tree implementation logic).

    // To simplify, let's just check Page 1 directly again.
    // Page 1 was the original node. It should now contain EITHER:
    // A) The Left partition (Large Keys)
    // B) The Right partition (Small Keys)
    // AND it should be a VALID page (not overflowing).

    var page1Frame = _bpm.GetFrameByPageId_TestOnly(new PageId(tableDef.TableId, 1));
    var page1Header = new PageHeader(new Page(page1Frame.CurrentPageId, page1Frame.PageData));

    // We expect Page 1 to NOT be empty.
    Assert.True(page1Header.ItemCount > 0);

    // CASE A: Page 1 holds the Large Keys (Left side).
    // It should hold very FEW items (e.g., 2 or 3).
    // If it held half the count (17), it would be corrupted/overflowed.

    // CASE B: Page 1 holds the Small Keys (Right side).
    // It should hold MANY items (e.g., 30+).

    // Given the logic, it's one or the other.
    // Fail if it holds "Middle" count (e.g., 10-20) which implies a count-based split 
    // that likely mixed Large and Small keys in a bad way or just happened to allow overflow.

    bool isLeftSkewed = page1Header.ItemCount < 5; // Holds just a few Large keys
    bool isRightSkewed = page1Header.ItemCount > 20; // Holds all the Small keys

    Assert.True(isLeftSkewed || isRightSkewed,
        $"Internal Node Split failed to distribute bytes evenly. Page 1 ItemCount: {page1Header.ItemCount}. " +
        "Expected either < 5 (Large Keys) or > 20 (Small Keys). A value in between implies a naive count-based split.");
  }

  private async Task<PageId> ManualCreateLeaf(TableDefinition def, int[] keys, string filler)
  {
    var page = await _bpm.CreatePageAsync(def.TableId);
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, def);
    foreach (var k in keys)
    {
      leafNode.TryInsert(new Record(DataValue.CreateInteger(k), DataValue.CreateString(filler)));
    }
    _bpm.UnpinPage(page.Id, true);
    return page.Id;
  }

  private async Task<PageId> ManualCreateInternal(TableDefinition def, (int key, PageId ptr)[] entries, PageId rightmost)
  {
    var page = await _bpm.CreatePageAsync(def.TableId);
    SlottedPage.Initialize(page, PageType.InternalNode);
    var node = new BTreeInternalNode(page, def);
    foreach (var e in entries)
    {
      node.InsertEntryForTest(new Key([DataValue.CreateInteger(e.key)]), e.ptr);
    }
    node.SetRightmostChildId(rightmost.PageIndex);
    _bpm.UnpinPage(page.Id, true);
    return page.Id;
  }

  private async Task SetParentPointer(PageId childId, PageId parentId)
  {
    // Helper to update the parent pointer of a child page
    var page = await _bpm.FetchPageAsync(childId);
    Assert.NotNull(page);
    var header = new PageHeader(page);
    header.ParentPageIndex = parentId.PageIndex;
    _bpm.UnpinPage(childId, isDirty: true);
  }

  private async Task<PageId> ManualCreateLeaf(TableDefinition def, string[] keys)
  {
    var page = await _bpm.CreatePageAsync(def.TableId);
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, def);
    foreach (var k in keys)
    {
      leafNode.TryInsert(new Record(DataValue.CreateString(k), DataValue.CreateInteger(0)));
    }
    _bpm.UnpinPage(page.Id, true);
    return page.Id;
  }

  private async Task<PageId> ManualCreateInternal(TableDefinition def, (string key, PageId ptr)[] entries, PageId? rightmost)
  {
    var page = await _bpm.CreatePageAsync(def.TableId);
    SlottedPage.Initialize(page, PageType.InternalNode);
    var node = new BTreeInternalNode(page, def);
    foreach (var e in entries)
    {
      node.InsertEntryForTest(new Key([DataValue.CreateString(e.key)]), e.ptr);
    }
    if (rightmost != null)
      node.SetRightmostChildId(rightmost.Value.PageIndex);

    _bpm.UnpinPage(page.Id, true);
    return page.Id;
  }
}