using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.StorageEngine;

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

    // 4. Verify the old root is now a leaf child (optional deep inspection)
    // The separator key should be somewhere between the keys (likely 30 due to median split)
    // The pointer associated with the separator should point to the old root (or new sibling depending on logic)
    // ...
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

    // Link Parent to Root
    await SetParentPointer(internalParentPageId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, largeTableDef, rootPageId);

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

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId);
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

    await SetParentPointer(parentNodeId, rootPageId);
    await SetParentPointer(siblingNodeId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId);
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

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId);
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
    await SetParentPointer(l1_Target, rootPageId);
    await SetParentPointer(l1_Sibling1, rootPageId);
    await SetParentPointer(l1_Sibling2, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId);
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

    await SetParentPointer(parentNodeId, rootPageId);
    await SetParentPointer(siblingNodeId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId);

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