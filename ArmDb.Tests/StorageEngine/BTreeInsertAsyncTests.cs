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
    hugeKeyTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Huge", new[] { "KeyData" }));

    // Helper strings for keys (Length ~3000)
    string kA = new string('A', 3000);
    string kB = new string('B', 3000); // Insert Target
    string kC = new string('C', 3000);
    string kE = new string('E', 3000); // Separator 1
    string kF = new string('F', 3000);
    string kG = new string('G', 3000); // Separator 2
    // string kH = new string('H', 3000);
    // string kL = new string('L', 3000); // Child of N
    string kM = new string('M', 3000); // Root Separator
    // string kN = new string('N', 3000); // Separator 3 (Right Sibling)
    // string kR = new string('R', 3000); // Rightmost Leaf

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
    // TODO: The new tree is pointing the left most internal node separator key for B
    // back to the page ID 0 when it should now point to page ID 8 (contains B, C). The 
    // internal node at page index 5's rightmost pointer is not getting adjusted to
    // point to the new right sibling leaf node after the split.
    var foundB = await btree.SearchAsync(new Key([DataValue.CreateString(kB)]));
    Assert.NotNull(foundB);
    Assert.Equal(1, foundB.Values[1].GetAs<int>());

    // 4. Verify neighbors
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kA)])));
    Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kC)])));
    // Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kH)])));
    // Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kN)])));
    // Assert.NotNull(await btree.SearchAsync(new Key([DataValue.CreateString(kR)])));
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