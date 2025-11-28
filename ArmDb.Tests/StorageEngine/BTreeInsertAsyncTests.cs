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

  private async Task<PageId> ManualCreateLeaf(TableDefinition def, int[] keys, string filler)
  {
    var page = await _bpm.CreatePageAsync(def.TableId);
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, def);
    foreach (var k in keys)
    {
      leafNode.TryInsert(new Record(DataValue.CreateInteger(k), DataValue.CreateString(filler)));
    }
    await _bpm.UnpinPageAsync(page.Id, true);
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
    await _bpm.UnpinPageAsync(page.Id, true);
    return page.Id;
  }

  private async Task SetParentPointer(PageId childId, PageId parentId)
  {
    // Helper to update the parent pointer of a child page
    var page = await _bpm.FetchPageAsync(childId);
    Assert.NotNull(page);
    var header = new PageHeader(page);
    header.ParentPageIndex = parentId.PageIndex;
    await _bpm.UnpinPageAsync(childId, isDirty: true);
  }
}