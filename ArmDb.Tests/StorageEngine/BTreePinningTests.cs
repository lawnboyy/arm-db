using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeTests
{
  [Fact]
  public async Task InsertAsync_SimpleInsert_ReleasesAllPins()
  {
    // Arrange
    var btree = await BTree.CreateAsync(_bpm, _tableDef);
    var rootPageId = btree.GetRootPageIdForTest();

    var record = new Record(DataValue.CreateInteger(1), DataValue.CreateString("Test"));

    // Act
    await btree.InsertAsync(record);

    // Assert
#if DEBUG
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootPageId);
    Assert.NotNull(rootFrame);

    // After insertion is complete, the root (and any leaf) should be unpinned
    Assert.Equal(0, rootFrame.PinCount);
#endif
  }

  [Fact]
  public async Task InsertAsync_DuplicateKeyError_ReleasesAllPins()
  {
    // Arrange
    var btree = await BTree.CreateAsync(_bpm, _tableDef);
    var rootPageId = btree.GetRootPageIdForTest();

    var record = new Record(DataValue.CreateInteger(1), DataValue.CreateString("Test"));
    await btree.InsertAsync(record); // Insert once

    // Act & Assert
    await Assert.ThrowsAsync<DuplicateKeyException>(() => btree.InsertAsync(record));

    // Assert Pins are released even after exception
#if DEBUG
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootPageId);
    Assert.NotNull(rootFrame);
    Assert.Equal(0, rootFrame.PinCount);
#endif
  }

  [Fact]
  public async Task InsertAsync_RecursiveSplit_ReleasesAllPins()
  {
    // Arrange
    // Define schema with a LARGE Primary Key to force Internal Nodes to split easily.
    var hugeKeyTableDef = new TableDefinition("RecursiveSplitTable_HugePK_Pinning");
    hugeKeyTableDef.AddColumn(new ColumnDefinition("KeyData", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    hugeKeyTableDef.AddColumn(new ColumnDefinition("Val", new DataTypeInfo(PrimitiveDataType.Int), false));
    hugeKeyTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Huge", new[] { "KeyData" }));

    // Helper strings for keys
    string kA = new string('A', 3000);
    string kB = new string('B', 3000); // Insert Target
    string kC = new string('C', 3000);
    string kE = new string('E', 3000);
    string kF = new string('F', 3000);
    string kG = new string('G', 3000);
    string kH = new string('H', 3000);
    string kM = new string('M', 3000);
    string kN = new string('N', 3000);

    // --- Construct Tree ---
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(hugeKeyTableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // Leaves
    var leafTarget = await ManualCreateLeaf(hugeKeyTableDef, new[] { kA, kC }); // Full [A, C] (Page 0)
    var leaf2 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kF }); // (Page 1)
    var leaf3 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kH }); // (Page 2)
    var leaf4 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kN }); // (Page 3)

    // Parent Internal Node (L1) - Full
    // Entries: (E, leafTarget), (G, leaf2). Rightmost: leaf3.
    var parentNodeId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kE, leafTarget), (kG, leaf2) },
        leaf3); // (Page 4)

    await SetParentPointer(leafTarget, parentNodeId);
    await SetParentPointer(leaf2, parentNodeId);
    await SetParentPointer(leaf3, parentNodeId);

    // Sibling Internal Node (L1)
    var siblingNodeId = await ManualCreateInternal(hugeKeyTableDef,
        new (string, PageId)[0],
        leaf4); // (Page 5)
    await SetParentPointer(leaf4, siblingNodeId);

    // Root Node (L0) - Not Full
    var rootPageId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kM, parentNodeId) },
        siblingNodeId); // (Page 6)
    // Set the root page pointer in the table header page...
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;

    await SetParentPointer(parentNodeId, rootPageId);
    await SetParentPointer(siblingNodeId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId, tableHeaderPage);

    /*
       Initial Tree Structure (with Page Indices):
       * = Rightmost Child Pointer

                                 [ M ]* (Page 6)
                                /      \
                     [ E | G ]* (Page 4)  [   ]* (Page 5)
                    /    |    \               \
             (Pg 0)   (Pg 1)  (Pg 2)         (Pg 3)
           [ A | C ]   [ F ]   [ H ]          [ N ]
    */

    // Act
    // Insert B. This triggers Leaf Split -> Parent Split.
    var recordToInsert = new Record(DataValue.CreateString(kB), DataValue.CreateInteger(1));
    await btree.InsertAsync(recordToInsert);

    /*
       Resulting Tree Structure (with Page Indices):
       * = Rightmost Child Pointer

                                 [ E | M ]* (Page 6)
                                /    |    \
                               /     |     \
              .---------------'      |      '---------------.
              |                      |                      |
          [ B ]* (Page 4)        [ G ]* (Page 8)       [   ]* (Page 5)
         /      \               /      \                    \
     (Pg 0)   (Pg 7)        (Pg 1)    (Pg 2)               (Pg 3)
     [ A ]    [ B | C ]      [ F ]     [ H ]                [ N ]
    */

    // Assert
#if DEBUG
    // Verify critical pages are unpinned

    // 1. Root
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootPageId);
    Assert.NotNull(rootFrame);
    Assert.Equal(0, rootFrame.PinCount);

    // 2. Parent (L1) - Split source
    var parentFrame = _bpm.GetFrameByPageId_TestOnly(parentNodeId);
    Assert.NotNull(parentFrame);
    Assert.Equal(0, parentFrame.PinCount);

    // 3. Leaf Target - Split source
    var leafFrame = _bpm.GetFrameByPageId_TestOnly(leafTarget);
    Assert.NotNull(leafFrame);
    Assert.Equal(0, leafFrame.PinCount);

    // 4. Sibling Internal Node (should be untouched but verify no accidental pins)
    var siblingFrame = _bpm.GetFrameByPageId_TestOnly(siblingNodeId);
    if (siblingFrame != null) Assert.Equal(0, siblingFrame.PinCount);
#endif
  }

  [Fact]
  public async Task InsertAsync_LeafSplitOnly_ReleasesAllPins()
  {
    // Arrange
    var hugeKeyTableDef = new TableDefinition("RecursiveSplitTable_HugePK_Pinning");
    hugeKeyTableDef.AddColumn(new ColumnDefinition("KeyData", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    hugeKeyTableDef.AddColumn(new ColumnDefinition("Val", new DataTypeInfo(PrimitiveDataType.Int), false));
    hugeKeyTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Huge", new[] { "KeyData" }));

    string kA = new string('A', 3000);
    string kB = new string('B', 3000); // Insert Target
    string kC = new string('C', 3000);
    string kE = new string('E', 3000);
    string kH = new string('H', 3000);
    string kM = new string('M', 3000);
    string kN = new string('N', 3000);

    // --- Construct Tree ---
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(hugeKeyTableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // Leaf Target (Full): [A, C]
    var leafTarget = await ManualCreateLeaf(hugeKeyTableDef, new[] { kA, kC });
    // Leaf Sibling: [H]
    var leafSibling = await ManualCreateLeaf(hugeKeyTableDef, new[] { kH });

    // Parent (L1) - Not Full (1 Entry)
    // Entries: (E, leafTarget). Rightmost: leafSibling.
    var parentNodeId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kE, leafTarget) },
        leafSibling);
    await SetParentPointer(leafTarget, parentNodeId);
    await SetParentPointer(leafSibling, parentNodeId);

    // Root (L0)
    // Entries: (M, parentNodeId). Rightmost: Dummy.
    var dummyLeaf = await ManualCreateLeaf(hugeKeyTableDef, new[] { kN });
    var rootPageId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kM, parentNodeId) },
        dummyLeaf); // Dummy rightmost
    // Set the root page pointer in the table header page...
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;
    await SetParentPointer(parentNodeId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId, tableHeaderPage);

    // Act
    // Insert B. 
    // LeafTarget [A, C] splits -> Promotes B.
    // Parent [E] inserts B -> [B, E]. Parent has space (max 2). No split.
    var recordToInsert = new Record(DataValue.CreateString(kB), DataValue.CreateInteger(1));
    await btree.InsertAsync(recordToInsert);

    // Assert
#if DEBUG
    // 1. Root
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootPageId);
    Assert.NotNull(rootFrame);
    Assert.Equal(0, rootFrame.PinCount);

    // 2. Parent - Absorbed the key
    var parentFrame = _bpm.GetFrameByPageId_TestOnly(parentNodeId);
    Assert.NotNull(parentFrame);
    Assert.Equal(0, parentFrame.PinCount);

    // 3. Leaf Target - Split
    var leafFrame = _bpm.GetFrameByPageId_TestOnly(leafTarget);
    Assert.NotNull(leafFrame);
    Assert.Equal(0, leafFrame.PinCount);
#endif
  }

  [Fact]
  public async Task InsertAsync_LeafAndParentSplit_NoRootSplit_ReleasesAllPins()
  {
    // Arrange
    var hugeKeyTableDef = new TableDefinition("RecursiveSplitTable_HugePK_Pinning");
    hugeKeyTableDef.AddColumn(new ColumnDefinition("KeyData", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    hugeKeyTableDef.AddColumn(new ColumnDefinition("Val", new DataTypeInfo(PrimitiveDataType.Int), false));
    hugeKeyTableDef.AddConstraint(new PrimaryKeyConstraint("PK_Huge", new[] { "KeyData" }));

    string kA = new string('A', 3000);
    string kB = new string('B', 3000); // Insert Target
    string kC = new string('C', 3000);
    string kE = new string('E', 3000);
    string kF = new string('F', 3000);
    string kG = new string('G', 3000);
    string kH = new string('H', 3000);
    string kM = new string('M', 3000);
    string kN = new string('N', 3000);

    // --- Construct Tree ---
    // Start by creating our table header page.
    var tableHeaderPage = await _bpm.CreatePageAsync(hugeKeyTableDef.TableId);
    SlottedPage.Initialize(tableHeaderPage, PageType.TableHeader);

    // Leaf Target (Full): [A, C]
    var leafTarget = await ManualCreateLeaf(hugeKeyTableDef, new[] { kA, kC });
    var leaf2 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kF });
    var leaf3 = await ManualCreateLeaf(hugeKeyTableDef, new[] { kH });

    // Parent (L1) - Full (2 Entries)
    // Entries: (E, leafTarget), (G, leaf2). Rightmost: leaf3.
    var parentNodeId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kE, leafTarget), (kG, leaf2) },
        leaf3);
    await SetParentPointer(leafTarget, parentNodeId);
    await SetParentPointer(leaf2, parentNodeId);
    await SetParentPointer(leaf3, parentNodeId);

    // Root (L0) - Not Full (1 Entry)
    // Entries: (M, parentNodeId).
    var dummyLeaf = await ManualCreateLeaf(hugeKeyTableDef, new[] { kN });
    var rootPageId = await ManualCreateInternal(hugeKeyTableDef,
        new[] { (kM, parentNodeId) },
        dummyLeaf);

    // Set the root page pointer in the table header page...
    new PageHeader(tableHeaderPage).RootPageIndex = rootPageId.PageIndex;
    await SetParentPointer(parentNodeId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId, tableHeaderPage);

    // Act
    // Insert B. 
    // 1. LeafTarget splits -> Promotes B.
    // 2. Parent inserts B -> [B, E, G]. Full (max 2). Parent Splits.
    //    Median E promoted.
    // 3. Root inserts E -> [E, M]. Root has space. No split.
    var recordToInsert = new Record(DataValue.CreateString(kB), DataValue.CreateInteger(1));
    await btree.InsertAsync(recordToInsert);

    // Assert
#if DEBUG
    // 1. Root - Absorbed key E
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootPageId);
    Assert.NotNull(rootFrame);
    Assert.Equal(0, rootFrame.PinCount);

    // 2. Parent - Split source
    var parentFrame = _bpm.GetFrameByPageId_TestOnly(parentNodeId);
    Assert.NotNull(parentFrame);
    Assert.Equal(0, parentFrame.PinCount);

    // 3. Leaf Target - Split source
    var leafFrame = _bpm.GetFrameByPageId_TestOnly(leafTarget);
    Assert.NotNull(leafFrame);
    Assert.Equal(0, leafFrame.PinCount);
#endif
  }

  [Fact]
  public async Task InsertAsync_RecordLargerThanPage_ThrowsExceptionAndReleasesPins()
  {
    // Arrange
    var btree = await BTree.CreateAsync(_bpm, _tableDef);
    var rootPageId = btree.GetRootPageIdForTest();

    // Create a record larger than the page size (8192 bytes).
    // 9000 characters + overhead will definitely not fit.
    var hugeRecord = new Record(
        DataValue.CreateInteger(1),
        DataValue.CreateString(new string('X', 9000))
    );

    // Act & Assert
    // We expect an InvalidOperationException (or similar) because SplitAndInsert 
    // will fail to insert the record even into a fresh, empty page.
    await Assert.ThrowsAsync<InvalidOperationException>(() => btree.InsertAsync(hugeRecord));

    // Verify pins are released
#if DEBUG
    var rootFrame = _bpm.GetFrameByPageId_TestOnly(rootPageId);
    // Depending on implementation, the root might still be in memory but must have 0 pins.
    if (rootFrame != null)
    {
      Assert.Equal(0, rootFrame.PinCount);
    }
#endif
  }
}