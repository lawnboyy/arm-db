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

    await SetParentPointer(parentNodeId, rootPageId);
    await SetParentPointer(siblingNodeId, rootPageId);

    var btree = await BTree.CreateAsync(_bpm, hugeKeyTableDef, rootPageId);

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
}