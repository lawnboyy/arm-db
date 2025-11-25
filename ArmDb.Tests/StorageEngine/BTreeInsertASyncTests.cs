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
}