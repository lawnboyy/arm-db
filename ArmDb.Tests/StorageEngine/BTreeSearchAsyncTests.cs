using ArmDb.DataModel;
using ArmDb.StorageEngine;

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
}