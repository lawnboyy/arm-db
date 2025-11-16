using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
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
}