using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
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
}