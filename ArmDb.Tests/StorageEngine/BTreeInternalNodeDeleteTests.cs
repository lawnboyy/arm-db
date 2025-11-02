using ArmDb.DataModel;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeInternalNodeTests
{
  [Fact]
  public void Delete_WhenKeyExists_RemovesEntryAndReturnsTrue()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Populate the node with three entries
    var key100 = new Key([DataValue.CreateInteger(100)]);
    var pageId10 = new PageId(1, 10);
    var key200 = new Key([DataValue.CreateInteger(200)]);
    var pageId20 = new PageId(1, 20);
    var key300 = new Key([DataValue.CreateInteger(300)]);
    var pageId30 = new PageId(1, 30);

    internalNode.InsertEntryForTest(key100, pageId10); // slot 0
    internalNode.InsertEntryForTest(key200, pageId20); // slot 1
    internalNode.InsertEntryForTest(key300, pageId30); // slot 2

    Assert.Equal(3, internalNode.ItemCount); // Verify setup

    var keyToDelete = key200; // Delete the middle entry

    // Act
    // This is the method you will implement
    bool success = internalNode.Delete(keyToDelete);

    // Assert
    Assert.True(success, "Delete should return true for an existing key.");

    // 1. Verify the item count was decremented
    Assert.Equal(2, internalNode.ItemCount);

    // 2. Verify the deleted key is no longer found
    //    We can't use Search directly as it finds the next *pointer*,
    //    so we check the raw entries.
    var (entry0Key, _) = internalNode.GetEntryForTest(0);
    var (entry1Key, _) = internalNode.GetEntryForTest(1);

    Assert.Equal(key100, entry0Key); // Slot 0 is unchanged
    Assert.Equal(key300, entry1Key); // Slot 1 now contains the old Slot 2 data

    // 3. Verify accessing the old slot index (2) now fails
    Assert.Throws<ArgumentOutOfRangeException>(() => internalNode.GetEntryForTest(2));
  }

  [Fact]
  public void Delete_WhenKeyNotFound_ReturnsFalseAndDoesNotModifyPage()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Populate the node with two entries
    var key100 = new Key([DataValue.CreateInteger(100)]);
    var pageId10 = new PageId(1, 10);
    var key300 = new Key([DataValue.CreateInteger(300)]);
    var pageId30 = new PageId(1, 30);

    internalNode.InsertEntryForTest(key100, pageId10); // slot 0
    internalNode.InsertEntryForTest(key300, pageId30); // slot 1

    Assert.Equal(2, internalNode.ItemCount); // Verify setup

    var keyToDelete = new Key([DataValue.CreateInteger(200)]); // Key does not exist

    // Act
    bool success = internalNode.Delete(keyToDelete);

    // Assert
    Assert.False(success, "Delete should return false for a non-existent key.");

    // 1. Verify the item count is unchanged
    Assert.Equal(2, internalNode.ItemCount);

    // 2. Verify the original data is still present and unmodified
    var (entry0Key, _) = internalNode.GetEntryForTest(0);
    var (entry1Key, _) = internalNode.GetEntryForTest(1);

    Assert.Equal(key100, entry0Key);
    Assert.Equal(key300, entry1Key);
  }

  [Fact]
  public void Delete_WithNullKey_ThrowsArgumentNullException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);
    Key? nullKey = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("keyToDelete", () => internalNode.Delete(nullKey!));
  }

  [Fact]
  public void Delete_WhenKeyIsFirstEntry_RemovesAndCompactsCorrectly()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Populate with [100, 200, 300]
    var key100 = new Key([DataValue.CreateInteger(100)]);
    var pageId10 = new PageId(1, 10);
    var key200 = new Key([DataValue.CreateInteger(200)]);
    var pageId20 = new PageId(1, 20);
    var key300 = new Key([DataValue.CreateInteger(300)]);
    var pageId30 = new PageId(1, 30);
    internalNode.InsertEntryForTest(key100, pageId10); // slot 0
    internalNode.InsertEntryForTest(key200, pageId20); // slot 1
    internalNode.InsertEntryForTest(key300, pageId30); // slot 2

    var keyToDelete = key100; // Delete the first entry

    // Act
    bool success = internalNode.Delete(keyToDelete);

    // Assert
    Assert.True(success);
    Assert.Equal(2, internalNode.ItemCount);

    // Verify the remaining entries were shifted left
    var (entry0Key, _) = internalNode.GetEntryForTest(0);
    var (entry1Key, _) = internalNode.GetEntryForTest(1);

    Assert.Equal(key200, entry0Key); // Slot 0 should now hold key 200
    Assert.Equal(key300, entry1Key); // Slot 1 should now hold key 300
  }

  [Fact]
  public void Delete_WhenKeyIsLastEntry_RemovesCorrectly()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Populate with [100, 200, 300]
    var key100 = new Key([DataValue.CreateInteger(100)]);
    var pageId10 = new PageId(1, 10);
    var key200 = new Key([DataValue.CreateInteger(200)]);
    var pageId20 = new PageId(1, 20);
    var key300 = new Key([DataValue.CreateInteger(300)]);
    var pageId30 = new PageId(1, 30);
    internalNode.InsertEntryForTest(key100, pageId10); // slot 0
    internalNode.InsertEntryForTest(key200, pageId20); // slot 1
    internalNode.InsertEntryForTest(key300, pageId30); // slot 2

    var keyToDelete = key300; // Delete the last entry

    // Act
    bool success = internalNode.Delete(keyToDelete);

    // Assert
    Assert.True(success);
    Assert.Equal(2, internalNode.ItemCount);

    // Verify the remaining entries are still in place
    var (entry0Key, _) = internalNode.GetEntryForTest(0);
    var (entry1Key, _) = internalNode.GetEntryForTest(1);
    Assert.Equal(key100, entry0Key);
    Assert.Equal(key200, entry1Key);

    // Verify accessing the old last slot now fails
    Assert.Throws<ArgumentOutOfRangeException>(() => internalNode.GetEntryForTest(2));
  }
}
