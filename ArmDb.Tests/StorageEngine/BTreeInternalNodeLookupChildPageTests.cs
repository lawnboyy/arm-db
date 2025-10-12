using ArmDb.DataModel;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeInternalNodeTests
{
  [Fact]
  public void LookupChildPage_WhenKeyIsLessThanAll_ReturnsFirstPointer()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // This internal node will point to three child pages:
    // - Child 1 (page index 10): for keys < 100
    // - Child 2 (page index 20): for keys >= 100 and < 200
    // - Child 3 (page index 30): for keys >= 200
    var key100 = new Key([DataValue.CreateInteger(100)]);
    var childPageId10 = new PageId(1, 10);
    var key200 = new Key([DataValue.CreateInteger(200)]);
    var childPageId20 = new PageId(1, 20);
    var rightmostChildPageId = new PageId(1, 30);

    // Manually add the entries to the page using lower-level helpers
    // (BTreeInternalNode.Insert will be tested later)
    var entry1Bytes = BTreeInternalNode.SerializeEntry(key100, childPageId10, tableDef);
    var entry2Bytes = BTreeInternalNode.SerializeEntry(key200, childPageId20, tableDef);
    SlottedPage.TryAddRecord(page, entry1Bytes, 0);
    SlottedPage.TryAddRecord(page, entry2Bytes, 1);
    new PageHeader(page).RightmostChildPageIndex = rightmostChildPageId.PageIndex;

    // The key to search for is less than all keys in the node
    var searchKey = new Key([DataValue.CreateInteger(50)]);
    var expectedChildPageId = childPageId10;

    // Act
    PageId actualChildPageId = internalNode.LookupChildPage(searchKey);

    // Assert
    Assert.Equal(expectedChildPageId, actualChildPageId);
  }

  [Fact]
  public void LookupChildPage_WhenKeyIsBetweenTwoKeys_ReturnsCorrectPointer()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Setup is the same as the previous test:
    // Ptr at slot 0 (page 10): keys < 100
    // Ptr at slot 1 (page 20): keys >= 100 and < 200
    // Rightmost ptr (page 30): keys >= 200
    var key100 = new Key([DataValue.CreateInteger(100)]);
    var childPageId10 = new PageId(1, 10);
    var key200 = new Key([DataValue.CreateInteger(200)]);
    var childPageId20 = new PageId(1, 20);
    var rightmostChildPageId = new PageId(1, 30);

    var entry1Bytes = BTreeInternalNode.SerializeEntry(key100, childPageId10, tableDef);
    var entry2Bytes = BTreeInternalNode.SerializeEntry(key200, childPageId20, tableDef);
    SlottedPage.TryAddRecord(page, entry1Bytes, 0);
    SlottedPage.TryAddRecord(page, entry2Bytes, 1);
    new PageHeader(page).RightmostChildPageIndex = rightmostChildPageId.PageIndex;

    // The key to search for falls between key 100 and key 200
    var searchKey = new Key([DataValue.CreateInteger(150)]);
    // The correct pointer is the one associated with the *next highest key* (200), which is pageId 20.
    var expectedChildPageId = childPageId20;

    // Act
    PageId actualChildPageId = internalNode.LookupChildPage(searchKey);

    // Assert
    Assert.Equal(expectedChildPageId, actualChildPageId);
  }

  [Fact]
  public void LookupChildPage_WhenKeyIsGreaterThanAll_ReturnsRightmostPointer()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Setup is the same as the previous test:
    // Ptr at slot 0 (page 10): keys < 100
    // Ptr at slot 1 (page 20): keys >= 100 and < 200
    // Rightmost ptr (page 30): for keys >= 200
    var key100 = new Key([DataValue.CreateInteger(100)]);
    var childPageId10 = new PageId(1, 10);
    var key200 = new Key([DataValue.CreateInteger(200)]);
    var childPageId20 = new PageId(1, 20);
    var rightmostChildPageId = new PageId(1, 30);

    var entry1Bytes = BTreeInternalNode.SerializeEntry(key100, childPageId10, tableDef);
    var entry2Bytes = BTreeInternalNode.SerializeEntry(key200, childPageId20, tableDef);
    SlottedPage.TryAddRecord(page, entry1Bytes, 0);
    SlottedPage.TryAddRecord(page, entry2Bytes, 1);
    new PageHeader(page).RightmostChildPageIndex = rightmostChildPageId.PageIndex;

    // The key to search for is greater than all keys in the node
    var searchKey = new Key([DataValue.CreateInteger(250)]);
    var expectedChildPageId = rightmostChildPageId;

    // Act
    PageId actualChildPageId = internalNode.LookupChildPage(searchKey);

    // Assert
    Assert.Equal(expectedChildPageId, actualChildPageId);
  }
}
