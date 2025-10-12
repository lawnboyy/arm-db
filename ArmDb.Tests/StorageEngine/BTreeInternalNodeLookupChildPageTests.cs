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

  [Fact]
  public void LookupChildPage_WhenKeyEqualsLargestKey_ReturnsRightmostPointer()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Setup is the same as the previous tests
    var key100 = new Key([DataValue.CreateInteger(100)]);
    var childPageId10 = new PageId(1, 10);
    var key200 = new Key([DataValue.CreateInteger(200)]); // Largest key
    var childPageId20 = new PageId(1, 20);
    var rightmostChildPageId = new PageId(1, 30);

    var entry1Bytes = BTreeInternalNode.SerializeEntry(key100, childPageId10, tableDef);
    var entry2Bytes = BTreeInternalNode.SerializeEntry(key200, childPageId20, tableDef);
    SlottedPage.TryAddRecord(page, entry1Bytes, 0);
    SlottedPage.TryAddRecord(page, entry2Bytes, 1);
    new PageHeader(page).RightmostChildPageIndex = rightmostChildPageId.PageIndex;

    // The key to search for is EQUAL to the largest key in the node
    var searchKey = new Key([DataValue.CreateInteger(200)]);
    var expectedChildPageId = rightmostChildPageId;

    // Act
    PageId actualChildPageId = internalNode.LookupChildPage(searchKey);

    // Assert
    Assert.Equal(expectedChildPageId, actualChildPageId);
  }

  [Fact]
  public void LookupChildPage_WhenKeyEqualsNonLargestKey_ReturnsCorrectPointer()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Setup with three separator keys
    // Ptr at slot 0 (page 10): keys < 100
    // Ptr at slot 1 (page 20): keys >= 100 and < 200
    // Ptr at slot 2 (page 30): keys >= 200 and < 300
    // Rightmost ptr (page 40): for keys >= 300
    var key100 = new Key([DataValue.CreateInteger(100)]);
    var childPageId10 = new PageId(1, 10);
    var key200 = new Key([DataValue.CreateInteger(200)]);
    var childPageId20 = new PageId(1, 20);
    var key300 = new Key([DataValue.CreateInteger(300)]);
    var childPageId30 = new PageId(1, 30);
    var rightmostChildPageId = new PageId(1, 40);

    SlottedPage.TryAddRecord(page, BTreeInternalNode.SerializeEntry(key100, childPageId10, tableDef), 0);
    SlottedPage.TryAddRecord(page, BTreeInternalNode.SerializeEntry(key200, childPageId20, tableDef), 1);
    SlottedPage.TryAddRecord(page, BTreeInternalNode.SerializeEntry(key300, childPageId30, tableDef), 2);
    new PageHeader(page).RightmostChildPageIndex = rightmostChildPageId.PageIndex;

    // The key to search for is EQUAL to the first key in the node
    var searchKey = new Key([DataValue.CreateInteger(100)]);
    // Since key >= 100, we should follow the pointer for the next range [100..200),
    // which is the pointer stored with the key 200.
    var expectedChildPageId = childPageId20;

    // Act
    PageId actualChildPageId = internalNode.LookupChildPage(searchKey);

    // Assert
    Assert.Equal(expectedChildPageId, actualChildPageId);
  }

  [Fact]
  public void LookupChildPage_OnEmptyNode_AlwaysReturnsRightmostPointer()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Set the rightmost pointer on the empty page
    var rightmostChildPageId = new PageId(1, 50);
    new PageHeader(page).RightmostChildPageIndex = rightmostChildPageId.PageIndex;

    // The search key can be anything, as there are no keys to compare against
    var searchKey = new Key([DataValue.CreateInteger(12345)]);
    var expectedChildPageId = rightmostChildPageId;

    // Act
    PageId actualChildPageId = internalNode.LookupChildPage(searchKey);

    // Assert
    Assert.Equal(expectedChildPageId, actualChildPageId);
  }

  [Theory]
  [InlineData(50, 10)] // Key < separator, should return left pointer
  [InlineData(100, 20)] // Key == separator, should return right pointer
  [InlineData(150, 20)] // Key > separator, should return right pointer
  public void LookupChildPage_OnSingleEntryNode_ReturnsCorrectPointers(int searchKeyValue, int expectedPageIndex)
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    // Node contains one separator key (100)
    // Its pointer (page 10) is for keys < 100
    // The rightmost pointer (page 20) is for keys >= 100
    var separatorKey = new Key([DataValue.CreateInteger(100)]);
    var leftChildPageId = new PageId(1, 10);
    var rightChildPageId = new PageId(1, 20);

    var entryBytes = BTreeInternalNode.SerializeEntry(separatorKey, leftChildPageId, tableDef);
    SlottedPage.TryAddRecord(page, entryBytes, 0);
    new PageHeader(page).RightmostChildPageIndex = rightChildPageId.PageIndex;

    var searchKey = new Key([DataValue.CreateInteger(searchKeyValue)]);
    var expectedChildPageId = new PageId(1, expectedPageIndex);

    // Act
    PageId actualChildPageId = internalNode.LookupChildPage(searchKey);

    // Assert
    Assert.Equal(expectedChildPageId, actualChildPageId);
  }

  // --- Test Data Source for Composite Key Lookups ---
  public static IEnumerable<object[]> CompositeKeyLookup_TestData()
  {
    // Setup:
    // Page contains two separator keys: ('Engineering', 200) and ('Sales', 100)
    // Pointers:
    // - childPageId10: for keys < ('Engineering', 200)
    // - childPageId20: for keys >= ('Engineering', 200) AND < ('Sales', 100)
    // - rightmostChildPageId (30): for keys >= ('Sales', 100)

    // Case 1: Search key is less than all keys
    yield return new object[]
    {
      new Key([DataValue.CreateString("Admin"), DataValue.CreateInteger(999)]),
      10 // Expected PageIndex
    };

    // Case 2: Search key's first part matches, second part is smaller
    yield return new object[]
    {
        new Key([DataValue.CreateString("Engineering"), DataValue.CreateInteger(150)]),
        10 // Expected PageIndex
    };

    // Case 3: Search key exactly matches the first separator key
    yield return new object[]
    {
      new Key([DataValue.CreateString("Engineering"), DataValue.CreateInteger(200)]),
      20 // Expected PageIndex (should go to the next bucket)
    };

    // Case 4: Search key is between the two separator keys
    yield return new object[]
    {
      new Key([DataValue.CreateString("Marketing"), DataValue.CreateInteger(1)]),
      20 // Expected PageIndex
    };

    // Case 5: Search key exactly matches the second (largest) separator key
    yield return new object[]
    {
      new Key([DataValue.CreateString("Sales"), DataValue.CreateInteger(100)]),
      30 // Expected PageIndex (should go to rightmost pointer)
    };

    // Case 6: Search key is greater than all keys
    yield return new object[]
    {
      new Key([DataValue.CreateString("Support"), DataValue.CreateInteger(1)]),
      30 // Expected PageIndex (should go to rightmost pointer)
    };
  }

  [Theory]
  [MemberData(nameof(CompositeKeyLookup_TestData))]
  public void LookupChildPage_WithCompositeKey_ReturnsCorrectPointer(Key searchKey, int expectedPageIndex)
  {
    // Arrange
    var tableDef = CreateCompositePKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);

    var keyEng = new Key([DataValue.CreateString("Engineering"), DataValue.CreateInteger(200)]);
    var childPageId10 = new PageId(1, 10);
    var keySales = new Key([DataValue.CreateString("Sales"), DataValue.CreateInteger(100)]);
    var childPageId20 = new PageId(1, 20);
    var rightmostChildPageId = new PageId(1, 30);

    SlottedPage.TryAddRecord(page, BTreeInternalNode.SerializeEntry(keyEng, childPageId10, tableDef), 0);
    SlottedPage.TryAddRecord(page, BTreeInternalNode.SerializeEntry(keySales, childPageId20, tableDef), 1);
    new PageHeader(page).RightmostChildPageIndex = rightmostChildPageId.PageIndex;

    var expectedChildPageId = new PageId(1, expectedPageIndex);

    // Act
    PageId actualChildPageId = internalNode.LookupChildPage(searchKey);

    // Assert
    Assert.Equal(expectedChildPageId, actualChildPageId);
  }

  [Fact]
  public void LookupChildPage_WithNullSearchKey_ThrowsArgumentNullException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var internalNode = new BTreeInternalNode(page, tableDef);
    Key? nullKey = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("searchKey", () => internalNode.LookupChildPage(nullKey!));
  }
}
