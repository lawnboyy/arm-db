using System;
using Xunit;
using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
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
}


/*
// To make this compile, you'll need to add the LookupChildPage method to BTreeInternalNode
// and a way to serialize/deserialize its entries.

namespace ArmDb.StorageEngine;

internal sealed class BTreeInternalNode
{
    // ... constructor ...

    internal PageId LookupChildPage(Key searchKey)
    {
        // TODO: Implement binary search over slots.
        // For each slot, deserialize the (Key, PageId) pair.
        // Use KeyComparer to find the correct child pointer.
        // Handle the right-most pointer case.
        throw new NotImplementedException();
    }

    // You will also need these helpers (or similar)
    internal static byte[] SerializeEntry(Key key, PageId childPageId, TableDefinition tableDef)
    {
        // TODO: Implement serialization of the (Key, PageId) pair
        throw new NotImplementedException();
    }

    internal static (Key key, PageId childPageId) DeserializeEntry(ReadOnlySpan<byte> data, TableDefinition tableDef)
    {
        // TODO: Implement deserialization of the (Key, PageId) pair
        throw new NotImplementedException();
    }
}
*/

