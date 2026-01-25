using ArmDb.DataModel;
using ArmDb.Storage;

namespace ArmDb.Tests.Unit.Storage.BTreeTests;

public partial class BTreeLeafNodeTests
{
  public static IEnumerable<object[]> DeleteScenarios_TestData()
  {
    // Initial set of keys for all scenarios
    var initialKeys = new[] { 10, 20, 30, 40 };

    // Scenario 1: Delete a middle item
    yield return new object[]
    {
            20, // Key to delete
            initialKeys,
            new[] { 10, 30, 40 } // Expected remaining keys
    };

    // Scenario 2: Delete the first item
    yield return new object[]
    {
            10, // Key to delete
            initialKeys,
            new[] { 20, 30, 40 } // Expected remaining keys
    };

    // Scenario 3: Delete the last item
    yield return new object[]
    {
            40, // Key to delete
            initialKeys,
            new[] { 10, 20, 30 } // Expected remaining keys
    };
  }

  [Theory]
  [MemberData(nameof(DeleteScenarios_TestData))]
  public void Delete_WhenKeyExists_RemovesRecordAndReturnsTrue(int keyToDelete, int[] initialKeys, int[] expectedRemainingKeys)
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Populate the page with the initial set of records
    foreach (var key in initialKeys)
    {
      var row = new ArmDb.DataModel.Record(DataValue.CreateInteger(key), DataValue.CreateString($"Data for {key}"));
      var result = leafNode.TryInsert(row);
      Assert.True(result);
    }
    Assert.Equal(initialKeys.Length, leafNode.ItemCount); // Verify setup

    var searchKeyToDelete = new Key([DataValue.CreateInteger(keyToDelete)]);

    // Act
    bool success = leafNode.Delete(searchKeyToDelete);

    // Assert
    Assert.True(success, "Delete should return true for an existing key.");
    Assert.Equal(expectedRemainingKeys.Length, leafNode.ItemCount);

    // Verify the deleted key is no longer found
    Assert.Null(leafNode.Search(searchKeyToDelete));

    // Verify all remaining keys are still present
    foreach (var key in expectedRemainingKeys)
    {
      var foundRow = leafNode.Search(new Key([DataValue.CreateInteger(key)]));
      Assert.NotNull(foundRow);
      Assert.Equal(key, foundRow.Values[0].GetAs<int>());
    }
  }

  [Fact]
  public void Delete_WhenKeyDoesNotExist_ReturnsFalseAndDoesNotModifyPage()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    var row10 = new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row30 = new ArmDb.DataModel.Record(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30"));
    leafNode.TryInsert(row10);
    leafNode.TryInsert(row30);

    var pageStateBefore = page.Data.ToArray(); // Snapshot the page state
    var initialItemCount = leafNode.ItemCount;

    var nonExistentKey = new Key([DataValue.CreateInteger(20)]); // Key does not exist

    // Act
    bool success = leafNode.Delete(nonExistentKey);

    // Assert
    Assert.False(success, "Delete should return false for a non-existent key.");
    Assert.Equal(initialItemCount, leafNode.ItemCount); // Item count should not change

    // Verify the page content is completely unmodified
    var pageStateAfter = page.Data.ToArray();
    Assert.True(pageStateBefore.SequenceEqual(pageStateAfter));
  }

  [Fact]
  public void Delete_WhenKeyIsNull_ThrowsArgumentNullException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);
    leafNode.TryInsert(new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10")));
    Key? nullKey = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("keyToDelete", () => leafNode.Delete(nullKey!));
  }
}
