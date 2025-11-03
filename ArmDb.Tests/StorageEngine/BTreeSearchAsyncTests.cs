using ArmDb.DataModel;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeTests
{
  // [Fact]
  // public async Task SearchAsync_OnEmptyTree_ReturnsNull()
  // {
  //   // Arrange
  //   // 1. Create a new, empty BTree. This creates the root leaf page.
  //   var btree = await BTree.CreateAsync(_bpm, _tableDef);

  //   // 2. A key to search for (any key)
  //   var searchKey = new Key([DataValue.CreateInteger(100)]);

  //   // Act
  //   // 3. This is the method to implement.
  //   var result = await btree.SearchAsync(searchKey);

  //   // Assert
  //   // 4. On an empty tree, the search should find nothing.
  //   Assert.Null(result);
  // }
}