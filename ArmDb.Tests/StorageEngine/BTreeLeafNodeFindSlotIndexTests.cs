using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeLeafNodeTests
{
  [Theory]
  // Key to Search For, Expected Slot Index
  [InlineData(40, 3)] // The initial midpoint
  [InlineData(20, 1)] // A value in the lower half
  [InlineData(60, 5)] // A value in the upper half
  [InlineData(10, 0)] // The first element
  [InlineData(70, 6)] // The last element
  public void FindSlotIndex_WhenKeyExists_IteratesAndReturnsCorrectIndex(int keyToFind, int expectedIndex)
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var nullVarchar = DataValue.CreateNull(PrimitiveDataType.Varchar);

    // Populate the page with a larger, odd number of sorted records
    SlottedPage.TryAddItem(page, RecordSerializer.Serialize(tableDef, new DataRow(DataValue.CreateInteger(10), nullVarchar)), 0);
    SlottedPage.TryAddItem(page, RecordSerializer.Serialize(tableDef, new DataRow(DataValue.CreateInteger(20), nullVarchar)), 1);
    SlottedPage.TryAddItem(page, RecordSerializer.Serialize(tableDef, new DataRow(DataValue.CreateInteger(30), nullVarchar)), 2);
    SlottedPage.TryAddItem(page, RecordSerializer.Serialize(tableDef, new DataRow(DataValue.CreateInteger(40), nullVarchar)), 3);
    SlottedPage.TryAddItem(page, RecordSerializer.Serialize(tableDef, new DataRow(DataValue.CreateInteger(50), nullVarchar)), 4);
    SlottedPage.TryAddItem(page, RecordSerializer.Serialize(tableDef, new DataRow(DataValue.CreateInteger(60), nullVarchar)), 5);
    SlottedPage.TryAddItem(page, RecordSerializer.Serialize(tableDef, new DataRow(DataValue.CreateInteger(70), nullVarchar)), 6);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateInteger(keyToFind)]);

    // Act
    int foundIndex = leafNode.FindPrimaryKeySlotIndex(searchKey);

    // Assert
    Assert.Equal(expectedIndex, foundIndex);
  }

  private static TableDefinition CreateIntPKTable()
  {
    var tableDef = new TableDefinition("IntPKTable");
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Varchar, 100), isNullable: true));
    tableDef.AddConstraint(new PrimaryKeyConstraint("IntPKTable", ["Id"]));
    return tableDef;
  }
}