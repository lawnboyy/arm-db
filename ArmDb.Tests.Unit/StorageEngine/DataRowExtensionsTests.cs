using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.Storage; // The namespace where the extension method will live

namespace ArmDb.Tests.Unit.Storage;

public class DataRowExtensionsTests
{
  [Fact]
  public void GetPrimaryKey_WithSingleColumnKey_ReturnsCorrectKey()
  {
    // Arrange
    // 1. Define schema: (ID INT PK, Name VARCHAR)
    var tableDef = CreateTestTable(new[] { "ID" },
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: true)
    );

    // 2. Create the source DataRow
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(123),
        DataValue.CreateString("Test")
    );

    // 3. This is the Key object we expect to be created
    var expectedKey = new Key([DataValue.CreateInteger(123)]);

    // Act
    // Call the extension method on the DataRow instance
    Key actualKey = row.GetPrimaryKey(tableDef);

    // Assert
    Assert.NotNull(actualKey);
    Assert.Equal(expectedKey, actualKey);
  }

  [Fact]
  public void GetPrimaryKey_WithCompositeKey_ReturnsCorrectKeyInOrder()
  {
    // Arrange
    // 1. Define schema with a composite PK on (OrgName, EmployeeId)
    var tableDef = CreateTestTable(new[] { "OrgName", "EmployeeId" },
        new ColumnDefinition("OrgName", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false),
        new ColumnDefinition("EmployeeId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false)
    );

    // 2. Create the source DataRow
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateString("Sales"),
        DataValue.CreateInteger(901),
        DataValue.CreateBoolean(true)
    );

    // 3. This is the Key object we expect, with values in the correct PK order
    var expectedKey = new Key(
    [
        DataValue.CreateString("Sales"),
        DataValue.CreateInteger(901)
    ]);

    // Act
    Key actualKey = row.GetPrimaryKey(tableDef);

    // Assert
    Assert.NotNull(actualKey);
    Assert.Equal(expectedKey, actualKey);
  }

  [Fact]
  public void GetPrimaryKey_WithReorderedCompositeKey_ReturnsKeyInCorrectPKOrder()
  {
    // Arrange
    // 1. Define schema where PK column order is different from table column order
    var tableDef = CreateTestTable(new[] { "LastName", "FirstName" }, // PK is (LastName, FirstName)
                                                                      // Table column order is (FirstName, IsActive, LastName)
        new ColumnDefinition("FirstName", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false),
        new ColumnDefinition("LastName", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false)
    );

    // 2. Create the source DataRow, with values matching the physical table column order
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateString("Alice"), // FirstName
        DataValue.CreateBoolean(true),   // IsActive
        DataValue.CreateString("Smith")  // LastName
    );

    // 3. This is the Key object we expect, with values in the logical PRIMARY KEY order
    var expectedKey = new Key(
    [
        DataValue.CreateString("Smith"), // LastName comes first in the PK
        DataValue.CreateString("Alice")  // FirstName comes second
    ]);

    // Act
    Key actualKey = row.GetPrimaryKey(tableDef);

    // Assert
    Assert.NotNull(actualKey);
    Assert.Equal(expectedKey, actualKey);
  }

  private static TableDefinition CreateTestTable(string[] pkColumnNames, params ColumnDefinition[] columns)
  {
    var tableDef = new TableDefinition("TestTable");
    foreach (var col in columns)
    {
      tableDef.AddColumn(col);
    }
    // Only add PK if names are provided
    if (pkColumnNames.Length > 0)
    {
      tableDef.AddConstraint(new PrimaryKeyConstraint("TestTable", pkColumnNames));
    }
    return tableDef;
  }
}