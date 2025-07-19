using System;
using System.Text;
using Xunit;
using ArmDb.SchemaDefinition;
using ArmDb.DataModel;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class RecordSerializerTests // Use partial to extend the existing test class
{
  [Fact]
  public void DeserializePrimaryKey_WithSingleFixedColumn_ExtractsCorrectKey()
  {
    // Arrange
    // 1. Define schema: (ID INT PK NOT NULL, Name VARCHAR NOT NULL)
    var tableDef = CreateTestTableWithPK("ID",
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false)
    );

    // 2. This is the original Key we expect to get back
    var expectedKey = new Key(new[] { DataValue.CreateInteger(123) });

    // 3. This is the known-good serialized byte array for the full row (123, "Alice")
    var serializedData = new byte[]
    {
            0,            // Null Bitmap
            123, 0, 0, 0, // ID = 123
            5, 0, 0, 0,   // Length of "Alice" (5)
            65, 108, 105, 99, 101 // "Alice"
    };

    // Act
    Key actualKey = RecordSerializer.DeserializePrimaryKey(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualKey);
    Assert.Equal(expectedKey, actualKey);
  }

  // Helper to create a table definition with a primary key
  private static TableDefinition CreateTestTableWithPK(string pkColumnName, params ColumnDefinition[] columns)
  {
    var tableDef = new TableDefinition("TestTableWithPK");
    foreach (var col in columns)
    {
      tableDef.AddColumn(col);
    }
    tableDef.AddConstraint(new PrimaryKeyConstraint("TestTableWithPK", new[] { pkColumnName }));
    return tableDef;
  }
}