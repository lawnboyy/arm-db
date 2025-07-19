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

  [Fact]
  public void DeserializePrimaryKey_WithCompositeFixedKey_ExtractsCorrectKey()
  {
    // Arrange
    // 1. Define schema with a composite PK on two INT columns
    var tableDef = new TableDefinition("TestCompositePK");
    tableDef.AddColumn(new ColumnDefinition("PartId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("SubId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("Description", new DataTypeInfo(PrimitiveDataType.Varchar, 100), isNullable: false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("TestCompositePK", new[] { "PartId", "SubId" }));

    // 2. This is the original Key we expect to get back
    var expectedKey = new Key(new[]
    {
        DataValue.CreateInteger(500), // PartId
        DataValue.CreateInteger(10)   // SubId
    });

    // 3. This is the serialized byte array for the full row (500, 10, "Engine Block")
    var serializedData = new byte[]
    {
        // --- Header ---
        0,              // Null Bitmap
        // --- Fixed-Length Data ---
        244, 1, 0, 0,   // PartId = 500 (decimal)
        10, 0, 0, 0,    // SubId = 10 (decimal)
        // --- Variable-Length Data ---
        12, 0, 0, 0,    // Length of "Engine Block" (12)
        69, 110, 103, 105, 110, 101, 32, 66, 108, 111, 99, 107 // "Engine Block"
    };

    // Act
    Key actualKey = RecordSerializer.DeserializePrimaryKey(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualKey);
    Assert.Equal(expectedKey, actualKey);
  }

  [Fact]
  public void DeserializePrimaryKey_WithMixedTypeKey_ExtractsCorrectKey()
  {
    // Arrange
    // 1. Define schema with a composite PK on a VARCHAR and an INT column.
    //    A non-key column is placed first to make the test more robust.
    var tableDef = new TableDefinition("TestMixedPK");
    tableDef.AddColumn(new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("OrgName", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("EmployeeId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("TestMixedPK", ["OrgName", "EmployeeId"]));

    // 2. This is the original Key we expect to get back
    var expectedKey = new Key(new[]
    {
        DataValue.CreateString("Sales"),  // OrgName
        DataValue.CreateInteger(901)    // EmployeeId
    });

    // 3. This is the serialized byte array for the full row (true, "Sales", 901)
    // Format: [NullBitmap][Fixed: IsActive, EmployeeId][Variable: OrgName Length + Data]
    var serializedData = new byte[]
    {
        // --- Header ---
        0,              // Null Bitmap
        // --- Fixed-Length Data ---
        1,              // IsActive = true
        133, 3, 0, 0,   // EmployeeId = 901 (decimal)
        // --- Variable-Length Data ---
        5, 0, 0, 0,     // Length of "Sales" (5)
        83, 97, 108, 101, 115 // "Sales"
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