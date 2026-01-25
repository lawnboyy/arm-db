using System;
using System.Text;
using Xunit;
using ArmDb.SchemaDefinition;
using ArmDb.DataModel;
using ArmDb.Storage;

namespace ArmDb.Tests.Unit.Storage;

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

  [Fact]
  public void DeserializePrimaryKey_WithNullInKey_ThrowsInvalidDataException()
  {
    // Arrange
    // 1. Define schema where ID is a non-nullable PK
    var tableDef = CreateTestTableWithPK("ID",
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false)
    );

    // 2. This is a corrupted serialized byte array.
    // The null bitmap (first byte) has bit 0 set to 1, marking the 'ID' column as NULL,
    // which violates the primary key constraint.
    var corruptedData = new byte[]
    {
        // --- Header ---
        1,              // Null Bitmap (0b00000001) -> Col 0 is NULL
        // --- Fixed-Length Data ---
        // (No data for ID because it's marked as null)
        // --- Variable-Length Data ---
        5, 0, 0, 0,     // Length of "Alice" (5)
        65, 108, 105, 99, 101 // "Alice"
    };

    // Act & Assert
    // The method should detect the invalid state and throw an exception.
    // InvalidDataException is a good choice for when data stream is not in the expected format.
    var ex = Assert.Throws<InvalidDataException>(() =>
        RecordSerializer.DeserializePrimaryKey(tableDef, corruptedData.AsSpan())
    );

    // Optional: Verify the exception message is helpful
    Assert.Contains("Primary key column 'ID' cannot be null.", ex.Message);
  }

  [Fact]
  public void DeserializePrimaryKey_WithKeyColumnNotFirstInFixedSection_ExtractsCorrectly()
  {
    // Arrange
    // 1. Define schema where the PK ('UserID') is not the first column,
    //    and also not the first fixed-size column.
    var tableDef = new TableDefinition("TestKeyNotFirst");
    tableDef.AddColumn(new ColumnDefinition("FirstName", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("UserID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("TestKeyNotFirst", new[] { "UserID" }));

    // 2. This is the original Key we expect to get back
    var expectedKey = new Key(new[] { DataValue.CreateInteger(123) });

    // 3. This is the serialized byte array for the full row ("Alice", true, 123)
    // Format: [NullBitmap][Fixed: IsActive, UserID][Variable: FirstName Length + Data]
    var serializedData = new byte[]
    {
        // --- Header ---
        0,              // Null Bitmap
        // --- Fixed-Length Data Section (in schema order) ---
        1,              // IsActive = true
        123, 0, 0, 0,   // UserID = 123
        // --- Variable-Length Data Section ---
        5, 0, 0, 0,     // Length of "Alice" (5)
        65, 108, 105, 99, 101 // "Alice"
    };

    // Act
    Key actualKey = RecordSerializer.DeserializePrimaryKey(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualKey);
    Assert.Equal(expectedKey, actualKey);
  }

  [Fact]
  public void DeserializePrimaryKey_WithReorderedCompositeKey_ReturnsKeyInCorrectOrder()
  {
    // Arrange
    // 1. Define a schema where the Primary Key column order is DIFFERENT
    //    from the table's column order.
    var tableDef = new TableDefinition("TestReorderedPK");
    // Table column order: ColA (fixed), ColB (variable), ColC (fixed)
    tableDef.AddColumn(new ColumnDefinition("ColA", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("ColB", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("ColC", new DataTypeInfo(PrimitiveDataType.BigInt), isNullable: false));
    // Primary Key order: ColC, ColA
    tableDef.AddConstraint(new PrimaryKeyConstraint("TestReorderedPK", ["ColC", "ColA"]));

    // 2. This is the original Key we expect to get back, in PK order (ColC, then ColA)
    var expectedKey = new Key(
    [
        DataValue.CreateBigInteger(999L), // ColC's value
        DataValue.CreateInteger(10)     // ColA's value
    ]);

    // 3. This is the serialized byte array for the full row (10, "hello", 999L).
    //    Note that the fixed-size data is stored in TABLE order (ColA, then ColC).
    var serializedData = new byte[]
    {
        // --- Header ---
        0,                      // Null Bitmap
        // --- Fixed-Length Data Section (in table column order) ---
        10, 0, 0, 0,            // ColA = 10
        231, 3, 0, 0, 0, 0, 0, 0, // ColC = 999
        // --- Variable-Length Data Section ---
        5, 0, 0, 0,             // Length of "hello" (5)
        104, 101, 108, 108, 111 // "hello"
    };

    // Act
    // The DeserializePrimaryKey method must read the data in table order
    // but construct the Key object in primary key order.
    Key actualKey = RecordSerializer.DeserializePrimaryKey(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualKey);
    Assert.Equal(expectedKey, actualKey);
  }

  [Fact]
  public void DeserializePrimaryKey_WithComplexReorderedCompositeKey_ReturnsKeyInCorrectOrder()
  {
    // Arrange
    // 1. Define a schema where the PK order is different from the table's physical column order.
    var tableDef = new TableDefinition("TestComplexReorderedPK");
    // Physical column order: Region (var), CustomerID (fixed), IsActive (fixed), OrderDate (fixed), OrderID (fixed)
    tableDef.AddColumn(new ColumnDefinition("Region", new DataTypeInfo(PrimitiveDataType.Varchar, 10), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("CustomerID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("OrderDate", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("OrderID", new DataTypeInfo(PrimitiveDataType.BigInt), isNullable: false));
    // Primary Key logical order: OrderDate, Region, CustomerID
    tableDef.AddConstraint(new PrimaryKeyConstraint("TestComplexReorderedPK", new[] { "OrderDate", "Region", "CustomerID" }));

    // 2. This is the original Key we expect to get back, in PK order.
    var orderDate = new DateTime(2025, 8, 3, 10, 30, 0, DateTimeKind.Utc);
    long ticks = orderDate.ToBinary();
    var expectedKey = new Key(new[]
    {
        DataValue.CreateDateTime(orderDate), // 1st in PK
        DataValue.CreateString("NA"),        // 2nd in PK
        DataValue.CreateInteger(123)         // 3rd in PK
    });

    // 3. This is the serialized byte array for the full row.
    //    Data is stored in PHYSICAL table order.
    var serializedData = new byte[]
    {
        // --- Header ---
        0,                                  // Null Bitmap
        // --- Fixed-Length Data Section (in table column order) ---
        123, 0, 0, 0,                       // CustomerID (int)
        1,                                  // IsActive (bool)
        // OrderDate (DateTime as long via ToBinary)
        0, 4, 38, 179, 120, 210, 221, 72,
        // OrderID (long)
        202, 113, 15, 99, 2, 0, 0, 0,
        // --- Variable-Length Data Section (in table column order) ---
        2, 0, 0, 0,                         // Length of "NA" (2)
        78, 65                              // "NA"
    };

    // Act
    // The DeserializePrimaryKey method must read the data in physical order
    // but construct the Key object in the logical primary key order.
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