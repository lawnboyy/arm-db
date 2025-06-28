using ArmDb.SchemaDefinition;
using ArmDb.DataModel;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public class RecordSerializerTests
{


  [Fact]
  public void Serialize_WithOnlyFixedNonNullableColumns_CreatesCorrectByteArray()
  {
    // Arrange
    // 1. Define the table schema: (ID INT NOT NULL, Balance BIGINT NOT NULL, IsActive BOOL NOT NULL)
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Balance", new DataTypeInfo(PrimitiveDataType.BigInt), isNullable: false), // Assuming you add BigInt -> long
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false)
    );

    // 2. Create the data row corresponding to the schema
    var row = new DataRow(
        DataValue.CreateInteger(1024),          // ID = 1024
        DataValue.CreateBigInteger(5000000000L),  // Balance = 5 billion
        DataValue.CreateBoolean(true)           // IsActive = true
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap: 3 columns -> 1 byte. No nulls, so it's 0b00000000 -> 0x00
    // ID (int, 4 bytes, little-endian for 1024): 00 04 00 00
    // Balance (long, 8 bytes, little-endian for 5,000,000,000): 00 E4 0B 54 01 00 00 00
    // IsActive (bool, 1 byte): 01
    var expectedBytes = new byte[]
    {
      // --- Header ---
      0x00, // Null Bitmap
      // --- Fixed-Length Data ---
      0x00, 0x04, 0x00, 0x00,                               // ID = 1024
      0x00, 0xF2, 0x05, 0x2A, 0x01, 0x00, 0x00, 0x00,       // Balance = 5,000,000,000
      0x01                                                // IsActive = true
    };

    // Act
    // Call the static Serialize method (which doesn't exist yet)
    byte[] actualBytes = RecordSerializer.Serialize(tableDef, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes, actualBytes); // Compare the byte arrays for equality
  }

  [Fact]
  public void Serialize_WithNullFixedSizeColumns_SetsBitmapAndOmitsData()
  {
    // Arrange
    // 1. Define schema with nullable columns:
    // (ID INT NOT NULL, LastLogin DATETIME NULL, StatusCode INT NULL, IsActive BOOL NOT NULL)
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("LastLogin", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: true),
        new ColumnDefinition("StatusCode", new DataTypeInfo(PrimitiveDataType.Int), isNullable: true),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false)
    );

    // 2. Create data row where LastLogin (column 1) is NULL
    var row = new DataRow(
        DataValue.CreateInteger(10),                             // ID
        DataValue.CreateNull(PrimitiveDataType.DateTime),      // LastLogin = NULL
        DataValue.CreateInteger(200),                            // StatusCode
        DataValue.CreateBoolean(true)                            // IsActive
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap (1 byte): 4 columns.
    // Col 0 (ID) is not null -> bit 0 is 0
    // Col 1 (LastLogin) IS NULL -> bit 1 is 1
    // Col 2 (StatusCode) is not null -> bit 2 is 0
    // Col 3 (IsActive) is not null -> bit 3 is 0
    // Bitmap = 0b00000010 = 0x02
    // Fixed-Length Data: Only non-null columns are written
    // ID (int, 4 bytes for 10):         0A 00 00 00
    // LastLogin (DateTime, 8 bytes):  SKIPPED
    // StatusCode (int, 4 bytes for 200):C8 00 00 00
    // IsActive (bool, 1 byte for true): 01
    var expectedBytes = new byte[]
    {
      // --- Header ---
      0x02, // Null Bitmap
      // --- Fixed-Length Data ---
      0x0A, 0x00, 0x00, 0x00, // ID = 10
      0xC8, 0x00, 0x00, 0x00, // StatusCode = 200
      0x01                        // IsActive = true
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes.Length, actualBytes.Length); // Verify final size is correct
    Assert.Equal(expectedBytes, actualBytes); // Compare byte arrays
  }

  // Helper to create a simple table definition for tests
  private static TableDefinition CreateTestTable(params ColumnDefinition[] columns)
  {
    var tableDef = new TableDefinition("TestTable");
    foreach (var col in columns)
    {
      tableDef.AddColumn(col);
    }
    return tableDef;
  }
}

// NOTE: To make this compile, you'll need to:
// 1. Add `BigInt` to your `PrimitiveDataType` enum.
// 2. Add a `DataValue.CreateBigInt(long value)` factory method.
// 3. Create the static class `RecordSerializer` with a placeholder `Serialize` method, like so:

/*
// In ArmDb.StorageEngine/RecordSerializer.cs
namespace ArmDb.StorageEngine;
using ArmDb.SchemaDefinition;
using ArmDb.DataModel;

internal static class RecordSerializer
{
    public static byte[] Serialize(TableDefinition tableDef, DataRow row)
    {
        // TODO: Implement actual serialization logic
        throw new NotImplementedException();
    }
}
*/