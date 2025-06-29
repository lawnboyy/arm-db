using ArmDb.SchemaDefinition;
using ArmDb.DataModel;
using ArmDb.StorageEngine;
using System.Text;

namespace ArmDb.UnitTests.StorageEngine;

public partial class RecordSerializerTests
{
  [Fact]
  public void Deserialize_WithOnlyFixedNonNullableColumns_ReconstructsCorrectRow()
  {
    // Arrange
    // 1. Define the exact same schema used for the corresponding Serialize test
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Balance", new DataTypeInfo(PrimitiveDataType.BigInt), isNullable: false),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false)
    );

    // 2. Create the original DataRow that we expect to get back
    var expectedRow = new DataRow(
        DataValue.CreateInteger(1024),
        DataValue.CreateBigInteger(5_000_000_000L),
        DataValue.CreateBoolean(true)
    );

    // 3. Define the known-good serialized byte array for the row above.
    // This is the output we validated from the corresponding Serialize test.
    // Format: [NullBitmap][ID][Balance][IsActive]
    var serializedData = new byte[]
    {
      0,                      // Null Bitmap (0b00000000)
      0, 4, 0, 0,             // ID = 1024 (little-endian)
      0, 242, 5, 42, 1, 0, 0, 0,  // Balance = 5,000,000,000 (little-endian)
      1                       // IsActive = true
    };

    // Act
    // Call the Deserialize method (which you will implement)
    DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(tableDef.Columns.Count, actualRow.Arity); // Ensure same number of columns

    // DataRow and DataValue have Equals overrides, so we can compare them directly.
    Assert.Equal(expectedRow, actualRow);

    // For more granular checks:
    // Assert.Equal(expectedRow.Values[0], actualRow.Values[0]); // ID
    // Assert.Equal(expectedRow.Values[1], actualRow.Values[1]); // Balance
    // Assert.Equal(expectedRow.Values[2], actualRow.Values[2]); // IsActive
  }

  [Fact]
  public void Deserialize_WithNullFixedSizeColumns_ReconstructsNullsCorrectly()
  {
    // Arrange
    // 1. Define schema with nullable columns, matching the corresponding Serialize test
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("LastLogin", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: true),
        new ColumnDefinition("StatusCode", new DataTypeInfo(PrimitiveDataType.Int), isNullable: true),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false)
    );

    // 2. This is the original DataRow we expect to get back after deserialization
    var expectedRow = new DataRow(
        DataValue.CreateInteger(10),
        DataValue.CreateNull(PrimitiveDataType.DateTime), // LastLogin is NULL
        DataValue.CreateInteger(200),
        DataValue.CreateBoolean(true)
    );

    // 3. This is the known-good serialized byte array for the row above.
    //    It has bit 1 set in the null bitmap.
    var serializedData = new byte[]
    {
      // --- Header ---
      2,           // Null Bitmap (0b00000010)
      // --- Fixed-Length Data Section (LastLogin is omitted) ---
      10, 0, 0, 0,  // ID = 10
      200, 0, 0, 0, // StatusCode = 200
      1            // IsActive = true
    };

    // Act
    // Call the Deserialize method with the serialized data
    DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(expectedRow.Arity, actualRow.Arity);

    // Assert that the entire reconstructed row is equal to the original
    Assert.Equal(expectedRow, actualRow);

    // For more specific debugging, you can also check individual values:
    Assert.False(actualRow.Values[0].IsNull, "The ID column should not be null.");
    Assert.Equal(10, actualRow.Values[0].GetAs<int>());
    Assert.True(actualRow.Values[1].IsNull, "The LastLogin column should be NULL.");
    Assert.False(actualRow.Values[2].IsNull, "The StatusCode column should not be null.");
    Assert.Equal(200, actualRow.Values[2].GetAs<int>());
    Assert.False(actualRow.Values[3].IsNull, "The IsActive column should not be null.");
    Assert.True(actualRow.Values[3].GetAs<bool>());
  }
}