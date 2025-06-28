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
}