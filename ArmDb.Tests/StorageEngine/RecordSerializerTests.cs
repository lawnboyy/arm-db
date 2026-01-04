using ArmDb.SchemaDefinition;
using ArmDb.DataModel;
using ArmDb.Storage;

namespace ArmDb.UnitTests.Storage;

public partial class RecordSerializerTests
{
  [Fact]
  public void SerializeAndDeserialize_RoundTrip_ReturnsEqualDataRow()
  {
    // Arrange
    // 1. Define a comprehensive, interleaved schema
    var tableDef = CreateTestTable(
        new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: true),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false),
        new ColumnDefinition("Notes", new DataTypeInfo(PrimitiveDataType.Varchar, 200), isNullable: false),
        new ColumnDefinition("Balance", new DataTypeInfo(PrimitiveDataType.BigInt), isNullable: false),
        new ColumnDefinition("LastLogin", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: true),
        new ColumnDefinition("Avatar", new DataTypeInfo(PrimitiveDataType.Blob, 4096), isNullable: true)
    );

    // 2. Create the original DataRow with a mix of data types and nulls
    var originalRow = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(12345),
        DataValue.CreateString("Zürich Test 😊"), // Multi-byte characters
        DataValue.CreateBoolean(true),
        DataValue.CreateString(""), // Empty string
        DataValue.CreateBigInteger(9_876_543_210L),
        DataValue.CreateNull(PrimitiveDataType.DateTime),
        DataValue.CreateBlob(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF })
    );

    // Act
    // 3. Serialize the row to bytes
    byte[] serializedBytes = RecordSerializer.Serialize(tableDef.Columns, originalRow);

    // 4. Deserialize the bytes back into a new row object
    ArmDb.DataModel.Record deserializedRow = RecordSerializer.Deserialize(tableDef.Columns, serializedBytes.AsSpan());

    // Assert
    // 5. The deserialized row should be identical to the original row.
    //    This relies on the Equals overrides on DataRow and DataValue.
    Assert.NotNull(deserializedRow);
    Assert.Equal(originalRow, deserializedRow);
  }
}