using ArmDb.SchemaDefinition;
using ArmDb.DataModel;
using ArmDb.StorageEngine;

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
    Assert.Equal(expectedRow.Values[0], actualRow.Values[0]); // ID
    Assert.Equal(expectedRow.Values[1], actualRow.Values[1]); // Balance
    Assert.Equal(expectedRow.Values[2], actualRow.Values[2]); // IsActive
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

  [Fact]
  public void Deserialize_WithInterleavedColumns_ReconstructsCorrectRow()
  {
    // Arrange
    // 1. Define the interleaved schema
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false),
        new ColumnDefinition("Notes", new DataTypeInfo(PrimitiveDataType.Varchar, 200), isNullable: true)
    );

    // 2. This is the original DataRow we expect to get back
    var expectedRow = new DataRow(
        DataValue.CreateInteger(123),
        DataValue.CreateString("Test User"),
        DataValue.CreateBoolean(true),
        DataValue.CreateNull(PrimitiveDataType.Varchar)
    );

    // 3. This is the known-good serialized byte array.
    // Format: [NullBitmap][Fixed: ID][Fixed: IsActive][Variable: Name Length + Data]
    var serializedData = new byte[]
    {
      // --- Header ---
      8,           // Null Bitmap (0b00001000, for column 3 'Notes')
      // --- Fixed-Length Data Section ---
      123, 0, 0, 0,  // ID = 123
      1,             // IsActive = true
      // --- Variable-Length Data Section ---
      9, 0, 0, 0,   // Length of "Test User" (9)
      84, 101, 115, 116, 32, 85, 115, 101, 114 // "Test User"
    };

    // Act
    DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(expectedRow.Arity, actualRow.Arity);
    Assert.Equal(expectedRow, actualRow);

    // Granular check to be explicit
    Assert.Equal(123, actualRow.Values[0].GetAs<int>());
    Assert.Equal("Test User", actualRow.Values[1].GetAs<string>());
    Assert.True(actualRow.Values[2].GetAs<bool>());
    Assert.True(actualRow.Values[3].IsNull);
  }

  [Fact]
  public void Deserialize_WithBlobColumn_ReconstructsCorrectly()
  {
    // Arrange
    // 1. Define schema with a fixed type and a BLOB
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("ImageData", new DataTypeInfo(PrimitiveDataType.Blob, 1024), isNullable: false)
    );

    // 2. This is the original DataRow we expect to get back
    var blobData = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE };
    var expectedRow = new DataRow(
        DataValue.CreateInteger(1337),
        DataValue.CreateBlob(blobData)
    );

    // 3. This is the known-good serialized byte array for the row above.
    // Format: [NullBitmap][ID][BlobLength][BlobData]
    var serializedData = new byte[]
    {
      // --- Header ---
      0,           // Null Bitmap
      // --- Fixed-Length Data ---
      57, 5, 0, 0, // ID = 1337 (decimal)
      // --- Variable-Length Data ---
      6, 0, 0, 0,  // Length of blobData (6)
      0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE // The blob data
    };

    // Act
    DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(expectedRow, actualRow);

    // Granular check on the blob data
    var actualBlob = actualRow.Values[1].GetAs<byte[]>();
    Assert.True(blobData.AsSpan().SequenceEqual(actualBlob));
  }

  [Fact]
  public void Deserialize_WithEmptyVarchar_ReconstructsCorrectly()
  {
    // Arrange
    // 1. Define schema with a Varchar column
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Tag", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false)
    );

    // 2. This is the original DataRow we expect to get back
    var expectedRow = new DataRow(
        DataValue.CreateInteger(404),
        DataValue.CreateString("") // The Tag is an empty string
    );

    // 3. This is the known-good serialized byte array.
    // Format: [NullBitmap][ID][TagLength][TagData]
    var serializedData = new byte[]
    {
      // --- Header ---
      0,            // Null Bitmap (no nulls)
      // --- Fixed-Length Data ---
      148, 1, 0, 0, // ID = 404 (0x0194)
      // --- Variable-Length Data ---
      0, 0, 0, 0    // Length of Tag "" is 0
                    // 0 bytes of data for Tag
    };

    // Act
    DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(expectedRow, actualRow);

    // Granular check
    var tagValue = actualRow.Values[1];
    Assert.False(tagValue.IsNull);
    Assert.Equal(string.Empty, tagValue.GetAs<string>());
  }

  [Fact]
  public void Deserialize_WithEmptyBlob_ReconstructsCorrectly()
  {
    // Arrange
    // 1. Define schema with a Blob column
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Blob, 1024), isNullable: false)
    );

    // 2. This is the original DataRow we expect to get back
    var expectedRow = new DataRow(
        DataValue.CreateInteger(500),
        DataValue.CreateBlob(Array.Empty<byte>()) // The Data is an empty byte array
    );

    // 3. This is the known-good serialized byte array.
    // Format: [NullBitmap][ID][DataLength][DataData]
    var serializedData = new byte[]
    {
      // --- Header ---
      0,            // Null Bitmap (no nulls)
      // --- Fixed-Length Data ---
      244, 1, 0, 0, // ID = 500 (0x01F4)
      // --- Variable-Length Data ---
      0, 0, 0, 0    // Length of Data byte[0] is 0
                    // 0 bytes of data for Data
    };

    // Act
    DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(expectedRow, actualRow);

    // Granular check
    var blobValue = actualRow.Values[1];
    Assert.False(blobValue.IsNull);
    Assert.Empty(blobValue.GetAs<byte[]>());
  }

  [Fact]
  public void Deserialize_WithAllNullValues_ReconstructsAllNullRow()
  {
    // Arrange
    // 1. Define schema with all nullable columns
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: true),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: true),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: true)
    );

    // 2. This is the original DataRow we expect to get back
    var expectedRow = new DataRow(
        DataValue.CreateNull(PrimitiveDataType.Int),
        DataValue.CreateNull(PrimitiveDataType.Varchar),
        DataValue.CreateNull(PrimitiveDataType.Boolean)
    );

    // 3. This is the known-good serialized byte array.
    // It contains ONLY the null bitmap with bits 0, 1, and 2 set.
    // Bitmap = 0b00000111 = 7 (decimal)
    var serializedData = new byte[] { 7 };

    // Act
    DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(expectedRow, actualRow);

    // Granular check for clarity
    Assert.True(actualRow.Values.All(v => v.IsNull));
  }

  [Fact]
  public void Deserialize_WithMultiByteCharacters_ReconstructsCorrectString()
  {
    // Arrange
    // 1. Define the schema
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("City", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false)
    );

    // 2. This is the original DataRow we expect to get back
    string cityValue = "Zürich";
    var expectedRow = new DataRow(
        DataValue.CreateInteger(101),
        DataValue.CreateString(cityValue)
    );

    // 3. This is the known-good serialized byte array for the row above.
    // "Zürich" has 6 characters but takes 7 bytes in UTF-8.
    var serializedData = new byte[]
    {
      // --- Header ---
      0,                      // Null Bitmap
      // --- Fixed-Length Data ---
      101, 0, 0, 0,           // ID = 101
      // --- Variable-Length Data ---
      7, 0, 0, 0,             // Length of "Zürich" in bytes (7)
      90, 195, 188, 114, 105, 99, 104 // "Zürich" (Z, ü, r, i, c, h) in UTF-8
    };

    // Act
    DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(expectedRow, actualRow);

    // Granular check for clarity
    Assert.Equal(cityValue, actualRow.Values[1].GetAs<string>());
  }

  [Fact]
  public void Deserialize_WithAllDataTypesMixedAndNulls_ReconstructsCorrectRow()
  {
    // Arrange
    // 1. Define the exact same complex schema from the corresponding Serialize test
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false),
        new ColumnDefinition("LastLogin", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: true),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false),
        new ColumnDefinition("Notes", new DataTypeInfo(PrimitiveDataType.Varchar, 200), isNullable: false),
        new ColumnDefinition("Balance", new DataTypeInfo(PrimitiveDataType.BigInt), isNullable: false),
        new ColumnDefinition("Avatar", new DataTypeInfo(PrimitiveDataType.Blob, 4096), isNullable: true)
    );

    // 2. This is the original DataRow we expect to get back
    var expectedRow = new DataRow(
        DataValue.CreateInteger(101),
        DataValue.CreateString("Test User"),
        DataValue.CreateNull(PrimitiveDataType.DateTime),  // LastLogin is NULL
        DataValue.CreateBoolean(true),
        DataValue.CreateString(""),                        // Notes is an empty string
        DataValue.CreateBigInteger(5_000_000_000L),
        DataValue.CreateNull(PrimitiveDataType.Blob)       // Avatar is NULL
    );

    // 3. This is the known-good serialized byte array for the row above.
    // Format: [NullBitmap][Fixed:ID,IsActive,Balance][Variable:NameLength,NameData,NotesLength,NotesData]
    var serializedData = new byte[]
    {
        // --- Header ---
        68,                         // Null Bitmap (0b01000100 for nulls at index 2 and 6)
        // --- Fixed-Length Data Section ---
        101, 0, 0, 0,               // ID = 101
        1,                          // IsActive = true
        0, 242, 5, 42, 1, 0, 0, 0,  // Balance = 5,000,000,000
        // --- Variable-Length Data Section ---
        9, 0, 0, 0,                 // Length of "Test User" (9)
        84, 101, 115, 116, 32, 85, 115, 101, 114, // "Test User"
        0, 0, 0, 0                  // Length of Notes "" (0)
    };

    // Act
    DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(expectedRow, actualRow);
  }

  [Fact]
  public void Deserialize_WithDateTime_ReconstructsKindCorrectly()
  {
    // Arrange
    // 1. Define schema with two DateTime columns
    var tableDef = CreateTestTable(
        new ColumnDefinition("EventId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("TimestampUtc", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: false),
        new ColumnDefinition("TimestampLocal", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: false)
    );

    // 2. Create two DateTime objects with different Kind properties
    var utcTimestamp = new DateTime(2025, 7, 5, 15, 30, 0, DateTimeKind.Utc);
    var localTimestamp = new DateTime(2025, 7, 5, 10, 30, 0, DateTimeKind.Local);

    // This is the original DataRow we expect to get back
    var expectedRow = new DataRow(
        DataValue.CreateInteger(42),
        DataValue.CreateDateTime(utcTimestamp),
        DataValue.CreateDateTime(localTimestamp)
    );

    // 3. Manually construct the serialized byte array using ToBinary() for both dates
    long binaryUtc = utcTimestamp.ToBinary();
    long binaryLocal = localTimestamp.ToBinary();

    using (var ms = new MemoryStream())
    using (var writer = new BinaryWriter(ms))
    {
      writer.Write((byte)0);              // Null Bitmap
      writer.Write((int)42);              // EventId
      writer.Write(binaryUtc);            // TimestampUtc
      writer.Write(binaryLocal);          // TimestampLocal
      var serializedData = ms.ToArray();

      // Act
      DataRow actualRow = RecordSerializer.Deserialize(tableDef, serializedData.AsSpan());

      // Assert
      Assert.NotNull(actualRow);
      Assert.Equal(expectedRow, actualRow);

      // Granular assertions to be explicit
      var deserializedUtc = actualRow.Values[1].GetAs<DateTime>();
      var deserializedLocal = actualRow.Values[2].GetAs<DateTime>();

      Assert.Equal(DateTimeKind.Utc, deserializedUtc.Kind);
      Assert.Equal(utcTimestamp.Ticks, deserializedUtc.Ticks);

      Assert.Equal(DateTimeKind.Local, deserializedLocal.Kind);
      Assert.Equal(localTimestamp.Ticks, deserializedLocal.Ticks);
    }
  }
}