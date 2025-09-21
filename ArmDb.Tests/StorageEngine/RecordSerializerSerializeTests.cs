using ArmDb.SchemaDefinition;
using ArmDb.DataModel;
using ArmDb.StorageEngine;
using System.Text;

namespace ArmDb.UnitTests.StorageEngine;

public partial class RecordSerializerTests
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
    var row = new ArmDb.DataModel.Record(
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
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

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
    var row = new ArmDb.DataModel.Record(
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
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes.Length, actualBytes.Length); // Verify final size is correct
    Assert.Equal(expectedBytes, actualBytes); // Compare byte arrays
  }

  [Fact]
  public void Serialize_WithVariableLengthColumns_WritesLengthPrefixAndData()
  {
    // Arrange
    // 1. Define schema with a mix of fixed and variable types
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, maxLength: 32), isNullable: false),
        new ColumnDefinition("Status", new DataTypeInfo(PrimitiveDataType.Varchar, maxLength: 32), isNullable: false)
    );

    // 2. Create data row
    string nameValue = "Alice";
    string statusValue = "Active";
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(99),
        DataValue.CreateString(nameValue),
        DataValue.CreateString(statusValue)
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap (1 byte): No nulls -> 0x00
    //
    // Fixed-Length Data Section:
    // ID (int, 4 bytes for 99): 63 00 00 00
    //
    // Variable-Length Data Section:
    // Name Length (int, 4 bytes for 5): 05 00 00 00
    // Name Data ("Alice" in UTF-8, 5 bytes): 41 6C 69 63 65
    // Status Length (int, 4 bytes for 6): 06 00 00 00
    // Status Data ("Active" in UTF-8, 6 bytes): 41 63 74 69 76 65
    var expectedBytes = new byte[]
    {
      // --- Header ---
      0x00,                                           // Null Bitmap            decimal: [0]
      // --- Fixed-Length Data ---
      0x63, 0x00, 0x00, 0x00,                         // ID = 99,               decimal: [99, 0, 0, 0]
      // --- Variable-Length Data ---
      0x05, 0x00, 0x00, 0x00,                         // Length of "Alice" (5), decimal: [5, 0, 0, 0]
      0x41, 0x6C, 0x69, 0x63, 0x65,                   // "Alice"                decimal: [65, 108, 105, 99, 101]
      0x06, 0x00, 0x00, 0x00,                         // Length of "Active" (6) decimal: [6, 0, 0, 0]      
      0x41, 0x63, 0x74, 0x69, 0x76, 0x65            // "Active"                 decimal: [65, 99, 116, 105, 118, 101]
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes, actualBytes); // Compare byte arrays
  }

  [Fact]
  public void Serialize_WithInterleavedFixedAndVariableColumns_SeparatesSectionsCorrectly()
  {
    // Arrange
    // 1. Define an interleaved schema: (ID INT, Name VARCHAR, IsActive BOOL, Notes VARCHAR NULL)
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, maxLength: 32), isNullable: false),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false),
        new ColumnDefinition("Notes", new DataTypeInfo(PrimitiveDataType.Varchar, maxLength: 32), isNullable: true)
    );

    // 2. Create data row. Make the last column NULL to test all features at once.
    string nameValue = "Test User"; // 9 bytes in UTF-8
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(123),
        DataValue.CreateString(nameValue),
        DataValue.CreateBoolean(true),
        DataValue.CreateNull(PrimitiveDataType.Varchar) // Notes column is NULL
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap (1 byte): 4 columns. 4th col is NULL, so bit 3 is 1 -> 0b00001000 -> 0x08
    //
    // Fixed-Length Data Section (contiguous):
    // ID (int, 4 bytes for 123):         7B 00 00 00
    // IsActive (bool, 1 byte for true):  01
    //
    // Variable-Length Data Section (contiguous):
    // Name Length (int, 4 bytes for 9):  09 00 00 00
    // Name Data ("Test User", 9 bytes):  54 65 73 74 20 55 73 65 72
    //
    // Note: The 'Notes' column is NULL, so it takes up no space in the data sections.
    var expectedBytes = new byte[]
    {
      // --- Header ---
      0x08, // Null Bitmap (8)

      // --- Fixed-Length Data Section ---
      0x7B, 0x00, 0x00, 0x00, // ID = 123
      0x01,                   // IsActive = true

      // --- Variable-Length Data Section ---
      0x09, 0x00, 0x00, 0x00, // Length of "Test User" (9)
      // "Test User" in UTF-8
      0x54, 0x65, 0x73, 0x74, 0x20, 0x55, 0x73, 0x65, 0x72
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    Assert.NotNull(actualBytes);
    // Use Assert.Equal for a more helpful failure message if arrays differ
    Assert.Equal(expectedBytes, actualBytes);
  }

  [Fact]
  public void Serialize_WithBlobColumn_WritesLengthPrefixAndDataCorrectly()
  {
    // Arrange
    // 1. Define schema with a fixed type and a BLOB
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("ImageData", new DataTypeInfo(PrimitiveDataType.Blob, maxLength: 32), isNullable: false)
    );

    // 2. Create data row with a sample byte array for the blob
    var blobData = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE }; // 6 bytes
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(1337),
        DataValue.CreateBlob(blobData)
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap (1 byte): No nulls -> 0x00
    //
    // Fixed-Length Data Section:
    // ID (int, 4 bytes for 1337): 39 05 00 00  (1337 = 5 * 256 + 57)
    //
    // Variable-Length Data Section:
    // ImageData Length (int, 4 bytes for 6): 06 00 00 00
    // ImageData Data (6 bytes): DE AD BE EF CA FE
    var expectedBytes = new byte[]
    {
      // --- Header ---
      0, // Null Bitmap

      // --- Fixed-Length Data ---
      57, 5, 0, 0, // ID = 1337

      // --- Variable-Length Data ---
      6, 0, 0, 0, // Length of blobData (6)
      0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE // The blob data itself
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes, actualBytes); // Compare byte arrays
  }

  [Fact]
  public void Serialize_WithBlobFollowedByVarchar_CorrectlyAdvancesOffset()
  {
    // Arrange
    // This schema, with a BLOB followed by another variable-length field,
    // will expose the bug if the offset isn't advanced after writing the blob data.
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("ImageData", new DataTypeInfo(PrimitiveDataType.Blob, 1024), isNullable: false),
        new ColumnDefinition("Caption", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false)
    );

    // Data for the row
    var blobData = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }; // 4 bytes
    string captionValue = "A Photo"; // 7 bytes in UTF-8

    var row = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(200),
        DataValue.CreateBlob(blobData),
        DataValue.CreateString(captionValue)
    );

    // Expected byte representation if the logic is CORRECT
    var expectedBytes = new byte[]
    {
        // --- Header ---
        0, // Null Bitmap

        // --- Fixed-Length Data ---
        200, 0, 0, 0, // ID = 200

        // --- Variable-Length Data ---
        // First var-len field: Blob
        4, 0, 0, 0,   // Length of blobData (4)
        0xDE, 0xAD, 0xBE, 0xEF, // The blob data

        // Second var-len field: Varchar
        7, 0, 0, 0,   // Length of "A Photo" (7)
        // "A Photo" in UTF-8 bytes: 65, 32, 80, 104, 111, 116, 111
        65, 32, 80, 104, 111, 116, 111
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    // With the bug, the 'Caption' length and data would overwrite the 'ImageData' data
    // because the offset was not advanced after writing the blob bytes.
    // The resulting actualBytes array would be shorter and have incorrect content.
    // This assertion will fail until the bug is fixed.
    Assert.Equal(expectedBytes, actualBytes);
  }

  [Fact]
  public void Serialize_WithNullVariableLengthColumn_SetsBitmapAndOmitsData()
  {
    // Arrange
    // 1. Define schema with a nullable variable-length column
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, maxLength: 32), isNullable: true), // Nullable VARCHAR
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false)
    );

    // 2. Create data row where the Name column (index 1) is NULL
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(789),
        DataValue.CreateNull(PrimitiveDataType.Varchar), // Name = NULL
        DataValue.CreateBoolean(true)                   // IsActive = false
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap (1 byte): 3 columns. Col 1 (Name) is NULL, so bit 1 is set.
    // Bitmap = 0b00000010 = 2 (decimal)
    //
    // Fixed-Length Data Section (contiguous):
    // ID (int, 4 bytes for 789): 21, 3, 0, 0
    // IsActive (bool, 1 byte for false): 0
    //
    // Variable-Length Data Section:
    // Name (Varchar): SKIPPED because it is NULL
    var expectedBytes = new byte[]
    {
      // --- Header ---
      2, // Null Bitmap

      // --- Fixed-Length Data Section ---
      21, 3, 0, 0, // ID = 789
      1,           // IsActive = true

      // --- Variable-Length Data Section ---
      // (This section should be empty)
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes, actualBytes);
  }

  [Fact]
  public void Serialize_WithEmptyVariableLengthColumns_WritesZeroLengthPrefix()
  {
    // Arrange
    // 1. Define schema with fixed, varchar, and blob types
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 32), isNullable: false),
        new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Blob, 32), isNullable: false)
    );

    // 2. Create data row with an empty string and an empty byte array
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(2025),
        DataValue.CreateString(""),            // Name is an empty string
        DataValue.CreateBlob(Array.Empty<byte>()) // Data is an empty byte array
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap (1 byte): No nulls -> 0x00
    //
    // Fixed-Length Data Section:
    // ID (int, 4 bytes for 2025): E9 07 00 00
    //
    // Variable-Length Data Section:
    // Name Length (int, 4 bytes for 0): 00 00 00 00
    // Name Data: (0 bytes)
    // Data Length (int, 4 bytes for 0): 00 00 00 00
    // Data Data: (0 bytes)
    var expectedBytes = new byte[]
    {
      // --- Header ---
      0, // Null Bitmap

      // --- Fixed-Length Data Section ---
      233, 7, 0, 0, // ID = 2025 (0x07E9)

      // --- Variable-Length Data Section ---
      0, 0, 0, 0, // Length of Name "" (0)
      // 0 bytes of data for Name
      0, 0, 0, 0  // Length of Data byte[0] (0)
                  // 0 bytes of data for Data
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes, actualBytes);
  }

  [Fact]
  public void Serialize_WithAllNullValues_WritesOnlyNullBitmap()
  {
    // Arrange
    // 1. Define schema with all nullable columns
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: true),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: true),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: true)
    );

    // 2. Create data row where every value is NULL
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateNull(PrimitiveDataType.Int),
        DataValue.CreateNull(PrimitiveDataType.Varchar),
        DataValue.CreateNull(PrimitiveDataType.Boolean)
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap (1 byte): 3 columns, all are NULL.
    // Col 0 (ID) is NULL       -> bit 0 is 1 -> ...001
    // Col 1 (Name) is NULL     -> bit 1 is 1 -> ...010
    // Col 2 (IsActive) is NULL -> bit 2 is 1 -> ...100
    // Bitmap = 0b00000111 = 7 (decimal)
    //
    // Fixed-Length Data Section: Empty, all fixed-size columns are NULL.
    // Variable-Length Data Section: Empty, all variable-size columns are NULL.
    var expectedBytes = new byte[]
    {
      7 // The Null Bitmap (0b00000111)
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes, actualBytes); // Compare byte arrays
  }

  [Fact]
  public void Serialize_WithMultiByteCharacters_UsesCorrectByteCountForLength()
  {
    // Arrange
    // 1. Define schema with a varchar column
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("City", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false)
    );

    // 2. Create data row with a string containing a multi-byte character ('ü')
    string cityValue = "Zürich"; // string.Length is 6
                                 // Get the actual UTF-8 bytes and its length (which will be 7)
    byte[] cityBytes = Encoding.UTF8.GetBytes(cityValue);
    int cityByteLength = cityBytes.Length;
    Assert.NotEqual(cityValue.Length, cityByteLength); // Verify our test case is valid

    var row = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(101),
        DataValue.CreateString(cityValue)
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap (1 byte): No nulls -> 0
    // Fixed-Length Data:
    //   ID (int, 4 bytes for 101): 101, 0, 0, 0
    // Variable-Length Data:
    //   City Length (int, 4 bytes for 7): 7, 0, 0, 0
    //   City Data ("Zürich" in UTF-8, 7 bytes): 90, 195, 188, 114, 105, 99, 104
    var expectedBytes = new byte[]
    {
            // --- Header ---
            0, // Null Bitmap

            // --- Fixed-Length Data Section ---
            101, 0, 0, 0, // ID = 101

            // --- Variable-Length Data Section ---
            7, 0, 0, 0,   // Length of "Zürich" in bytes (7)
            // "Zürich" (Z, ü, r, i, c, h)
            90, 195, 188, 114, 105, 99, 104
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes, actualBytes);
  }

  [Fact]
  public void Serialize_WithAllDataTypesMixedAndNulls_CreatesCorrectRecord()
  {
    // Arrange
    // 1. Define a comprehensive, interleaved schema with all supported types and nullability
    var tableDef = CreateTestTable(
        new ColumnDefinition("ID", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
        new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false),
        new ColumnDefinition("LastLogin", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: true),
        new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false),
        new ColumnDefinition("Notes", new DataTypeInfo(PrimitiveDataType.Varchar, 200), isNullable: false),
        new ColumnDefinition("Balance", new DataTypeInfo(PrimitiveDataType.BigInt), isNullable: false),
        new ColumnDefinition("Avatar", new DataTypeInfo(PrimitiveDataType.Blob, 4096), isNullable: true)
    );

    // 2. Create a data row that exercises all cases
    string nameValue = "Test User"; // 9 bytes
    string notesValue = "";        // 0 bytes
    var row = new ArmDb.DataModel.Record(
        DataValue.CreateInteger(101),
        DataValue.CreateString(nameValue),
        DataValue.CreateNull(PrimitiveDataType.DateTime),  // LastLogin is NULL
        DataValue.CreateBoolean(true),
        DataValue.CreateString(notesValue),                // Notes is an empty string (not null)
        DataValue.CreateBigInteger(5_000_000_000L),
        DataValue.CreateNull(PrimitiveDataType.Blob)       // Avatar is NULL
    );

    // 3. Manually calculate the expected byte array output
    // Null Bitmap (1 byte): 7 columns. Col 2 (LastLogin) and Col 6 (Avatar) are NULL.
    // Bit 2 = 1 -> 0b00000100 (4)
    // Bit 6 = 1 -> 0b01000000 (64)
    // Bitmap = 0b01000100 = 68 (decimal) or 0x44
    //
    // Fixed-Length Data Section (contiguous):
    // ID (int, 4 bytes for 101):         101, 0, 0, 0
    // IsActive (bool, 1 byte for true):  1
    // Balance (long, 8 bytes):           0, 242, 5, 42, 1, 0, 0, 0
    //
    // Variable-Length Data Section (contiguous):
    // Name Length (int, 4 bytes for 9):  9, 0, 0, 0
    // Name Data ("Test User", 9 bytes):  84, 101, 115, 116, 32, 85, 115, 101, 114
    // Notes Length (int, 4 bytes for 0): 0, 0, 0, 0
    // Notes Data: (0 bytes)
    var expectedBytes = new byte[]
    {
      // --- Header ---
      68, // Null Bitmap

      // --- Fixed-Length Data Section ---
      101, 0, 0, 0,                         // ID
      1,                                    // IsActive
      0, 242, 5, 42, 1, 0, 0, 0,          // Balance

      // --- Variable-Length Data Section ---
      9, 0, 0, 0,                           // Length of Name
      84, 101, 115, 116, 32, 85, 115, 101, 114, // "Test User"
      0, 0, 0, 0                            // Length of Notes ""
    };

    // Act
    byte[] actualBytes = RecordSerializer.Serialize(tableDef.Columns, row);

    // Assert
    Assert.NotNull(actualBytes);
    Assert.Equal(expectedBytes, actualBytes);
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