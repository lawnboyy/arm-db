using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using ArmDb.DataModel;
using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

/// <summary>
/// Serializer for Data Rows for reading and writing to slotted page cells. The format of the data
/// record is | Header | Fixed Length Data | Variable Length Data |
/// 
/// Example:
/// 
/// [HEADER]---[FIXED-LENGTH DATA]------------[VARIABLE-LENGTH DATA]--------------------------------------------
/// +----------+-------------------+----------+------------------------+---------------------------------------+
/// | Null     | ID(4 bytes)       | IsActive | Name Length(4 bytes)   | Name Data('Alice' as UTF-8, 5 bytes)  |
/// | Bitmap   | (not null)        | (1 byte) | (not null)             |                                       |
/// | (1 byte) |                   |          |                        |                                       |
/// +----------+-------------------+----------+------------------------+---------------------------------------+
/// | 00001000 | 7B 00 00 00       | 01       | 05 00 00 00            | 41 6C 69 63 65                        |
/// +----------+-------------------+----------+------------------------+---------------------------------------+
/// ^          ^                   ^          ^                        ^
/// |          |                   |          |                        |
/// |          |                   |          +---- Length of 'Alice'  |
/// |          |                   +---- Value for IsActive(true)      |
/// |          +---- Value for ID(123)                                 |
/// +---- 4th column(Bio) is NULL, so 4th bit is 1
///
/// </summary>
internal static class RecordSerializer
{
  public static byte[] Serialize(TableDefinition tableDef, DataRow row)
  {
    // Initialize byte array to hold the serialized row...
    Dictionary<string, int> variableLengthColumnSizeLookup;
    var totalRecordSize = CalculateSerializedRecordSize(tableDef, row, out var nullBitmapSize, out variableLengthColumnSizeLookup);
    byte[] bytes = new byte[totalRecordSize];
    var recordSpan = bytes.AsSpan();

    // We'll do 2 serialization passes through the schema column, first for fixed sized columns to keep
    // them in a contiguous block, then a second pass for variable length columns which will reside in
    // a second contiguous block.

    // Row values are stored in the same order as the schema column definition order...
    var columns = tableDef.Columns;
    var columnCount = columns.Count();

    // Our starting index is be immediately after the null bitmap header.
    var currentOffset = nullBitmapSize;
    // The first pass is for fixed size only...
    for (int i = 0; i < columnCount; i++)
    {
      var columnDef = columns[i];
      if (columnDef.DataType.IsFixedSize)
      {
        var valueSize = columnDef.DataType.GetFixedSize();
        // We will not write any data if the value is null.
        // If the value is null set the flag in the bitmap and continue...
        if (row[i].IsNull)
        {
          // Set the corresponding bit in the null bitmap...
          // First determine which byte we need to use...
          var nullBitmapByteIndex = i / 8;
          // Now determine which bit to set within the byte...
          var bitInByte = i % 8;
          // Set the bit in the null bitmap that corresponds to this column.
          bytes[nullBitmapByteIndex] |= (byte)(1 << bitInByte);
          continue;
        }

        // Else the data value is not null, so we write it out to the byte array...
        var destination = recordSpan.Slice(currentOffset, valueSize);
        var currentValue = row[i].Value;
        switch (columnDef.DataType.PrimitiveType)
        {
          case PrimitiveDataType.BigInt:
            long longValue = Convert.ToInt64(currentValue);
            BinaryUtilities.WriteInt64LittleEndian(destination, longValue);
            break;
          case PrimitiveDataType.Boolean:
            bool boolValue = Convert.ToBoolean(currentValue);
            destination[0] = boolValue ? (byte)1 : (byte)0;
            break;
          case PrimitiveDataType.DateTime:
            DateTime dateTimeValue = Convert.ToDateTime(currentValue);
            BinaryUtilities.WriteInt64LittleEndian(destination, dateTimeValue.ToBinary());
            break;
          case PrimitiveDataType.Decimal:
            decimal decimalValue = Convert.ToDecimal(currentValue);
            MemoryMarshal.Write(destination, in decimalValue);
            break;
          case PrimitiveDataType.Float:
            double doubleValue = Convert.ToDouble(currentValue);
            // TODO: Add implementation for this to BinaryUtilities class.
            BinaryPrimitives.WriteDoubleLittleEndian(destination, doubleValue);
            break;
          case PrimitiveDataType.Int:
            int intValue = Convert.ToInt32(currentValue);
            BinaryUtilities.WriteInt32LittleEndian(destination, intValue);
            break;
          default:
            throw new Exception($"Unexpected fixed size data type: {columnDef.DataType.PrimitiveType}");
        }
        currentOffset += valueSize;
      }
    }

    // Second pass for variable length records...
    for (int i = 0; i < columnCount; i++)
    {
      var columnDef = columns[i];

      if (!columnDef.DataType.IsFixedSize)
      {
        // We will not write any data if the value is null.
        // If the value is null set the flag in the bitmap and continue...
        if (row[i].IsNull)
        {
          // Set the corresponding bit in the null bitmap...
          // First determine which byte we need to use...
          var nullBitmapByteIndex = i / 8;
          // Now determine which bit to set within the byte...
          var bitInByte = i % 8;
          // Set the bit in the null bitmap that corresponds to this column.
          bytes[nullBitmapByteIndex] |= (byte)(1 << bitInByte);
          continue;
        }

        // Else the data value is not null, so we write it out to the byte array...
        // We can pull the size of this variable size column value from our lookup...
        var valueSize = variableLengthColumnSizeLookup[columnDef.Name];
        // For any variable size column value, we'll need to write the length first...
        var dataLengthDestination = recordSpan.Slice(currentOffset, sizeof(int));
        BinaryUtilities.WriteInt32LittleEndian(dataLengthDestination, valueSize);

        // Move our offset to the start of where we want to write the data...
        currentOffset += sizeof(int);

        var dataDestination = recordSpan.Slice(currentOffset, valueSize);
        var currentValue = row[i].Value;
        switch (columnDef.DataType.PrimitiveType)
        {
          case PrimitiveDataType.Varchar:
            // TODO: This conversion has already been done once... consider capturing it in the variable size data lookup for reuse.
            string? stringValue = Convert.ToString(currentValue);
            byte[] varcharBytes = Encoding.UTF8.GetBytes(stringValue!) ?? throw new ArgumentNullException("Could not convert string value for serialization!");
            varcharBytes.CopyTo(dataDestination);
            currentOffset += valueSize;
            break;
          case PrimitiveDataType.Blob:
            byte[] blobValue = (byte[])currentValue!;
            blobValue.CopyTo(dataDestination);
            break;
          default:
            throw new Exception($"Unexpected variable size data type: {columnDef.DataType.PrimitiveType}");
        }
      }
    }

    return bytes;
  }

  public static DataRow Deserialize(TableDefinition tableDef, ReadOnlySpan<byte> recordData)
  {
    throw new NotImplementedException();
  }

  private static int CalculateSerializedRecordSize(TableDefinition tableDef, DataRow row, out int nullBitmapSize, out Dictionary<string, int> variableDataSizeLookup)
  {
    var columnCount = tableDef.Columns.Count();

    // Determine the size of the null bitmap in bytes based on how many columns we have. We need
    // 1 byte for every 8 columns, which gives us 1 bit per column.
    nullBitmapSize = (columnCount + 7) / 8;

    // If we have any variable length columns, we'll capture the sizes now in this first pass...
    variableDataSizeLookup = new Dictionary<string, int>();

    var totalRecordSize = nullBitmapSize;
    // Go through each column and determine the size...
    for (int i = 0; i < columnCount; i++)
    {
      var rowValue = row[i];
      // Null values will not be written, so only calculate space necessary for non-null values
      if (rowValue.IsNull)
        continue;

      var columnDef = tableDef.Columns[i];
      if (columnDef.DataType.IsFixedSize)
      {
        totalRecordSize += columnDef.DataType.GetFixedSize();
      }
      else // Handle variable length data types
      {
        totalRecordSize += sizeof(int); // to store the length of the variable length data

        switch (columnDef.DataType.PrimitiveType)
        {
          case PrimitiveDataType.Varchar:
            // Convert the object to string and get its UTF-8 byte count
            string? stringValue = Convert.ToString(rowValue.Value);
            var varcharSize = Encoding.UTF8.GetByteCount(stringValue!);
            totalRecordSize += varcharSize;
            // Add this size to our lookup for the caller...
            variableDataSizeLookup.Add(columnDef.Name, varcharSize);
            break;

          case PrimitiveDataType.Blob:
            // Cast the object to byte[] and get its length
            byte[] blobValue = (byte[])rowValue.Value!;
            totalRecordSize += blobValue.Length;
            // Add this size to our lookup for the caller...
            variableDataSizeLookup.Add(columnDef.Name, blobValue.Length);
            break;

          default:
            // This case shouldn't be hit if IsFixedSize is false, but good for safety
            throw new NotSupportedException($"Size calculation for variable-size type {columnDef.DataType.PrimitiveType} is not supported.");
        }
      }
    }

    return totalRecordSize;
  }
}