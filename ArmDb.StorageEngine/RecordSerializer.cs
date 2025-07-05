using System.Buffers.Binary;
using System.Data;
using System.Runtime.InteropServices;
using System.Text;
using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using DataRow = ArmDb.DataModel.DataRow;

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
  /// <summary>
  /// Serializes a DataRow into a byte array.
  /// </summary>
  /// <param name="tableDef"></param>
  /// <param name="row"></param>
  /// <returns></returns>
  /// <exception cref="Exception"></exception>
  /// <exception cref="ArgumentNullException"></exception>
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

    // TODO: We could refactor this to a single loop with separate offsets for fixed and variable length data...

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
            break;
          case PrimitiveDataType.Blob:
            byte[] blobValue = (byte[])currentValue!;
            blobValue.CopyTo(dataDestination);
            break;
          default:
            throw new Exception($"Unexpected variable size data type: {columnDef.DataType.PrimitiveType}");
        }
        currentOffset += valueSize;
      }
    }

    return bytes;
  }

  /// <summary>
  /// Deserializes a read-only span of bytes into a DataRow.
  /// </summary>
  /// <param name="tableDef"></param>
  /// <param name="recordData"></param>
  /// <returns></returns>
  /// <exception cref="Exception"></exception>
  public static DataRow Deserialize(TableDefinition tableDef, ReadOnlySpan<byte> recordData)
  {
    var columnCount = tableDef.Columns.Count();
    var rowValues = new DataValue[columnCount];

    // Determine the size of the null bitmap in bytes based on how many columns we have. We need
    // 1 byte for every 8 columns, which gives us 1 bit per column.
    var nullBitmapSize = (columnCount + 7) / 8;
    // Grab the null bitmap...
    var nullBitmap = recordData.Slice(0, nullBitmapSize);

    // We'll use 2 pointers, one for fixed size data and one for variable size data...
    var currentFixedSizedDataOffset = nullBitmapSize;
    var currentVariableSizedDataOffset = CalculateVariableLengthDataOffset(tableDef, nullBitmap);

    for (int i = 0; i < columnCount; i++)
    {
      var columnDef = tableDef.Columns[i];
      if (IsColumnValueNull(columnDef, nullBitmap, i))
      {
        // Create the null value for the column and continue...
        rowValues[i] = DataValue.CreateNull(columnDef.DataType.PrimitiveType);
        // There is no data written if the value is null, so we don't need to update either offset here...
        continue;
      }

      if (columnDef.DataType.IsFixedSize)
      {
        // Get the size of the column from the schema definition...
        var dataSize = columnDef.DataType.GetFixedSize();
        var dataValue = recordData.Slice(currentFixedSizedDataOffset, dataSize);
        // Read the data in...
        switch (columnDef.DataType.PrimitiveType)
        {
          case PrimitiveDataType.BigInt:
            long longValue = BinaryUtilities.ReadInt64LittleEndian(dataValue);
            rowValues[i] = DataValue.CreateBigInteger(longValue);
            break;
          case PrimitiveDataType.Boolean:
            bool boolValue = BinaryUtilities.ToBoolean(dataValue);
            rowValues[i] = DataValue.CreateBoolean(boolValue);
            break;
          case PrimitiveDataType.DateTime:
            long dateTicks = BinaryUtilities.ReadInt64LittleEndian(dataValue);
            DateTime dateTimeValue = DateTime.FromBinary(dateTicks);
            rowValues[i] = DataValue.CreateDateTime(dateTimeValue);
            break;
          case PrimitiveDataType.Decimal:
            decimal decimalValue = BinaryUtilities.ConvertSpanToDecimal(dataValue);
            rowValues[i] = DataValue.CreateDecimal(decimalValue);
            break;
          case PrimitiveDataType.Float:
            double doubleValue = BinaryPrimitives.ReadDoubleLittleEndian(dataValue); ;
            rowValues[i] = DataValue.CreateFloat(doubleValue);
            break;
          case PrimitiveDataType.Int:
            int intValue = BinaryUtilities.ReadInt32LittleEndian(dataValue);
            rowValues[i] = DataValue.CreateInteger(intValue);
            break;
          default:
            throw new Exception($"Unexpected fixed size data type: {columnDef.DataType.PrimitiveType}");
        }

        currentFixedSizedDataOffset += dataSize;
      }
      else // This is a variable length column
      {
        // Read the length of the variable sized column from the beginning of the variable length data offset...
        var dataSizeData = recordData.Slice(currentVariableSizedDataOffset, sizeof(int));
        int dataSize = BinaryUtilities.ReadInt32LittleEndian(dataSizeData);
        // Advance our variable length data offset past the length of this value...
        currentVariableSizedDataOffset += sizeof(int);
        var dataValue = recordData.Slice(currentVariableSizedDataOffset, dataSize);

        switch (columnDef.DataType.PrimitiveType)
        {
          case PrimitiveDataType.Varchar:
            var stringValue = Encoding.UTF8.GetString(dataValue);
            if (stringValue == null)
              throw new NullReferenceException("Expected a non-null string, but value is null!");
            rowValues[i] = DataValue.CreateString(stringValue);
            break;
          case PrimitiveDataType.Blob:
            rowValues[i] = DataValue.CreateBlob(dataValue.ToArray());
            break;
        }

        // Advance the variable length data offset past the actual data...
        currentVariableSizedDataOffset += dataSize;
      }
    }

    return new DataRow(rowValues);
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

  private static int CalculateVariableLengthDataOffset(TableDefinition tableDef, ReadOnlySpan<byte> nullBitmap)
  {
    var columnCount = tableDef.Columns.Count();

    // Determine the size of the null bitmap in bytes based on how many columns we have. We need
    // 1 byte for every 8 columns, which gives us 1 bit per column.
    var nullBitmapSize = (columnCount + 7) / 8;

    var offset = nullBitmapSize;
    // Go through each fixed column and determine the size...
    for (int i = 0; i < columnCount; i++)
    {
      // Null values will not be written, so only calculate space necessary for non-null values
      if (IsColumnValueNull(tableDef.Columns[i], nullBitmap, i))
        continue;

      var columnDef = tableDef.Columns[i];
      if (columnDef.DataType.IsFixedSize)
      {
        offset += columnDef.DataType.GetFixedSize();
      }
    }

    return offset;
  }

  private static bool IsColumnValueNull(ColumnDefinition columnDef, ReadOnlySpan<byte> nullBitmap, int index)
  {
    if (columnDef.IsNullable)
    {
      // Check the null bitmap to see if the value is null
      // Determine which byte to index...
      var nullBitmapByteIndex = index / 8;
      var nullBitmapByte = nullBitmap[nullBitmapByteIndex];
      // What bit are we interested in?
      var bitInByte = index % 8;
      // Is it set?
      var isNull = (nullBitmapByte & (1 << bitInByte)) != 0;

      return isNull;
    }

    return false;
  }
}