using ArmDb.SchemaDefinition;

namespace ArmDb.DataModel;

public abstract class KeyComparer : IComparer<Key>
{
  public int Compare(Key? x, Key? y)
  {
    // TODO: The values should be the same length, otherwise we have an error condition...

    // There probably aren't valid use cases for passing in null Key objects, though the values in
    // the Key objects could be null. But the Key object and its collection of values should never
    // be null, or there is some other problem. While the semantics of the IComparer interface
    // suggests we should do a comparison for null Key values, the better option is to throw here
    // to let a caller know that we wer not expecting this and either a use case does exist
    // which was unknown, or there is a bug somewhere in which a null Key object is getting passed
    // in.
    ArgumentNullException.ThrowIfNull(x, "Key x is null, which is unexpected!");
    ArgumentNullException.ThrowIfNull(y, "Key y is null, which is unexpected!");

    // Loop through each data value in the key and perform the comparison...
    for (var i = 0; i < x!.Values.Count; i++)
    {
      // Sanity check to make sure the types match for each Key column value comparison.
      if (x.Values[i].DataType != y.Values[i].DataType)
        throw new InvalidOperationException($"Found mismatched data types comparing keys at index {i}! Cannot compare {x.Values[i].DataType} with {y.Values[i].DataType}");

      var valueX = x!.Values[i];
      var valueY = y!.Values[i];

      if (valueX.IsNull && valueY.IsNull) return 0; // Two nulls are equal for sorting
      if (valueX.IsNull) return -1; // Null is less than any non-null value
      if (valueY.IsNull) return 1;  // Any non-null value is greater than a null value

      // Return the comaparison for the first pair of values that are not equivalent.
      var result = Compare(valueX, valueY);

      if (result != 0)
        return result;
    }

    return 0;
  }

  private int Compare(DataValue x, DataValue y)
  {
    // We've already checked the DataValues for null, so we can assume the underlying values are not null here.
    switch (x.DataType)
    {
      // Numeric types...
      case PrimitiveDataType.Int:
        int intX = (int)x!.Value!;
        int intY = (int)y!.Value!;
        return intX.CompareTo(intY);

      case PrimitiveDataType.BigInt:
        long longX = (long)x!.Value!;
        long longY = (long)y!.Value!;
        return longX.CompareTo(longY);

      case PrimitiveDataType.Decimal:
        decimal decimalX = (decimal)x!.Value!;
        decimal decimalY = (decimal)y!.Value!;
        return decimalX.CompareTo(decimalY);

      case PrimitiveDataType.Float:
        double doubleX = (double)x!.Value!;
        double doubleY = (double)y!.Value!;
        return doubleX.CompareTo(doubleY);

      // Non-numeric types
      case PrimitiveDataType.Varchar:
        string varcharX = (string)x!.Value!;
        string varcharY = (string)y!.Value!;
        return string.CompareOrdinal(varcharX, varcharY);

      case PrimitiveDataType.Boolean:
        bool boolX = (bool)x!.Value!;
        bool boolY = (bool)y!.Value!;
        return boolX.CompareTo(boolY);

      case PrimitiveDataType.DateTime:
        DateTime timeX = (DateTime)x!.Value!;
        DateTime timeY = (DateTime)y!.Value!;
        return timeX.CompareTo(timeY);

      case PrimitiveDataType.Blob:
        ReadOnlySpan<byte> blobX = (byte[])x!.Value!;
        ReadOnlySpan<byte> blobY = (byte[])y!.Value!;
        return blobX.SequenceCompareTo(blobY);
    }

    throw new NotImplementedException("Support for type has not been implemented.");
  }
}