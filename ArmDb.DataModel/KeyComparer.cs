using ArmDb.SchemaDefinition;

namespace ArmDb.DataModel;

public abstract class KeyComparer : IComparer<Key>
{
  private readonly IReadOnlyList<ColumnDefinition> _keyColumns;

  public KeyComparer(IReadOnlyList<ColumnDefinition> keyColumns)
  {
    _keyColumns = keyColumns;
  }

  public int Compare(Key? x, Key? y)
  {
    // TODO: The values should be the same length, otherwise we have an error condition...

    // Loop through each data value in the key and perform the comparison...
    for (var i = 0; i < x!.Values.Count; i++)
    {
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
    switch (x.DataType)
    {
      case PrimitiveDataType.Int:
        // Unbox the int values...
        // We've already checked for null, so we can assume the underlying value is not null here.
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

    }

    throw new NotImplementedException("Support for type has not been implemented.");
  }
}