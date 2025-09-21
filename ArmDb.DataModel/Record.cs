namespace ArmDb.DataModel;

/// <summary>
/// Represents a single record of data as an immutable, ordered sequence of DataValue objects.
/// </summary>
public sealed class Record : IEquatable<Record>
{
  // Internal storage - readonly array ensures the array reference doesn't change.
  // Immutability of the row's *content* relies on copying input in constructors
  // and DataValue itself being immutable.
  private readonly DataValue[] _values;

  /// <summary>
  /// Gets the ordered, read-only list of DataValue objects representing the data in this row.
  /// </summary>
  public IReadOnlyList<DataValue> Values => _values;

  /// <summary>
  /// Gets the number of values (columns) in the row. Also known as Arity.
  /// </summary>
  public int Arity => _values.Length;

  /// <summary>
  /// Gets the DataValue at the specified column index.
  /// </summary>
  /// <param name="index">The zero-based index of the column.</param>
  /// <returns>The DataValue at the specified index.</returns>
  /// <exception cref="IndexOutOfRangeException">Thrown if index is out of bounds.</exception>
  public DataValue this[int index] => _values[index];

  /// <summary>
  /// Initializes a new instance of the <see cref="Record"/> class from a sequence of DataValue objects.
  /// Creates a copy of the input sequence to ensure immutability.
  /// </summary>
  /// <param name="values">The sequence of DataValue objects for the row. Cannot be null or contain null elements.</param>
  /// <exception cref="ArgumentNullException">Thrown if values is null.</exception>
  /// <exception cref="ArgumentException">Thrown if the values sequence contains any null DataValue elements.</exception>
  public Record(IEnumerable<DataValue> values)
  {
    ArgumentNullException.ThrowIfNull(values);

    // Create a copy and check for null elements within the sequence during copy
    // Using ToArray implicitly iterates and would throw NullReferenceException if source itself yields null,
    // but let's add an explicit check for clarity and better exception.
    _values = values.ToArray();
    if (_values.Any(v => v is null))
    {
      // This shouldn't happen if DataValue instances are always created via factories,
      // but provides defense against incorrect usage.
      throw new ArgumentException("Input sequence cannot contain null DataValue elements.", nameof(values));
    }
  }

  /// <summary>
  /// Initializes a new instance of the <see cref="Record"/> class from a parameter array of DataValue objects.
  /// Creates a clone of the input array to ensure immutability if the caller retains the original array.
  /// </summary>
  /// <param name="values">The array of DataValue objects for the row. Cannot be null or contain null elements.</param>
  /// <exception cref="ArgumentNullException">Thrown if values is null.</exception>
  /// <exception cref="ArgumentException">Thrown if the values array contains any null DataValue elements.</exception>
  public Record(params DataValue[] values)
  {
    ArgumentNullException.ThrowIfNull(values);

    if (values.Any(v => v is null))
    {
      throw new ArgumentException("Input array cannot contain null DataValue elements.", nameof(values));
    }
    // Clone the array to prevent external modification if the caller holds onto the original 'values'
    _values = (DataValue[])values.Clone();
  }

  // --- Overrides ---

  /// <summary>
  /// Returns a string representation of the row, typically showing contained values.
  /// Example: "(1, 'Active', NULL)"
  /// </summary>
  public override string ToString()
  {
    // Use the ToString() implementation of the contained DataValue objects
    return $"({string.Join(", ", _values.Select(v => v.ToString()))})";
  }

  /// <summary>
  /// Determines whether the specified object is equal to the current DataRow object.
  /// Equality is based on the sequential equality of the contained DataValue objects.
  /// </summary>
  public override bool Equals(object? obj)
  {
    return Equals(obj as Record);
  }

  /// <summary>
  /// Indicates whether the current DataRow is equal to another DataRow object.
  /// Equality is based on the sequential equality of the contained DataValue objects.
  /// </summary>
  public bool Equals(Record? other)
  {
    if (other is null) return false;
    if (ReferenceEquals(this, other)) return true;

    // Check if arity is the same first for quick exit
    if (this.Arity != other.Arity) return false;

    // Use SequenceEqual, which uses the Equals method of DataValue for element comparison
    return _values.SequenceEqual(other._values);
  }

  /// <summary>
  /// Serves as the default hash function. Calculates hash code based on the sequence of contained DataValues.
  /// </summary>
  public override int GetHashCode()
  {
    // Combine hash codes of elements respecting order
    var hc = new HashCode();
    foreach (var value in _values)
    {
      // Uses the GetHashCode implementation of DataValue
      hc.Add(value);
    }
    return hc.ToHashCode();
  }

  /// <summary>
  /// Determines whether two specified DataRow objects have the same value sequence.
  /// </summary>
  public static bool operator ==(Record? left, Record? right)
  {
    if (ReferenceEquals(left, right)) return true; // Same instance or both null
    if (left is null || right is null) return false; // One is null, the other isn't
    return left.Equals(right);
  }

  /// <summary>
  /// Determines whether two specified DataRow objects have different value sequences.
  /// </summary>
  public static bool operator !=(Record? left, Record? right)
  {
    return !(left == right);
  }
}