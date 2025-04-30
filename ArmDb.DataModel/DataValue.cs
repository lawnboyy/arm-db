using System.Diagnostics.CodeAnalysis;
using ArmDb.SchemaDefinition;

namespace ArmDb.DataModel;

/// <summary>
/// Represents a single data value within a DataRow, capable of holding different types
/// including SQL NULL. This implementation uses object? for storage, leading to
/// boxing for value types. Instances are immutable.
/// </summary>
public sealed class DataValue : IEquatable<DataValue>
{
  /// <summary>
  /// Gets the actual data value.
  /// This will be null if the represented value is SQL NULL.
  /// Value types (int, bool, decimal, etc.) will be boxed when stored here.
  /// </summary>
  public object? Value { get; private init; }

  /// <summary>
  /// Gets the intended SQL primitive data type for this value.
  /// This is important for interpreting the value, especially for NULLs.
  /// </summary>
  public PrimitiveDataType DataType { get; private init; }

  /// <summary>
  /// Gets a value indicating whether this instance represents SQL NULL.
  /// </summary>
  public bool IsNull => Value == null;

  /// <summary>
  /// Private constructor to enforce creation via static factory methods.
  /// </summary>
  private DataValue(PrimitiveDataType type, object? value)
  {
    // Basic check: if we know the type must have a value, ensure value isn't null unless IsNull is true?
    // This logic is implicitly handled by IsNull check and factory methods.
    DataType = type;
    Value = value; // Assigns the (potentially boxed) value or null
  }

  /// <summary>Creates a DataValue for a 64-bit integer.</summary>
  public static DataValue CreateInteger(long value) => new DataValue(PrimitiveDataType.Int, value);

  /// <summary>Creates a DataValue for a string.</summary>
  /// <exception cref="ArgumentNullException">Thrown if value is null.</exception>
  public static DataValue CreateString(string value)
  {
    ArgumentNullException.ThrowIfNull(value);
    return new DataValue(PrimitiveDataType.Varchar, value); // Defaulting to Varchar, could add Char later
  }

  /// <summary>Creates a DataValue for a boolean.</summary>
  public static DataValue CreateBoolean(bool value) => new DataValue(PrimitiveDataType.Boolean, value);

  /// <summary>Creates a DataValue for a decimal.</summary>
  public static DataValue CreateDecimal(decimal value) => new DataValue(PrimitiveDataType.Decimal, value);

  /// <summary>Creates a DataValue for a DateTime.</summary>
  public static DataValue CreateDateTime(DateTime value) => new DataValue(PrimitiveDataType.DateTime, value);

  /// <summary>Creates a DataValue for a double-precision float.</summary>
  public static DataValue CreateFloat(double value) => new DataValue(PrimitiveDataType.Float, value);

  /// <summary>Creates a DataValue for a byte array (blob).</summary>
  /// <exception cref="ArgumentNullException">Thrown if value is null.</exception>
  public static DataValue CreateBlob(byte[] value)
  {
    ArgumentNullException.ThrowIfNull(value);
    return new DataValue(PrimitiveDataType.Blob, value);
  }

  /// <summary>
  /// Creates a DataValue representing SQL NULL for a specific data type.
  /// </summary>
  /// <param name="type">The SQL data type the NULL represents. Cannot be Unknown.</param>
  /// <exception cref="ArgumentException">Thrown if type is Unknown.</exception>
  public static DataValue CreateNull(PrimitiveDataType type)
  {
    if (type == PrimitiveDataType.Unknown)
      throw new ArgumentException("Cannot create a NULL value with an Unknown data type.", nameof(type));
    return new DataValue(type, null); // Value is explicitly null
  }

  /// <summary>
  /// Gets the value cast to the specified type T.
  /// </summary>
  /// <typeparam name="T">The target type.</typeparam>
  /// <returns>The value cast to T.</returns>
  /// <exception cref="InvalidOperationException">Thrown if the current value represents SQL NULL.</exception>
  /// <exception cref="InvalidCastException">Thrown if the stored value cannot be cast to type T, or if the requested type is incompatible with the stored DataType.</exception>
  [return: NotNull] // Indicates return value is not null if method succeeds
  public T GetAs<T>()
  {
    if (IsNull)
    {
      throw new InvalidOperationException("Cannot get value; the DataValue represents SQL NULL.");
    }

    // Perform the cast. This will throw InvalidCastException if incompatible.
    // Use null-forgiving operator (!) because we've checked IsNull.
    try
    {
      // Direct cast might work for reference types or exact value type matches.
      // For potentially different but compatible numeric types (e.g., getting int from long),
      // Convert.ChangeType might be more flexible, but direct cast is stricter.
      // Let's stick with direct cast for now.
      return (T)Value!;
    }
    catch (InvalidCastException ex)
    {
      // Provide a more informative exception message
      throw new InvalidCastException($"Cannot cast DataValue (Type: {DataType}, Actual Stored Type: {Value?.GetType().Name ?? "null"}) to {typeof(T).Name}.", ex);
    }
    catch (NullReferenceException ex) // Should not happen due to IsNull check, but belt-and-suspenders
    {
      throw new InvalidCastException($"Attempted to cast a null value unexpectedly. DataValue Type: {DataType}", ex);
    }
  }

  /// <summary>
  /// Returns a string representation of the value (or "NULL").
  /// </summary>
  public override string ToString()
  {
    if (IsNull) return "NULL";

    // Handle specific formatting if needed (e.g., for dates, decimals)
    switch (DataType)
    {
      // case PrimitiveDataType.DateTime when Value is DateTime dt:
      //    return dt.ToString("o"); // ISO 8601 example
      // case PrimitiveDataType.Decimal when Value is decimal dec:
      //    return dec.ToString("G"); // General format example
      default:
        // Value should not be null here if IsNull is false
        return Value?.ToString() ?? string.Empty; // Fallback, should ideally have value
    }
  }

  /// <summary>
  /// Determines whether the specified object is equal to the current object.
  /// Equality requires both DataType and Value to be equal (using object.Equals for Value).
  /// Note: Two NULL DataValues of the same DataType ARE considered equal by this implementation.
  /// </summary>
  public override bool Equals(object? obj)
  {
    return Equals(obj as DataValue);
  }

  /// <summary>
  /// Indicates whether the current object is equal to another object of the same type.
  /// Equality requires both DataType and Value to be equal (using object.Equals for Value).
  /// Note: Two NULL DataValues of the same DataType ARE considered equal by this implementation.
  /// </summary>
  public bool Equals(DataValue? other)
  {
    if (other is null) return false;
    if (ReferenceEquals(this, other)) return true;

    // Data types must match for equality
    if (DataType != other.DataType) return false;

    // If types match, check values. Handle Blob specially.
    if (DataType == PrimitiveDataType.Blob)
    {
      // Handle cases where one or both values might be null *or* byte[]
      if (Value is byte[] thisBlob && other.Value is byte[] otherBlob)
      {
        // Both are non-null byte arrays: compare content using Span sequence equality (efficient)
        return ((ReadOnlySpan<byte>)thisBlob).SequenceEqual((ReadOnlySpan<byte>)otherBlob);
      }
      else
      {
        // If only one is byte[] or both are null, standard object.Equals handles it correctly:
        // - byte[] vs null -> false
        // - null vs byte[] -> false
        // - null vs null -> true
        return Equals(Value, other.Value);
      }
    }
    else
    {
      // For all other data types, standard object.Equals is sufficient
      // (handles value types, strings correctly, reference types by reference if needed)
      return Equals(Value, other.Value);
    }
  }

  /// <summary>
  /// Serves as the default hash function. Combines DataType and Value hash codes.
  /// </summary>
  public override int GetHashCode()
  {
    // Must be consistent with Equals: if Equals is true, GetHashCode must be the same.
    if (DataType == PrimitiveDataType.Blob && Value is byte[] blob)
    {
      // Calculate a content-based hash code for the byte array
      var hc = new HashCode();
      hc.Add(DataType); // Include type in hash
      hc.AddBytes(blob.AsSpan()); // Efficiently add byte content to hash
      return hc.ToHashCode();
    }
    else
    {
      // Default implementation works for other types and nulls
      // HashCode.Combine handles null Value appropriately
      return HashCode.Combine(DataType, Value);
    }
  }

  /// <summary>
  /// Determines whether two specified DataValue objects have the same value.
  /// </summary>
  public static bool operator ==(DataValue? left, DataValue? right)
  {
    // Handles cases where one or both are null System.Object references
    if (ReferenceEquals(left, right)) return true; // Same instance or both null
    if (left is null || right is null) return false; // One is null, the other isn't
    return left.Equals(right); // Use instance Equals method
  }

  /// <summary>
  /// Determines whether two specified DataValue objects have different values.
  /// </summary>
  public static bool operator !=(DataValue? left, DataValue? right)
  {
    return !(left == right);
  }
}