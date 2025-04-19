namespace ArmDb.Core.SchemaDefinition;

/// <summary>
/// Encapsulates the complete information about a column's data type,
/// including the primitive type and any necessary parameters like length, precision, or scale.
/// Instances of this class are immutable after creation.
/// </summary>
public sealed class DataTypeInfo : IEquatable<DataTypeInfo> // Sealed + IEquatable is good practice for value-like objects
{
  /// <summary>
  /// Gets the underlying primitive data type.
  /// </summary>
  public PrimitiveDataType PrimitiveType { get; init; }

  /// <summary>
  /// Gets the maximum length for variable-length types like Varchar or Blob.
  /// Null for types where length is not applicable or fixed.
  /// </summary>
  public int? MaxLength { get; init; }

  /// <summary>
  /// Gets the total number of digits allowed for numeric types like Decimal.
  /// Null for non-numeric types or where precision is not applicable.
  /// </summary>
  public int? Precision { get; init; }

  /// <summary>
  /// Gets the number of digits to the right of the decimal point for numeric types like Decimal.
  /// Must be less than or equal to Precision. Null otherwise.
  /// </summary>
  public int? Scale { get; init; }

  /// <summary>
  /// Initializes a new instance of the <see cref="DataTypeInfo"/> class with validation.
  /// Use static factory methods like CreateVarchar, CreateDecimal etc. for potentially cleaner creation.
  /// </summary>
  /// <param name="primitiveType">The base primitive type.</param>
  /// <param name="maxLength">Required and must be positive for Varchar/Blob. Must be null otherwise.</param>
  /// <param name="precision">Optional for Decimal (must be positive). Must be null otherwise.</param>
  /// <param name="scale">Optional for Decimal (must be non-negative and <= precision). Must be null otherwise. Defaults to 0 if precision is specified but scale is not.</param>
  /// <exception cref="ArgumentOutOfRangeException">Thrown if parameters are invalid for the given primitive type (e.g., negative length, scale > precision).</exception>
  /// <exception cref="ArgumentException">Thrown if required parameters are missing, extraneous parameters are provided, or the primitive type is Unknown.</exception>
  public DataTypeInfo(PrimitiveDataType primitiveType, int? maxLength = null, int? precision = null, int? scale = null)
  {
    // Assign PrimitiveType first for use in validation messages if needed
    PrimitiveType = primitiveType;

    // Validate parameters based on the primitive type
    switch (primitiveType)
    {
      case PrimitiveDataType.Varchar:
      case PrimitiveDataType.Blob: // Assuming Blob also uses MaxLength here
        if (maxLength == null)
          throw new ArgumentException($"{primitiveType} requires a MaxLength.", nameof(maxLength));
        if (maxLength <= 0)
          throw new ArgumentOutOfRangeException(nameof(maxLength), $"{primitiveType} MaxLength must be positive.");
        if (precision != null || scale != null)
          throw new ArgumentException($"{primitiveType} cannot have Precision or Scale.", $"{nameof(precision)}/{nameof(scale)}");
        // Assign valid parameters
        MaxLength = maxLength;
        Precision = null;
        Scale = null;
        break;

      case PrimitiveDataType.Decimal:
        if (maxLength != null)
          throw new ArgumentException("Decimal cannot have MaxLength.", nameof(maxLength));

        int? finalPrecision = precision;
        int? finalScale = scale;

        if (finalPrecision != null) // Precision is specified
        {
          if (finalPrecision <= 0)
            throw new ArgumentOutOfRangeException(nameof(precision), "Decimal Precision must be positive.");
          if (finalScale != null) // Scale is also specified
          {
            if (finalScale < 0)
              throw new ArgumentOutOfRangeException(nameof(scale), "Decimal Scale cannot be negative.");
            if (finalScale > finalPrecision)
              throw new ArgumentOutOfRangeException(nameof(scale), "Decimal Scale cannot be greater than Precision.");
          }
          else
          {
            // Default scale to 0 if precision is specified but scale is not
            finalScale = 0;
          }
        }
        else if (finalScale != null) // Scale specified WITHOUT precision
        {
          // Standard SQL generally requires precision if scale is set. Disallow this.
          throw new ArgumentException("Decimal Scale cannot be specified without Precision.", nameof(scale));
        }
        // Assign potentially updated parameters
        MaxLength = null;
        Precision = finalPrecision;
        Scale = finalScale;
        break;

      case PrimitiveDataType.Integer:
      case PrimitiveDataType.Boolean:
      case PrimitiveDataType.DateTime:
      case PrimitiveDataType.Float:
        // These types typically don't use these parameters in standard SQL definitions
        if (maxLength != null || precision != null || scale != null)
          throw new ArgumentException($"{primitiveType} does not support MaxLength, Precision, or Scale.", $"{nameof(maxLength)}/{nameof(precision)}/{nameof(scale)}");
        // Assign null to all parameters
        MaxLength = null;
        Precision = null;
        Scale = null;
        break;

      case PrimitiveDataType.Unknown:
      default:
        // Includes PrimitiveDataType.Unknown or any future values not handled above
        throw new ArgumentException($"Unsupported or unknown primitive data type specified: {primitiveType}", nameof(primitiveType));
    }
  }

  /// <summary>
  /// Returns a string representation of the data type information (e.g., "VARCHAR(100)", "DECIMAL(10, 2)", "INTEGER").
  /// </summary>
  public override string ToString()
  {
    return PrimitiveType switch
    {
      PrimitiveDataType.Varchar => $"VARCHAR({MaxLength})",
      PrimitiveDataType.Blob => $"BLOB({MaxLength})", // Assuming length matters for display
      PrimitiveDataType.Decimal => (Precision, Scale) switch
      {
        (not null, not null) => $"DECIMAL({Precision}, {Scale})",
        // Case where only Precision is set (Scale defaults to 0 in constructor) - should not happen if Scale is always set when Precision is.
        // Let's handle it defensively anyway or rely on constructor guarantee. If Scale is always set, this case is redundant.
        // Assuming constructor guarantees Scale is set if Precision is:
        // (not null, null) => $"DECIMAL({Precision})",
        _ => "DECIMAL" // Default if no precision/scale specified (allowed by constructor if both are null)
      },
      PrimitiveDataType.Integer => "INTEGER",
      PrimitiveDataType.Boolean => "BOOLEAN",
      PrimitiveDataType.DateTime => "DATETIME",
      PrimitiveDataType.Float => "FLOAT",
      // PrimitiveDataType.Unknown case handled by constructor exception
      _ => PrimitiveType.ToString().ToUpperInvariant() // Should not be reached if constructor validates properly
    };
  }

  /// <summary>
  /// Determines whether the specified object is equal to the current object.
  /// </summary>
  public override bool Equals(object? obj)
  {
    return Equals(obj as DataTypeInfo);
  }

  /// <summary>
  /// Indicates whether the current object is equal to another object of the same type.
  /// Comparison includes the primitive type and relevant parameters (MaxLength, Precision, Scale).
  /// </summary>
  public bool Equals(DataTypeInfo? other)
  {
    if (other is null)
    {
      return false;
    }

    // Optimization: Reference equality
    if (ReferenceEquals(this, other))
    {
      return true;
    }

    // Compare primitive type and relevant parameters
    return PrimitiveType == other.PrimitiveType &&
           MaxLength == other.MaxLength &&
           Precision == other.Precision &&
           Scale == other.Scale;
  }

  /// <summary>
  /// Serves as the default hash function. Combines hash codes of relevant properties.
  /// </summary>
  public override int GetHashCode()
  {
    return HashCode.Combine(PrimitiveType, MaxLength, Precision, Scale);
  }

  /// <summary>
  /// Determines whether two specified DataTypeInfo objects have the same value.
  /// </summary>
  public static bool operator ==(DataTypeInfo? left, DataTypeInfo? right)
  {
    if (left is null)
    {
      return right is null;
    }
    return left.Equals(right);
  }

  /// <summary>
  /// Determines whether two specified DataTypeInfo objects have different values.
  /// </summary>
  public static bool operator !=(DataTypeInfo? left, DataTypeInfo? right)
  {
    return !(left == right);
  }
}