namespace ArmDb.Core.SchemaDefinition; // File-scoped namespace

/// <summary>
/// Defines the fundamental data types supported by the database system.
/// </summary>
public enum PrimitiveDataType
{
  /// <summary>
  /// Represents an unknown or unsupported type. Should not typically be used for column definitions.
  /// </summary>
  Unknown = 0,

  /// <summary>
  /// Represents a 64-bit signed integer (maps to C# long).
  /// </summary>
  Integer,

  /// <summary>
  /// Represents a variable-length string of characters. Will require parameters like MaxLength later.
  /// </summary>
  Varchar,

  /// <summary>
  /// Represents a boolean value (true/false).
  /// </summary>
  Boolean,

  /// <summary>
  /// Represents a fixed-point number. Will require parameters like Precision and Scale later.
  /// </summary>
  Decimal,

  /// <summary>
  /// Represents a date and time value.
  /// </summary>
  DateTime,

  /// <summary>
  /// Represents a double-precision floating-point number (maps to C# double).
  /// </summary>
  Float,

  /// <summary>
  /// Represents a variable-length binary data blob. May require parameters like MaxLength later.
  /// </summary>
  Blob
  // Add other types as needed (e.g., Date, Time, SmallInt, Char, Text, etc.)
}