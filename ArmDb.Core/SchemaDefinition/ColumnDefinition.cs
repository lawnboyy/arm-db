namespace ArmDb.Core.SchemaDefinition;

/// <summary>
/// Represents the definition of a single column within a table.
/// Includes the column's name, data type, nullability, and optional default value.
/// Instances of this class are immutable after creation.
/// </summary>
public sealed class ColumnDefinition
{
  /// <summary>
  /// Gets the name of the column. Must be unique within its TableDefinition.
  /// </summary>
  public string Name { get; init; }

  /// <summary>
  /// Gets the detailed data type information for the column.
  /// </summary>
  public DataTypeInfo DataType { get; init; }

  /// <summary>
  /// Gets a value indicating whether the column allows NULL values.
  /// Defaults to true, aligning with common SQL behavior.
  /// </summary>
  public bool IsNullable { get; init; }

  /// <summary>
  /// Gets the optional SQL expression string defining the default value for this column.
  /// If null, the column has no explicit default value (database might have implicit defaults like NULL).
  /// The validity/parsing of this expression is handled at a later stage (e.g., query processing).
  /// </summary>
  public string? DefaultValueExpression { get; init; }

  /// <summary>
  /// Initializes a new instance of the <see cref="ColumnDefinition"/> class.
  /// </summary>
  /// <param name="name">The name of the column. Cannot be null or whitespace.</param>
  /// <param name="dataType">The data type information for the column. Cannot be null.</param>
  /// <param name="isNullable">Whether the column allows null values. Defaults to true.</param>
  /// <param name="defaultValueExpression">Optional default value expression string. Can be null, but not empty or just whitespace if provided.</param>
  /// <exception cref="ArgumentException">Thrown if name is null or whitespace, or if defaultValueExpression is empty or whitespace.</exception>
  /// <exception cref="ArgumentNullException">Thrown if dataType is null.</exception>
  public ColumnDefinition(string name, DataTypeInfo dataType, bool isNullable = true, string? defaultValueExpression = null)
  {
    // Validate mandatory parameters
    if (string.IsNullOrWhiteSpace(name))
    {
      // Use ArgumentException for null or whitespace check combined
      throw new ArgumentException("Column name cannot be null or whitespace.", nameof(name));
    }
    ArgumentNullException.ThrowIfNull(dataType);

    // Validate optional default value (allow null, but not empty/whitespace)
    if (defaultValueExpression != null && string.IsNullOrWhiteSpace(defaultValueExpression))
    {
      throw new ArgumentException("Default value expression cannot be empty or whitespace. Use null if no default is intended.", nameof(defaultValueExpression));
    }

    // Assign properties
    Name = name.Trim(); // Trim valid name
    DataType = dataType;
    IsNullable = isNullable;
    // Trim valid expression, or keep null
    DefaultValueExpression = string.IsNullOrWhiteSpace(defaultValueExpression) ? null : defaultValueExpression.Trim();
  }

  // Consider adding ToString() override for easier debugging?
  // public override string ToString() => $"{Name} {DataType}{(IsNullable ? "" : " NOT NULL")}{(DefaultValueExpression == null ? "" : $" DEFAULT ({DefaultValueExpression})")}";
}