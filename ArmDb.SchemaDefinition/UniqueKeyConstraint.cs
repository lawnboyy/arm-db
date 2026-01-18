namespace ArmDb.SchemaDefinition; // File-scoped namespace

/// <summary>
/// Represents a Unique constraint defined on one or more columns of a table.
/// Ensures that the combination of values in the specified columns is unique across all rows.
/// Standard SQL behavior typically allows multiple rows where one or more columns in the constraint are NULL.
/// Instances are immutable after creation.
/// </summary>
public sealed class UniqueKeyConstraint : Constraint
{
  /// <summary>
  /// Gets the ordered list of column names participating in the unique constraint.
  /// The list itself and the names within it are guaranteed to be non-null and non-whitespace.
  /// The order might be relevant depending on the underlying index implementation.
  /// </summary>
  public IReadOnlyList<string> ColumnNames { get; init; }

  /// <summary>
  /// Initializes a new instance of the <see cref="UniqueKeyConstraint"/> class.
  /// </summary>
  /// <param name="tableName">The name of the table this constraint belongs to (used for default naming and context). Must not be null or whitespace.</param>
  /// <param name="columnNames">The names of the columns forming the unique key. Must not be null or empty, cannot contain null/whitespace entries, and column names must be unique (case-insensitive).</param>
  /// <param name="name">Optional name for the constraint. If null/whitespace, a default name is generated (e.g., UQ_TableName_Suffix).</param>
  /// <exception cref="ArgumentNullException">Thrown if columnNames is null.</exception>
  /// <exception cref="ArgumentException">Thrown if tableName is null or whitespace, or if columnNames is empty, contains null/whitespace entries, or contains duplicate names.</exception>
  public UniqueKeyConstraint(string tableName, IEnumerable<string> columnNames, string? name = null)
      : base(name, "UQ", !string.IsNullOrWhiteSpace(tableName) ? tableName : throw new ArgumentException("Table name cannot be null or whitespace.", nameof(tableName))) // Use "UQ" prefix
  {
    ArgumentNullException.ThrowIfNull(columnNames);

    var columns = columnNames.ToList();

    // Validate column list content
    if (!columns.Any())
      throw new ArgumentException("Unique constraint must include at least one column.", nameof(columnNames));

    if (columns.Any(string.IsNullOrWhiteSpace))
      throw new ArgumentException("Unique constraint column names cannot be null or whitespace.", nameof(columnNames));

    // Ensure column names within the constraint are unique (case-insensitive check recommended for SQL behavior)
    if (columns.Distinct(StringComparer.OrdinalIgnoreCase).Count() != columns.Count)
      throw new ArgumentException("Unique constraint column names must be unique within the constraint definition (case-insensitive).", nameof(columnNames));

    // Assign the validated, immutable list
    ColumnNames = columns.AsReadOnly();
  }

  /// <summary>
  /// Retrieves the actual ColumnDefinition objects corresponding to the column names for this constraint
  /// from the provided TableDefinition. Useful for validation or index creation.
  /// </summary>
  /// <param name="table">The TableDefinition instance that owns this constraint. Must not be null.</param>
  /// <returns>An enumerable of non-null ColumnDefinition objects corresponding to the constraint's columns, in the order specified.</returns>
  /// <exception cref="ArgumentNullException">Thrown if table is null.</exception>
  /// <exception cref="InvalidOperationException">Thrown if any column name specified in the constraint is not found in the table definition.</exception>
  public IEnumerable<ColumnDefinition> GetColumns(TableDefinition table)
  {
    ArgumentNullException.ThrowIfNull(table);

    var columnDefinitions = new List<ColumnDefinition>(ColumnNames.Count);
    foreach (var columnName in ColumnNames) // Guaranteed non-null list/strings
    {
      // Assuming TableDefinition has GetColumn(string) -> ColumnDefinition?
      var column = table.GetColumn(columnName);
      if (column == null)
      {
        // Throw if a column defined in the UQ constraint doesn't exist in the table definition
        throw new InvalidOperationException($"Column '{columnName}' defined in unique constraint '{Name}' not found in table '{table.Name}'. Schema definition is inconsistent.");
      }
      columnDefinitions.Add(column); // column is non-null here
    }
    return columnDefinitions; // Return the list of found column definitions
  }

  // Potential override for ToString() if needed for debugging
  // public override string ToString() => $"CONSTRAINT {Name} UNIQUE ({string.Join(", ", ColumnNames)})";
}