using System;
using System.Collections.Generic;
using System.Linq;

namespace ArmDb.Core.SchemaDefinition; // File-scoped namespace

/// <summary>
/// Represents a Primary Key constraint defined on one or more columns of a table.
/// Ensures uniqueness and implies non-nullability for the specified columns.
/// A table can typically have only one Primary Key constraint.
/// Instances are immutable after creation.
/// </summary>
public sealed class PrimaryKeyConstraint : Constraint
{
  /// <summary>
  /// Gets the ordered list of column names participating in the primary key.
  /// The list itself and the names within it are guaranteed to be non-null and non-whitespace.
  /// The order might be relevant depending on the underlying index implementation.
  /// </summary>
  public IReadOnlyList<string> ColumnNames { get; init; }

  /// <summary>
  /// Initializes a new instance of the <see cref="PrimaryKeyConstraint"/> class.
  /// </summary>
  /// <param name="tableName">The name of the table this constraint belongs to (used for default naming and context). Must not be null or whitespace.</param>
  /// <param name="columnNames">The names of the columns forming the primary key. Must not be null or empty, cannot contain null/whitespace entries, and column names must be unique (case-insensitive).</param>
  /// <param name="name">Optional name for the constraint. If null/whitespace, a default name is generated (e.g., PK_TableName_Suffix).</param>
  /// <exception cref="ArgumentNullException">Thrown if columnNames is null.</exception>
  /// <exception cref="ArgumentException">Thrown if tableName is null or whitespace, or if columnNames is empty, contains null/whitespace entries, or contains duplicate names.</exception>
  public PrimaryKeyConstraint(string tableName, IEnumerable<string> columnNames, string? name = null)
      : base(name, "PK", !string.IsNullOrWhiteSpace(tableName) ? tableName : throw new ArgumentException("Table name cannot be null or whitespace.", nameof(tableName)))
  {
    ArgumentNullException.ThrowIfNull(columnNames);

    var columns = columnNames.ToList();

    // Validate column list content
    if (!columns.Any())
      throw new ArgumentException("Primary key must include at least one column.", nameof(columnNames));

    if (columns.Any(string.IsNullOrWhiteSpace))
      throw new ArgumentException("Primary key column names cannot be null or whitespace.", nameof(columnNames));

    // Ensure column names within the PK are unique (case-insensitive check recommended for SQL behavior)
    if (columns.Distinct(StringComparer.OrdinalIgnoreCase).Count() != columns.Count)
      throw new ArgumentException("Primary key column names must be unique within the constraint definition (case-insensitive).", nameof(columnNames));

    // Assign the validated, immutable list
    ColumnNames = columns.AsReadOnly();
  }

  /// <summary>
  /// Retrieves the actual ColumnDefinition objects corresponding to the column names for this constraint
  /// from the provided TableDefinition. Useful for validation or query planning.
  /// </summary>
  /// <param name="table">The TableDefinition instance that owns this constraint. Must not be null.</param>
  /// <returns>An enumerable of non-null ColumnDefinition objects corresponding to the constraint's columns, in the order specified.</returns>
  /// <exception cref="ArgumentNullException">Thrown if table is null.</exception>
  /// <exception cref="InvalidOperationException">Thrown if any column name specified in the constraint is not found in the table definition.</exception>
  public IEnumerable<ColumnDefinition> GetColumns(TableDefinition table)
  {
    ArgumentNullException.ThrowIfNull(table);

    // Consider adding a check: if (!ReferenceEquals(this.SourceTable, table)) throw... if SourceTable reference was stored.
    // Or: if (!table.Name.Equals(base.Name.Split('_')[1], ...)) throw... but relies on naming convention. Best handled by caller context.

    var columnDefinitions = new List<ColumnDefinition>(ColumnNames.Count);
    foreach (var columnName in ColumnNames) // ColumnNames is guaranteed non-null list of non-null strings
    {
      // Assuming TableDefinition has a method GetColumn(string) -> ColumnDefinition? or similar
      var column = table.GetColumn(columnName);
      if (column == null)
      {
        // Throw if a column defined in the PK constraint doesn't exist in the table definition
        throw new InvalidOperationException($"Column '{columnName}' defined in primary key '{Name}' not found in table '{table.Name}'. Schema definition is inconsistent.");
      }
      columnDefinitions.Add(column); // column is non-null here
    }
    return columnDefinitions; // Return the list of found column definitions
  }

  // Potential override for ToString() if needed for debugging
  // public override string ToString() => $"CONSTRAINT {Name} PRIMARY KEY ({string.Join(", ", ColumnNames)})";
}