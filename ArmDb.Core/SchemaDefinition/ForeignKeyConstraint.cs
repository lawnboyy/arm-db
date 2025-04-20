using System;
using System.Collections.Generic;
using System.Linq;

namespace ArmDb.Core.SchemaDefinition; // File-scoped namespace

/// <summary>
/// Represents a Foreign Key constraint definition, enforcing referential integrity
/// between columns in this table (referencing) and columns in a referenced (parent) table.
/// Instances are immutable after creation.
/// </summary>
public sealed class ForeignKeyConstraint : Constraint
{
  /// <summary>
  /// Gets the list of column names in the table defining this constraint (the "referencing" or "child" table).
  /// Guaranteed non-null list of non-null/whitespace strings.
  /// </summary>
  public IReadOnlyList<string> ReferencingColumnNames { get; init; }

  /// <summary>
  /// Gets the name of the table being referenced (the "parent" table).
  /// Guaranteed non-null/whitespace.
  /// </summary>
  public string ReferencedTableName { get; init; }

  /// <summary>
  /// Gets the list of column names in the referenced table that correspond positionally
  /// to the ReferencingColumnNames. These must typically form a Primary Key or Unique constraint
  /// in the referenced table. Guaranteed non-null list of non-null/whitespace strings.
  /// </summary>
  public IReadOnlyList<string> ReferencedColumnNames { get; init; }

  /// <summary>
  /// Gets the action to take on the referencing rows when the referenced key is updated.
  /// </summary>
  public ReferentialAction OnUpdateAction { get; init; }

  /// <summary>
  /// Gets the action to take on the referencing rows when the referenced row is deleted.
  /// </summary>
  public ReferentialAction OnDeleteAction { get; init; }

  /// <summary>
  /// Initializes a new instance of the <see cref="ForeignKeyConstraint"/> class.
  /// </summary>
  /// <param name="referencingTableName">The name of the table defining this constraint. Must not be null or whitespace.</param>
  /// <param name="referencingColumnNames">The column names in the referencing table. Must not be null/empty or contain null/whitespace/duplicates.</param>
  /// <param name="referencedTableName">The name of the table being referenced. Must not be null or whitespace.</param>
  /// <param name="referencedColumnNames">The corresponding column names in the referenced table. Must not be null/empty or contain null/whitespace/duplicates. Must match count of referencing columns.</param>
  /// <param name="name">Optional name for the constraint. If null/whitespace, a default name is generated (e.g., FK_RefingTable_RefedTable_Suffix).</param>
  /// <param name="onUpdateAction">Action to perform on referencing rows when referenced key is updated. Defaults to NoAction.</param>
  /// <param name="onDeleteAction">Action to perform on referencing rows when referenced row is deleted. Defaults to NoAction.</param>
  /// <exception cref="ArgumentNullException">Thrown if required arguments (column lists) are null.</exception>
  /// <exception cref="ArgumentException">Thrown if table names are null/whitespace, column lists are invalid (empty, contain invalid names, duplicates), or column counts don't match.</exception>
  public ForeignKeyConstraint(
      string referencingTableName,
      IEnumerable<string> referencingColumnNames,
      string referencedTableName,
      IEnumerable<string> referencedColumnNames,
      string? name = null,
      ReferentialAction onUpdateAction = ReferentialAction.NoAction,
      ReferentialAction onDeleteAction = ReferentialAction.NoAction)
      : base(name, "FK", !string.IsNullOrWhiteSpace(referencingTableName) ? referencingTableName : throw new ArgumentException("Referencing table name cannot be null or whitespace.", nameof(referencingTableName)))
  {
    // Validate simple args first
    ArgumentNullException.ThrowIfNull(referencingColumnNames);
    ArgumentNullException.ThrowIfNull(referencedColumnNames);
    if (string.IsNullOrWhiteSpace(referencedTableName))
      throw new ArgumentException("Referenced table name cannot be null or whitespace.", nameof(referencedTableName));

    var refingColsList = referencingColumnNames.ToList();
    var refedColsList = referencedColumnNames.ToList();

    // Validate referencing columns
    if (!refingColsList.Any())
      throw new ArgumentException("Referencing column list cannot be empty.", nameof(referencingColumnNames));
    if (refingColsList.Any(string.IsNullOrWhiteSpace))
      throw new ArgumentException("Referencing column names cannot be null or whitespace.", nameof(referencingColumnNames));
    if (refingColsList.Distinct(StringComparer.OrdinalIgnoreCase).Count() != refingColsList.Count)
      throw new ArgumentException("Referencing column names must be unique within the constraint definition (case-insensitive).", nameof(referencingColumnNames));

    // Validate referenced columns
    if (!refedColsList.Any())
      throw new ArgumentException("Referenced column list cannot be empty.", nameof(referencedColumnNames));
    if (refedColsList.Any(string.IsNullOrWhiteSpace))
      throw new ArgumentException("Referenced column names cannot be null or whitespace.", nameof(referencedColumnNames));
    if (refedColsList.Distinct(StringComparer.OrdinalIgnoreCase).Count() != refedColsList.Count)
      throw new ArgumentException("Referenced column names must be unique within the constraint definition (case-insensitive).", nameof(referencedColumnNames));

    // Validate column count match
    if (refingColsList.Count != refedColsList.Count)
      throw new ArgumentException("The number of referencing columns must match the number of referenced columns.", nameof(referencingColumnNames));

    // Assign validated values (properties are init-only)
    ReferencingColumnNames = refingColsList.AsReadOnly();
    ReferencedTableName = referencedTableName.Trim();
    ReferencedColumnNames = refedColsList.AsReadOnly();
    OnUpdateAction = onUpdateAction;
    OnDeleteAction = onDeleteAction;
  }

  /// <summary>
  /// Retrieves the actual ColumnDefinition objects for the referencing columns from the given table definition.
  /// </summary>
  /// <param name="referencingTable">The TableDefinition instance that defines this constraint. Must not be null.</param>
  /// <returns>An enumerable of non-null ColumnDefinition objects from the referencing table, in the order specified.</returns>
  /// <exception cref="ArgumentNullException">Thrown if referencingTable is null.</exception>
  /// <exception cref="InvalidOperationException">Thrown if any referencing column name is not found in the provided table definition.</exception>
  public IEnumerable<ColumnDefinition> GetReferencingColumns(TableDefinition referencingTable)
  {
    ArgumentNullException.ThrowIfNull(referencingTable);

    var columnDefinitions = new List<ColumnDefinition>(ReferencingColumnNames.Count);
    foreach (var columnName in ReferencingColumnNames) // Guaranteed non-null list/strings
    {
      // Assuming TableDefinition has GetColumn(string) -> ColumnDefinition?
      var column = referencingTable.GetColumn(columnName);
      if (column == null)
      {
        throw new InvalidOperationException($"Column '{columnName}' defined in foreign key '{Name}' not found in referencing table '{referencingTable.Name}'. Schema definition is inconsistent.");
      }
      columnDefinitions.Add(column); // column is non-null here
    }
    return columnDefinitions;
  }

  /// <summary>
  /// Retrieves the TableDefinition for the referenced table from the database schema.
  /// </summary>
  /// <param name="database">The DatabaseSchema instance containing the referenced table definition. Must not be null.</param>
  /// <returns>The non-null referenced TableDefinition.</returns>
  /// <exception cref="ArgumentNullException">Thrown if database is null.</exception>
  /// <exception cref="InvalidOperationException">Thrown if the referenced table name is not found in the database schema.</exception>
  public TableDefinition GetReferencedTable(DatabaseSchema database)
  {
    ArgumentNullException.ThrowIfNull(database);

    // Assuming DatabaseSchema has GetTable(string) -> TableDefinition?
    var referencedTable = database.GetTable(ReferencedTableName); // ReferencedTableName guaranteed non-null/whitespace
    if (referencedTable == null)
    {
      throw new InvalidOperationException($"Referenced table '{ReferencedTableName}' specified in foreign key '{Name}' not found in the database schema '{database.Name}'.");
    }
    return referencedTable; // referencedTable is non-null here
  }


  /// <summary>
  /// Retrieves the actual ColumnDefinition objects for the referenced columns from the referenced table,
  /// using the provided database schema to find the table definition.
  /// </summary>
  /// <param name="database">The DatabaseSchema instance containing the referenced table definition. Must not be null.</param>
  /// <returns>An enumerable of non-null ColumnDefinition objects from the referenced table, in the order specified.</returns>
  /// <exception cref="ArgumentNullException">Thrown if database is null.</exception>
  /// <exception cref="InvalidOperationException">Thrown if the referenced table or any referenced column name is not found.</exception>
  public IEnumerable<ColumnDefinition> GetReferencedColumns(DatabaseSchema database)
  {
    // Get the referenced table definition first (handles null DB check and table existence via exception)
    var referencedTable = GetReferencedTable(database); // Returns non-null TableDefinition or throws

    // Now retrieve the specific columns from the found table definition
    var columnDefinitions = new List<ColumnDefinition>(ReferencedColumnNames.Count);
    foreach (var columnName in ReferencedColumnNames) // Guaranteed non-null list/strings
    {
      // Assuming TableDefinition has GetColumn(string) -> ColumnDefinition?
      var column = referencedTable.GetColumn(columnName);
      if (column == null)
      {
        throw new InvalidOperationException($"Referenced column '{columnName}' specified in foreign key '{Name}' not found in referenced table '{referencedTable.Name}'. Schema definition is inconsistent.");
      }
      columnDefinitions.Add(column); // column is non-null here
    }
    return columnDefinitions;
  }

  // Potential override for ToString() if needed for debugging
  // public override string ToString() => $"CONSTRAINT {Name} FOREIGN KEY ({string.Join(", ", ReferencingColumnNames)}) REFERENCES {ReferencedTableName} ({string.Join(", ", ReferencedColumnNames)}) ON UPDATE {OnUpdateAction} ON DELETE {OnDeleteAction}";
}