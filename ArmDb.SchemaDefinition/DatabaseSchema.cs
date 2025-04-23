using System;
using System.Collections.Generic;
using System.Linq;

namespace ArmDb.SchemaDefinition; // File-scoped namespace

/// <summary>
/// Represents the complete schema definition for a database, including all its tables.
/// </summary>
public sealed class DatabaseSchema
{
  // Internal mutable collection for tables, keyed by name (case-insensitive)
  private readonly Dictionary<string, TableDefinition> _tables;

  /// <summary>
  /// Gets a read-only dictionary of all tables defined in the schema, keyed by table name (case-insensitive).
  /// </summary>
  public IReadOnlyDictionary<string, TableDefinition> Tables => _tables.AsReadOnly(); // Expose as read-only dictionary

  /// <summary>
  /// Gets the name of the database schema.
  /// </summary>
  public string Name { get; init; }

  /// <summary>
  /// Initializes a new instance of the <see cref="DatabaseSchema"/> class.
  /// </summary>
  /// <param name="name">The name of the database schema. Cannot be null or whitespace.</param>
  /// <exception cref="ArgumentException">Thrown if name is null or whitespace.</exception>
  public DatabaseSchema(string name)
  {
    if (string.IsNullOrWhiteSpace(name))
    {
      throw new ArgumentException("Database schema name cannot be null or whitespace.", nameof(name));
    }
    Name = name.Trim();

    // Initialize the internal table dictionary with case-insensitive comparison
    _tables = new Dictionary<string, TableDefinition>(StringComparer.OrdinalIgnoreCase);
  }

  /// <summary>
  /// Adds a table definition to the database schema.
  /// </summary>
  /// <param name="table">The table definition to add. Cannot be null.</param>
  /// <exception cref="ArgumentNullException">Thrown if table is null.</exception>
  /// <exception cref="ArgumentException">Thrown if a table with the same name (case-insensitive) already exists in the schema.</exception>
  public void AddTable(TableDefinition table)
  {
    ArgumentNullException.ThrowIfNull(table);

    if (_tables.ContainsKey(table.Name))
    {
      throw new ArgumentException($"A table with the name '{table.Name}' already exists in the database schema '{Name}'. Table names must be unique (case-insensitive).", nameof(table));
    }

    _tables.Add(table.Name, table);
  }

  /// <summary>
  /// Gets a table definition by its name (case-insensitive).
  /// </summary>
  /// <param name="name">The name of the table to retrieve.</param>
  /// <returns>The TableDefinition if found; otherwise, null.</returns>
  public TableDefinition? GetTable(string name)
  {
    ArgumentException.ThrowIfNullOrWhiteSpace(name); // Check input name
    return _tables.TryGetValue(name, out var table) ? table : null;
  }

  /// <summary>
  /// [Placeholder] Performs comprehensive validation of the entire schema *after* all tables and constraints
  /// have been added. This is crucial for checking cross-table dependencies like Foreign Keys.
  /// </summary>
  /// <remarks>
  /// Implementations should verify:
  /// - Foreign Key constraints reference existing tables and columns.
  /// - Referenced columns for FKs form a primary or unique key on the referenced table.
  /// - Data types between referencing and referenced FK columns are compatible.
  /// - No other inconsistencies exist (e.g., unresolved dependencies).
  /// </remarks>
  /// <exception cref="InvalidOperationException">Thrown if schema inconsistencies are found.</exception>
  public void ValidateSchemaIntegrity()
  {
    // --- Implementation deferred ---
    // This is where complex cross-table validation would go.

    // Example checks for Foreign Keys:
    foreach (var table in Tables.Values)
    {
      foreach (var fk in table.GetForeignKeyConstraints()) // Assumes TableDefinition has GetForeignKeyConstraints()
      {
        // 1. Check if referenced table exists
        var referencedTable = GetTable(fk.ReferencedTableName);
        if (referencedTable == null)
        {
          throw new InvalidOperationException($"Foreign Key '{fk.Name}' in table '{table.Name}' references table '{fk.ReferencedTableName}', which does not exist in the schema '{Name}'.");
        }

        // 2. Check if referenced columns exist in the referenced table
        var referencedColumns = new List<ColumnDefinition>();
        foreach (var colName in fk.ReferencedColumnNames)
        {
          var col = referencedTable.GetColumn(colName);
          if (col == null)
          {
            throw new InvalidOperationException($"Foreign Key '{fk.Name}' in table '{table.Name}' references column '{colName}' in table '{referencedTable.Name}', but the column does not exist.");
          }
          referencedColumns.Add(col);
        }

        // 3. Check if referenced columns form a PK or Unique constraint
        //    (Requires looking up PK/Unique constraints on referencedTable and matching columns)
        var pk = referencedTable.GetPrimaryKeyConstraint();
        bool isPkMatch = pk != null && pk.ColumnNames.SequenceEqual(fk.ReferencedColumnNames, StringComparer.OrdinalIgnoreCase);
        // bool isUniqueMatch = referencedTable.GetUniqueConstraints().Any(uq => uq.ColumnNames.SequenceEqual(fk.ReferencedColumnNames, StringComparer.OrdinalIgnoreCase)); // Assumes GetUniqueConstraints() exists
        // if (!isPkMatch && !isUniqueMatch) { ... throw ... }
        if (!isPkMatch)
        { // Simplified check for PK only for now
          throw new InvalidOperationException($"Foreign Key '{fk.Name}' in table '{table.Name}' references columns ({string.Join(", ", fk.ReferencedColumnNames)}) in table '{referencedTable.Name}', which do not form the primary key of that table.");
          // Add check for UNIQUE constraints later
        }


        // 4. Check data type compatibility
        var referencingColumns = fk.GetReferencingColumns(table).ToList(); // Already checks existence in referencing table
        for (int i = 0; i < referencingColumns.Count; i++)
        {
          if (!AreDataTypesCompatible(referencingColumns[i].DataType, referencedColumns[i].DataType))
          {
            throw new InvalidOperationException($"Foreign Key '{fk.Name}' in table '{table.Name}': Data type mismatch between referencing column '{referencingColumns[i].Name}' ({referencingColumns[i].DataType}) and referenced column '{referencedColumns[i].Name}' ({referencedColumns[i].DataType}).");
          }
        }

        // 5. Check nullability rules vs actions (e.g., SET NULL requires referencing columns to be nullable)
        if (fk.OnDeleteAction == ReferentialAction.SetNull || fk.OnUpdateAction == ReferentialAction.SetNull)
        {
          if (referencingColumns.Any(c => !c.IsNullable))
          {
            throw new InvalidOperationException($"Foreign Key '{fk.Name}' in table '{table.Name}' uses SET NULL action, but one or more referencing columns are defined as NOT NULL.");
          }
        }
        // Add checks for SET DEFAULT (requires default value) if implementing that action fully.

      } // end foreach fk
    } // end foreach table

    // Add other schema-wide checks...

    // If we reach here, the basic structural integrity (regarding FKs) is okay.
    Console.WriteLine($"Schema '{Name}' validation successful (basic FK checks)."); // Or log properly
  }

  /// <summary>
  /// [Helper Placeholder] Checks if two data types are compatible for FK relationships.
  /// Needs proper implementation based on desired compatibility rules.
  /// </summary>
  private bool AreDataTypesCompatible(DataTypeInfo referencingType, DataTypeInfo referencedType)
  {
    // Basic check: Primitive types must match exactly.
    if (referencingType.PrimitiveType != referencedType.PrimitiveType) return false;

    // Type-specific checks (e.g., Varchar lengths, Decimal precision/scale)
    switch (referencingType.PrimitiveType)
    {
      case PrimitiveDataType.Varchar:
        // Allow referencing Varchar(N) to reference Varchar(M) if N <= M? Or require exact match?
        // Let's require exact match for simplicity for now.
        return referencingType.MaxLength == referencedType.MaxLength;
      case PrimitiveDataType.Decimal:
        // Require exact match for precision and scale.
        return referencingType.Precision == referencedType.Precision && referencingType.Scale == referencedType.Scale;
      // Add checks for other types if needed (e.g., Blob length?)
      default:
        // For types without parameters (Integer, Boolean, etc.), matching primitive type is enough.
        return true;
    }
  }


  // Consider adding ToString() override for debugging
  // public override string ToString() => $"Database Schema: {Name} ({_tables.Count} Tables)";
}