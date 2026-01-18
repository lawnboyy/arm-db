namespace ArmDb.SchemaDefinition; // File-scoped namespace

/// <summary>
/// Represents the definition of a single table within the database schema,
/// including its name, columns, and constraints.
/// </summary>
public sealed class TableDefinition
{
  // Internal mutable collections
  private readonly List<ColumnDefinition> _columns;
  private readonly Dictionary<string, ColumnDefinition> _columnLookup;
  private readonly List<Constraint> _constraints;
  private readonly Dictionary<string, Constraint> _constraintLookup;

  // Public read-only views
  /// <summary>
  /// Gets the ordered list of columns defined for this table.
  /// </summary>
  public IReadOnlyList<ColumnDefinition> Columns => _columns.AsReadOnly();

  /// <summary>
  /// Gets the list of constraints defined for this table.
  /// </summary>
  public IReadOnlyList<Constraint> Constraints => _constraints.AsReadOnly();

  /// <summary>
  /// Gets the name of the table. Must be unique within the DatabaseSchema.
  /// </summary>
  public string Name { get; init; } // Can be init as it's set once at construction

  /// <summary>
  /// Unique identifier for this table definition.
  /// </summary>
  public int TableId { get; init; }

  /// <summary>
  /// Initializes a new instance of the <see cref="TableDefinition"/> class.
  /// </summary>
  /// <param name="name">The name of the table. Cannot be null or whitespace.</param>
  /// <exception cref="ArgumentException">Thrown if name is null or whitespace.</exception>
  public TableDefinition(string name, int tableId = 0)
  {
    TableId = tableId;

    if (string.IsNullOrWhiteSpace(name))
    {
      throw new ArgumentException("Table name cannot be null or whitespace.", nameof(name));
    }
    Name = name.Trim();

    // Initialize internal collections
    _columns = new List<ColumnDefinition>();
    _columnLookup = new Dictionary<string, ColumnDefinition>(StringComparer.OrdinalIgnoreCase); // Case-insensitive lookup
    _constraints = new List<Constraint>();
    _constraintLookup = new Dictionary<string, Constraint>(StringComparer.OrdinalIgnoreCase); // Case-insensitive lookup
  }

  /// <summary>
  /// Creates a deep copy of the current TableDefinition with a new Table ID.
  /// </summary>
  /// <param name="newId">The new Table ID to assign.</param>
  /// <returns>A new TableDefinition instance with the specified ID.</returns>
  public TableDefinition WithId(int newId)
  {
    var clone = new TableDefinition(Name, newId);

    // Add columns (Columns are immutable value objects, so safe to share references in "clone")
    foreach (var column in _columns)
    {
      clone.AddColumn(column);
    }

    // Add constraints
    foreach (var constraint in _constraints)
    {
      clone.AddConstraint(constraint);
    }

    return clone;
  }

  /// <summary>
  /// Adds a column definition to the table.
  /// </summary>
  /// <param name="column">The column definition to add. Cannot be null.</param>
  /// <exception cref="ArgumentNullException">Thrown if column is null.</exception>
  /// <exception cref="ArgumentException">Thrown if a column with the same name (case-insensitive) already exists.</exception>
  public void AddColumn(ColumnDefinition column)
  {
    ArgumentNullException.ThrowIfNull(column);

    if (_columnLookup.ContainsKey(column.Name))
    {
      throw new ArgumentException($"A column with the name '{column.Name}' already exists in table '{Name}'. Column names must be unique (case-insensitive).", nameof(column));
    }

    _columns.Add(column);
    _columnLookup.Add(column.Name, column);
  }

  /// <summary>
  /// Adds a constraint definition to the table. Performs validation specific to the constraint type.
  /// </summary>
  /// <param name="constraint">The constraint definition to add. Cannot be null.</param>
  /// <exception cref="ArgumentNullException">Thrown if constraint is null.</exception>
  /// <exception cref="ArgumentException">Thrown if a constraint with the same name (case-insensitive) already exists.</exception>
  /// <exception cref="InvalidOperationException">Thrown for constraint-specific violations (e.g., adding second PK, constraint column not found).</exception>
  public void AddConstraint(Constraint constraint)
  {
    ArgumentNullException.ThrowIfNull(constraint);

    if (_constraintLookup.ContainsKey(constraint.Name))
    {
      throw new ArgumentException($"A constraint with the name '{constraint.Name}' already exists in table '{Name}'. Constraint names must be unique (case-insensitive).", nameof(constraint));
    }

    // === Constraint-Specific Validation ===
    switch (constraint)
    {
      case PrimaryKeyConstraint pk:
        // 1. Ensure only one PK per table
        if (GetPrimaryKeyConstraint() != null)
        {
          throw new InvalidOperationException($"Cannot add Primary Key '{pk.Name}'. Table '{Name}' already has a Primary Key defined ('{GetPrimaryKeyConstraint()!.Name}').");
        }
        // 2. Validate PK columns exist in this table
        //    The GetColumns method handles the check and throws if columns don't exist.
        var pkColumns = pk.GetColumns(this).ToList(); // Materialize to ensure check runs

        // 3. (Optional but recommended) Ensure PK columns are implicitly NOT NULL
        foreach (var pkCol in pkColumns)
        {
          if (pkCol.IsNullable)
          {
            // Option 1: Throw immediately
            throw new InvalidOperationException($"Column '{pkCol.Name}' used in Primary Key '{pk.Name}' must not be nullable.");
            // Option 2: Log a warning? Less strict.
            // Option 3: Could potentially modify the ColumnDefinition here, but definitions should ideally be immutable once created. Sticking with throwing.
          }
        }
        break;

      case ForeignKeyConstraint fk:
        // 1. Validate REFERENCING columns exist in THIS table
        //    The GetReferencingColumns method handles this check.
        var fkRefingColumns = fk.GetReferencingColumns(this).ToList(); // Materialize to ensure check runs

        // 2. Full validation of the REFERENCED side (table/columns exist, types match, referenced cols are PK/Unique)
        //    should ideally happen at the DatabaseSchema level when the full context is available.
        //    We *could* attempt it here if DatabaseSchema reference was passed, but deferring is cleaner.
        break;

      case UniqueKeyConstraint uq: // Assuming UniqueConstraint exists (similar structure to PK)
        // 1. Validate Unique columns exist in THIS table
        var uqColumns = uq.GetColumns(this).ToList(); // Assumes UniqueConstraint has GetColumns similar to PK
        break;

        //case CheckConstraint ck: // Assuming CheckConstraint exists
        // 1. Basic validation? Maybe check if expression is empty?
        //    Full parsing/validation of the expression happens much later.
        // if (string.IsNullOrWhiteSpace(ck.Expression)) throw new ArgumentException(...);
        //break;

        // Add cases for other constraint types...
    }

    // Add constraint if all validation passed
    _constraints.Add(constraint);
    _constraintLookup.Add(constraint.Name, constraint);
  }

  /// <summary>
  /// Gets a column definition by its name (case-insensitive).
  /// </summary>
  /// <param name="name">The name of the column to retrieve.</param>
  /// <returns>The ColumnDefinition if found; otherwise, null.</returns>
  public ColumnDefinition? GetColumn(string name)
  {
    ArgumentException.ThrowIfNullOrWhiteSpace(name);
    return _columnLookup.TryGetValue(name, out var column) ? column : null;
  }

  /// <summary>
  /// Gets a constraint definition by its name (case-insensitive).
  /// </summary>
  /// <param name="name">The name of the constraint to retrieve.</param>
  /// <returns>The Constraint if found; otherwise, null.</returns>
  public Constraint? GetConstraint(string name)
  {
    ArgumentException.ThrowIfNullOrWhiteSpace(name);
    return _constraintLookup.TryGetValue(name, out var constraint) ? constraint : null;
  }

  /// <summary>
  /// Gets the Primary Key constraint defined for this table, if any.
  /// </summary>
  /// <returns>The PrimaryKeyConstraint if defined; otherwise, null.</returns>
  public PrimaryKeyConstraint? GetPrimaryKeyConstraint()
  {
    // Assumes only one PK constraint exists due to AddConstraint validation
    return _constraints.OfType<PrimaryKeyConstraint>().FirstOrDefault();
  }

  /// <summary>
  /// Gets all Foreign Key constraints defined for this table.
  /// </summary>
  /// <returns>An enumerable collection of ForeignKeyConstraint objects.</returns>
  public IEnumerable<ForeignKeyConstraint> GetForeignKeyConstraints()
  {
    return _constraints.OfType<ForeignKeyConstraint>();
  }

  public ColumnDefinition[] GetPrimaryKeyColumnDefinitions()
  {
    // Determine the primary key columns...
    var primaryKeyConstraint = GetPrimaryKeyConstraint();

    if (primaryKeyConstraint == null)
      throw new InvalidOperationException($"No primary key contstraint was found on table {Name}!");

    var keyColumns = new ColumnDefinition[primaryKeyConstraint.ColumnNames.Count];
    var primaryKeyColumnNames = primaryKeyConstraint.ColumnNames;

    // Ensure that the order of the column list matches the primary key column order...
    for (var i = 0; i < primaryKeyColumnNames.Count; i++)
    {
      keyColumns[i] = Columns.First(c => c.Name == primaryKeyColumnNames[i]);
    }

    return keyColumns;
  }

  // Add similar helpers for UniqueConstraint, CheckConstraint if needed.

  // Consider adding ToString() override for debugging
  // public override string ToString() => $"Table: {Name} ({_columns.Count} Columns, {_constraints.Count} Constraints)";
}