namespace ArmDb.SchemaDefinition; // File-scoped namespace

/// <summary>
/// Abstract base class for all schema constraint definitions (e.g., Primary Key, Foreign Key, Unique).
/// It primarily provides a common structure for constraint naming.
/// </summary>
public abstract class Constraint
{
  /// <summary>
  /// Gets the optional user-defined or system-generated name of the constraint.
  /// This name is guaranteed to be non-null and non-whitespace after construction.
  /// </summary>
  /// <remarks>
  /// The setter is protected init-only, meaning the name can only be set by derived class constructors
  /// via the base constructor call during object initialization.
  /// </remarks>
  public string Name { get; protected init; }

  /// <summary>
  /// Initializes a new instance of the <see cref="Constraint"/> class,
  /// handling the assignment or generation of the constraint name.
  /// </summary>
  /// <param name="name">Optional constraint name provided by the user. If null or whitespace, a default name is generated.</param>
  /// <param name="defaultPrefix">Default prefix used if name is auto-generated (e.g., "PK", "FK", "UQ", "CK"). Must not be null or whitespace if name is null/whitespace.</param>
  /// <param name="sourceName">A relevant name (typically the table name) used for generating the default name. Must not be null or whitespace if name is null/whitespace.</param>
  /// <exception cref="ArgumentException">Thrown if defaultPrefix or sourceName is null or whitespace when required for default name generation.</exception>
  protected Constraint(string? name, string defaultPrefix, string sourceName)
  {
    if (string.IsNullOrWhiteSpace(name))
    {
      // Validate required parameters for default name generation
      if (string.IsNullOrWhiteSpace(defaultPrefix))
        throw new ArgumentException("Default prefix cannot be null or whitespace when constraint name is auto-generated.", nameof(defaultPrefix));
      if (string.IsNullOrWhiteSpace(sourceName))
        throw new ArgumentException("Source name (e.g., table name) cannot be null or whitespace when constraint name is auto-generated.", nameof(sourceName));

      // Generate a default name: PREFIX_SourceName_8CharGuidSuffix
      // Using a GUID suffix helps ensure uniqueness if multiple constraints of the same type exist on a table without explicit names.
      Name = $"{defaultPrefix.Trim()}_{sourceName.Trim()}_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
    }
    else
    {
      // Use the provided name, ensuring it's trimmed.
      Name = name.Trim();
    }
  }

  // Derived classes will add specific properties (like ColumnNames, ReferencedTable, etc.)
  // and potentially override ToString() or add helper methods.
}