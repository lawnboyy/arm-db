namespace ArmDb.Core.SchemaDefinition;

public class Schema
{
  private readonly List<ColumnDefinition> _columns;
  private readonly string _name;

  public Schema(string name)
  {
    _name = name ?? throw new ArgumentNullException(nameof(name));
    _columns = new List<ColumnDefinition>();
  }

  /// <summary>
  /// Exposes the column definitions as read-only.
  /// </summary>
  public IEnumerable<ColumnDefinition> Columns => _columns.AsReadOnly();

  public string Name => _name;

  /// <summary>
  /// Adds a new column to the schema.
  /// Throws an exception if a column with the same name already exists.
  /// </summary>
  /// <param name="column">The column to add.</param>
  public void AddColumn(ColumnDefinition column)
  {
    if (_columns.Any(c => c.Name == column.Name))
    {
      throw new InvalidOperationException($"A column with the name '{column.Name}' already exists.");
    }

    _columns.Add(column);
  }

  /// <summary>
  /// Removes a column by name.
  /// Returns true if the column was removed, false if not found.
  /// </summary>
  /// <param name="columnName">The name of the column to remove.</param>
  public void RemoveColumn(string columnName)
  {
    var index = _columns.FindIndex(c => c.Name == columnName);
    if (index < 0)
    {
      throw new InvalidOperationException($"Column '{columnName}' does not exist.");
    }

    _columns.RemoveAt(index);
  }

  /// <summary>
  /// Retrieves a column definition by name.
  /// </summary>
  /// <param name="columnName">The name of the column.</param>
  /// <returns>The column definition if found, otherwise null.</returns>
  public ColumnDefinition? GetColumn(string columnName)
  {
    return _columns.FirstOrDefault(c => c.Name == columnName);
  }

  /// <summary>
  /// Gets the index of a column by name.
  /// </summary>
  /// <param name="columnName">The name of the column.</param>
  /// <returns>The index of the column in the schema, or -1 if not found.</returns>
  public int GetColumnIndex(string columnName)
  {
    return _columns.FindIndex(c => c.Name == columnName);
  }

  /// <summary>
  /// Gets the number of columns in the schema.
  /// </summary>
  public int ColumnCount => _columns.Count;
}
