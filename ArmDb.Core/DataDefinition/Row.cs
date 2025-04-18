using ArmDb.Core.SchemaDefinition;

namespace ArmDb.Core.DataDefinition;

public class Row
{
  private readonly Dictionary<string, object> _columnValues;
  private readonly Schema _schema;

  public Row(Schema schema)
  {
    _schema = schema ?? throw new ArgumentNullException(nameof(schema));
    _columnValues = new Dictionary<string, object>();
  }

  /// <summary>
  /// Sets the value for a specific column.
  /// </summary>
  /// <param name="columnName">The name of the column.</param>
  /// <param name="value">The value to set.</param>
  public void SetColumnValue(string columnName, object value)
  {
    var column = _schema.GetColumn(columnName);
    if (column == null)
    {
      throw new InvalidOperationException($"Column '{columnName}' does not exist.");
    }

    // Optionally, validate the type of value here.
    ValidateColumnValue(columnName, value);

    _columnValues[columnName] = value;
  }

  /// <summary>
  /// Gets the value of a specific column.
  /// </summary>
  /// <typeparam name="T">The expected type of the column value.</typeparam>
  /// <param name="columnName">The name of the column.</param>
  /// <returns>The value of the column.</returns>
  public T GetColumnValue<T>(string columnName)
  {
    if (_columnValues.TryGetValue(columnName, out var value))
    {
      if (value is T typedValue)
      {
        return typedValue;
      }
      throw new InvalidOperationException($"The value of column '{columnName}' is not of type {typeof(T)}.");
    }
    throw new InvalidOperationException($"Column '{columnName}' does not have a value.");
  }

  /// <summary>
  /// Validates that the value is of the correct type for the column.
  /// </summary>
  /// <param name="columnName">The name of the column.</param>
  /// <param name="value">The value to validate.</param>
  private void ValidateColumnValue(string columnName, object value)
  {
    var column = _schema.GetColumn(columnName);
    if (column != null)
    {
      // Validate based on ColumnDataType (runtime C# Type)
      if (value != null && value.GetType() != column.ColumnDataType)
      {
        throw new InvalidOperationException($"The value for column '{columnName}' should be of type {column.ColumnDataType}, but the provided value is of type {value.GetType()}.");
      }
    }
  }
}
