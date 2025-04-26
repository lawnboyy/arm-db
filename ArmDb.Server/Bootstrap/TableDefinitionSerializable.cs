using ArmDb.SchemaDefinition; // Needs reference to domain model

namespace ArmDb.Server.Bootstrap; // Updated namespace

/// <summary>
/// Serializable surrogate class for the entire TableDefinition JSON structure. Internal to bootstrap process.
/// </summary>
internal sealed class TableDefinitionSerializable
{
  public string? Name { get; init; }
  public List<ColumnDefinitionSerializable>? Columns { get; init; } // Uses local Serializable types
  public List<ConstraintSerializableBase>? Constraints { get; init; } // Uses local Serializable types

  public TableDefinitionSerializable() { }

  public TableDefinition ToTableDefinition()
  {
    if (string.IsNullOrWhiteSpace(Name))
      throw new InvalidOperationException("Deserialized table serializable object has missing or invalid Name.");

    var tableDef = new TableDefinition(Name);

    if (Columns != null)
    {
      foreach (var colSerializable in Columns)
      {
        if (colSerializable == null) throw new InvalidOperationException($"Table '{Name}' definition contains a null column serializable object.");
        tableDef.AddColumn(colSerializable.ToColumnDefinition());
      }
    }

    if (Constraints != null)
    {
      foreach (var conSerializable in Constraints)
      {
        if (conSerializable == null) throw new InvalidOperationException($"Table '{Name}' definition contains a null constraint serializable object.");
        tableDef.AddConstraint(conSerializable.ToConstraint(tableDef.Name));
      }
    }

    return tableDef;
  }
}