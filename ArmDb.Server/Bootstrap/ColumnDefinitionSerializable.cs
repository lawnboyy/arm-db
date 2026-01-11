using ArmDb.SchemaDefinition;

namespace ArmDb.Server.Bootstrap;

/// <summary>
/// Serializable surrogate class for ColumnDefinition. Internal to bootstrap process.
/// </summary>
internal sealed class ColumnDefinitionSerializable
{
  public string? Name { get; init; }
  public DataTypeInfoSerializable? DataType { get; init; } // Uses the local Serializable type
  public bool IsNullable { get; init; } = true;
  public int OrdinalPosition { get; init; } = 0;
  public string? DefaultValueExpression { get; init; }

  public ColumnDefinitionSerializable() { }

  public ColumnDefinition ToColumnDefinition()
  {
    if (string.IsNullOrWhiteSpace(Name))
      throw new InvalidOperationException("Deserialized column serializable object has missing or invalid Name.");
    if (DataType == null)
      throw new InvalidOperationException($"Deserialized column serializable object '{Name}' has missing DataTypeInfo.");

    return new ColumnDefinition(Name, DataType.ToDataTypeInfo(), IsNullable, OrdinalPosition, DefaultValueExpression);
  }
}