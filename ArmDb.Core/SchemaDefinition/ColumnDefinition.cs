namespace ArmDb.Core.SchemaDefinition;

public class ColumnDefinition
{
  public string Name { get; set; }
  public PrimitiveDataType ColumnType { get; set; }
  public Type ColumnDataType { get; set; }

  public ColumnDefinition(string name, PrimitiveDataType columnType)
  {
    Name = name;
    ColumnType = columnType;

    // Map ColumnType to C# Type
    ColumnDataType = columnType switch
    {
      PrimitiveDataType.Integer => typeof(int),
      PrimitiveDataType.Varchar => typeof(string),
      PrimitiveDataType.DateTime => typeof(DateTime),
      PrimitiveDataType.Boolean => typeof(bool),
      _ => throw new InvalidOperationException($"Unsupported column type: {columnType}")
    };
  }
}
