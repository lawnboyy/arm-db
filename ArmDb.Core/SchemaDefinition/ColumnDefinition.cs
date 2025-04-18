namespace ArmDb.Core.SchemaDefinition;

public class ColumnDefinition
{
  public string Name { get; set; }
  public ColumnType ColumnType { get; set; }
  public Type ColumnDataType { get; set; }

  public ColumnDefinition(string name, ColumnType columnType)
  {
    Name = name;
    ColumnType = columnType;

    // Map ColumnType to C# Type
    ColumnDataType = columnType switch
    {
      ColumnType.Int => typeof(int),
      ColumnType.String => typeof(string),
      ColumnType.DateTime => typeof(DateTime),
      ColumnType.Bool => typeof(bool),
      _ => throw new InvalidOperationException($"Unsupported column type: {columnType}")
    };
  }
}
