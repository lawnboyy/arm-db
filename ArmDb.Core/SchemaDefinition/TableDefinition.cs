namespace ArmDb.Core.SchemaDefinition
{
  public class TableDefinition
  {
    private readonly List<ColumnDefinition> _columns = new();

    public string Name { get; }
    public IReadOnlyList<ColumnDefinition> Columns => _columns;

    public TableDefinition(string name)
    {
      Name = name;
    }

    public void AddColumn(ColumnDefinition column)
    {
      _columns.Add(column);
    }
  }
}