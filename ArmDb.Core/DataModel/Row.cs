namespace ArmDb.Core.DataModel;

public class Row
{
  private readonly List<object> _values;

  public IEnumerable<object> Values { get { return _values;  } }

  public Row(Schema schema)
  {
    _values = new List<object>(schema.Columns.Count());
  }
}