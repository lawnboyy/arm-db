using ArmDb.Core.DataDefinition;
using ArmDb.Core.SchemaDefinition;

namespace ArmDb.Tests;

public class RowTests
{
  private Schema CreateSchema(params ColumnDefinition[] columns)
  {
    var schema = new Schema("TestSchema");
    foreach (var column in columns)
    {
      schema.AddColumn(column);
    }
    return schema;
  }

  [Fact]
  public void SetValue_ShouldStoreAndRetrieveValue()
  {
    var column = new ColumnDefinition("Age", ColumnType.Int);
    var schema = CreateSchema(column);
    var row = new Row(schema);

    row.SetColumnValue("Age", 42);
    var value = row.GetColumnValue<int>("Age");

    Assert.Equal(42, value);
  }

  [Fact]
  public void SetValue_ShouldThrowForInvalidType()
  {
    var column = new ColumnDefinition("IsActive", ColumnType.Bool);
    var schema = CreateSchema(column);
    var row = new Row(schema);

    Assert.Throws<InvalidOperationException>(() =>
      row.SetColumnValue("IsActive", "not a bool"));
  }

  [Fact]
  public void GetValue_ShouldThrowIfColumnNotSet()
  {
    var column = new ColumnDefinition("Name", ColumnType.String);
    var schema = CreateSchema(column);
    var row = new Row(schema);

    Assert.Throws<InvalidOperationException>(() =>
      row.GetColumnValue<string>("Name"));
  }
}
