using ArmDb.Core.SchemaDefinition;

namespace ArmDb.Tests;

public class SchemaTests
{
  [Fact]
  public void Constructor_ShouldInitializeWithName()
  {
    var schema = new Schema("Users");

    Assert.Equal("Users", schema.Name);
    Assert.Empty(schema.Columns);
  }

  [Fact]
  public void AddColumn_ShouldAddNewColumn()
  {
    var schema = new Schema("Users");
    var column = new ColumnDefinition("Id", PrimitiveDataType.Integer);

    schema.AddColumn(column);

    Assert.Contains(column, schema.Columns);
  }

  [Fact]
  public void AddColumn_ShouldThrowIfDuplicateName()
  {
    var schema = new Schema("Users");
    var column1 = new ColumnDefinition("Email", PrimitiveDataType.Varchar);
    var column2 = new ColumnDefinition("Email", PrimitiveDataType.Varchar);

    schema.AddColumn(column1);

    Assert.Throws<InvalidOperationException>(() =>
      schema.AddColumn(column2));
  }

  [Fact]
  public void RemoveColumn_ShouldRemoveExistingColumn()
  {
    var schema = new Schema("Users");
    var column = new ColumnDefinition("Name", PrimitiveDataType.Varchar);

    schema.AddColumn(column);
    schema.RemoveColumn("Name");

    Assert.Empty(schema.Columns);
  }

  [Fact]
  public void RemoveColumn_ShouldThrowIfColumnDoesNotExist()
  {
    var schema = new Schema("Users");

    Assert.Throws<InvalidOperationException>(() =>
      schema.RemoveColumn("NonExistent"));
  }

  [Fact]
  public void Columns_ShouldBeReadOnly()
  {
    var schema = new Schema("Users");
    schema.AddColumn(new ColumnDefinition("Role", PrimitiveDataType.Varchar));

    var columns = schema.Columns;

    Assert.Throws<NotSupportedException>(() =>
    {
      var asList = (IList<ColumnDefinition>)columns;
      asList.Add(new ColumnDefinition("Hacker", PrimitiveDataType.Varchar));
    });
  }
}
