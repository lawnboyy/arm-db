using ArmDb.Core.SchemaDefinition;

namespace ArmDb.Tests;

public class ColumnDefinitionTests
{
  [Fact]
  public void Constructor_ShouldMapColumnTypeToCorrectDataType()
  {
    var columnInt = new ColumnDefinition("TestInt", PrimitiveDataType.Integer);
    Assert.Equal(typeof(int), columnInt.ColumnDataType);

    var columnString = new ColumnDefinition("TestString", PrimitiveDataType.Varchar);
    Assert.Equal(typeof(string), columnString.ColumnDataType);

    var columnDateTime = new ColumnDefinition("TestDateTime", PrimitiveDataType.DateTime);
    Assert.Equal(typeof(DateTime), columnDateTime.ColumnDataType);

    var columnBool = new ColumnDefinition("TestBool", PrimitiveDataType.Bool);
    Assert.Equal(typeof(bool), columnBool.ColumnDataType);
  }

  [Fact]
  public void Constructor_ShouldThrowExceptionForInvalidColumnType()
  {
    Assert.Throws<InvalidOperationException>(() =>
      new ColumnDefinition("Invalid", (PrimitiveDataType)999));
  }
}
