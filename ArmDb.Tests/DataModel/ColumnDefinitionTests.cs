using ArmDb.Core.DataModel;

namespace ArmDb.Tests;

public class ColumnDefinitionTests
{
  [Fact]
  public void Constructor_ShouldMapColumnTypeToCorrectDataType()
  {
    var columnInt = new ColumnDefinition("TestInt", ColumnType.Int);
    Assert.Equal(typeof(int), columnInt.ColumnDataType);

    var columnString = new ColumnDefinition("TestString", ColumnType.String);
    Assert.Equal(typeof(string), columnString.ColumnDataType);

    var columnDateTime = new ColumnDefinition("TestDateTime", ColumnType.DateTime);
    Assert.Equal(typeof(DateTime), columnDateTime.ColumnDataType);

    var columnBool = new ColumnDefinition("TestBool", ColumnType.Bool);
    Assert.Equal(typeof(bool), columnBool.ColumnDataType);
  }

  [Fact]
  public void Constructor_ShouldThrowExceptionForInvalidColumnType()
  {
    Assert.Throws<InvalidOperationException>(() =>
      new ColumnDefinition("Invalid", (ColumnType)999));
  }
}
