using ArmDb.DataModel;
using ArmDb.SchemaDefinition;

namespace ArmDb.UnitTests.StorageEngine;

public class KeyComparerTests
{
  private static readonly ColumnDefinition IntIdColumn = new("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false);

  [Fact]
  public void Compare_WithSingleIntKey_ReturnsCorrectOrder()
  {
    // Arrange
    var keySchema = new List<ColumnDefinition> { IntIdColumn };
    var keyComparer = new TestKeyComparer(keySchema);

    var keyA = new Key([DataValue.CreateInteger(100)]);
    var keyB = new Key([DataValue.CreateInteger(200)]);
    var keyC = new Key([DataValue.CreateInteger(100)]); // Equal to keyA

    // Act
    int a_vs_b = keyComparer.Compare(keyA, keyB);
    int b_vs_a = keyComparer.Compare(keyB, keyA);
    int a_vs_c = keyComparer.Compare(keyA, keyC);

    // Assert
    Assert.True(a_vs_b < 0, "Key A (100) should be less than Key B (200)");
    Assert.True(b_vs_a > 0, "Key B (200) should be greater than Key A (100)");
    Assert.True(a_vs_c == 0, "Key A (100) should be equal to Key C (100)");
  }

  [Fact]
  public void Compare_WithNullValuesInKey_SortsNullsFirst()
  {
    // Arrange
    // Use a nullable column for the key schema
    var nullableIntColumn = new ColumnDefinition("NullableId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: true);
    var keySchema = new List<ColumnDefinition> { nullableIntColumn };
    var keyComparer = new TestKeyComparer(keySchema);

    var keyWithNull = new Key([DataValue.CreateNull(PrimitiveDataType.Int)]);
    var keyWithValue = new Key([DataValue.CreateInteger(100)]);
    var keyWithAnotherNull = new Key([DataValue.CreateNull(PrimitiveDataType.Int)]);

    // Act
    int null_vs_value = keyComparer.Compare(keyWithNull, keyWithValue);
    int value_vs_null = keyComparer.Compare(keyWithValue, keyWithNull);
    int null_vs_null = keyComparer.Compare(keyWithNull, keyWithAnotherNull);

    // Assert
    Assert.True(null_vs_value < 0, "A NULL key should be considered less than a key with a value.");
    Assert.True(value_vs_null > 0, "A key with a value should be considered greater than a NULL key.");
    Assert.True(null_vs_null == 0, "Two NULL keys should be considered equal for sorting purposes.");
  }

  private static readonly ColumnDefinition BigIntIdColumn = new("Id", new DataTypeInfo(PrimitiveDataType.BigInt), isNullable: false);

  [Fact]
  public void Compare_WithSingleBigIntKey_ReturnsCorrectOrder()
  {
    // Arrange
    var keySchema = new List<ColumnDefinition> { BigIntIdColumn };
    var keyComparer = new TestKeyComparer(keySchema);

    var keyA = new Key([DataValue.CreateBigInteger(5_000_000_000L)]);
    var keyB = new Key([DataValue.CreateBigInteger(6_000_000_000L)]);
    var keyC = new Key([DataValue.CreateBigInteger(5_000_000_000L)]); // Equal to keyA

    // Act
    int a_vs_b = keyComparer.Compare(keyA, keyB);
    int b_vs_a = keyComparer.Compare(keyB, keyA);
    int a_vs_c = keyComparer.Compare(keyA, keyC);

    // Assert
    Assert.True(a_vs_b < 0, "Key A (5B) should be less than Key B (6B)");
    Assert.True(b_vs_a > 0, "Key B (6B) should be greater than Key A (5B)");
    Assert.True(a_vs_c == 0, "Key A (5B) should be equal to Key C (5B)");
  }

  private static readonly ColumnDefinition DecimalAmountColumn = new("Amount", new DataTypeInfo(PrimitiveDataType.Decimal), isNullable: false);

  [Fact]
  public void Compare_WithSingleDecimalKey_ReturnsCorrectOrder()
  {
    // Arrange
    var keySchema = new List<ColumnDefinition> { DecimalAmountColumn };
    var keyComparer = new TestKeyComparer(keySchema);

    var keyA = new Key([DataValue.CreateDecimal(123.45m)]);
    var keyB = new Key([DataValue.CreateDecimal(678.90m)]);
    var keyC = new Key([DataValue.CreateDecimal(123.45m)]); // Equal to keyA

    // Act
    int a_vs_b = keyComparer.Compare(keyA, keyB);
    int b_vs_a = keyComparer.Compare(keyB, keyA);
    int a_vs_c = keyComparer.Compare(keyA, keyC);

    // Assert
    Assert.True(a_vs_b < 0, "Key A (123.45) should be less than Key B (678.90)");
    Assert.True(b_vs_a > 0, "Key B (678.90) should be greater than Key A (123.45)");
    Assert.True(a_vs_c == 0, "Key A (123.45) should be equal to Key C (123.45)");
  }

  private class TestKeyComparer : KeyComparer
  {
    public TestKeyComparer(IReadOnlyList<ColumnDefinition> columns) : base(columns)
    {
    }
  }
}