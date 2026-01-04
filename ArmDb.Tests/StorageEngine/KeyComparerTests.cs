using ArmDb.DataModel;
using ArmDb.SchemaDefinition;

namespace ArmDb.UnitTests.Storage;

public class KeyComparerTests
{
  private static readonly ColumnDefinition IntIdColumn = new("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false);

  [Fact]
  public void Compare_WithSingleIntKey_ReturnsCorrectOrder()
  {
    // Arrange
    var keyComparer = new KeyComparer();

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
    var keyComparer = new KeyComparer();

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

  [Fact]
  public void Compare_WithSingleBigIntKey_ReturnsCorrectOrder()
  {
    // Arrange
    var keyComparer = new KeyComparer();

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

  [Fact]
  public void Compare_WithSingleDecimalKey_ReturnsCorrectOrder()
  {
    // Arrange
    var keyComparer = new KeyComparer();

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

  [Fact]
  public void Compare_WithSingleFloatKey_ReturnsCorrectOrder()
  {
    // Arrange
    var keyComparer = new KeyComparer();

    var keyA = new Key([DataValue.CreateFloat(98.6)]);
    var keyB = new Key([DataValue.CreateFloat(101.1)]);
    var keyC = new Key([DataValue.CreateFloat(98.6)]); // Equal to keyA

    // Act
    int a_vs_b = keyComparer.Compare(keyA, keyB);
    int b_vs_a = keyComparer.Compare(keyB, keyA);
    int a_vs_c = keyComparer.Compare(keyA, keyC);

    // Assert
    Assert.True(a_vs_b < 0, "Key A (98.6) should be less than Key B (101.1)");
    Assert.True(b_vs_a > 0, "Key B (101.1) should be greater than Key A (98.6)");
    Assert.True(a_vs_c == 0, "Key A (98.6) should be equal to Key C (98.6)");
  }

  [Fact]
  public void Compare_WithSingleStringKey_ReturnsCorrectLexicographicalOrder()
  {
    // Arrange
    var keyComparer = new KeyComparer();

    var keyA = new Key([DataValue.CreateString("Apple")]);
    var keyB = new Key([DataValue.CreateString("Banana")]);
    var keyC = new Key([DataValue.CreateString("Apple")]);
    // Key with different casing to test case-sensitivity
    var keyD = new Key([DataValue.CreateString("apple")]);

    // Act
    int a_vs_b = keyComparer.Compare(keyA, keyB);
    int b_vs_a = keyComparer.Compare(keyB, keyA);
    int a_vs_c = keyComparer.Compare(keyA, keyC);
    int a_vs_d = keyComparer.Compare(keyA, keyD); // Case-sensitive comparison

    // Assert
    Assert.True(a_vs_b < 0, "Key A ('Apple') should be less than Key B ('Banana')");
    Assert.True(b_vs_a > 0, "Key B ('Banana') should be greater than Key A ('Apple')");
    Assert.True(a_vs_c == 0, "Key A ('Apple') should be equal to Key C ('Apple')");
    Assert.True(a_vs_d < 0, "Key A ('Apple') should be less than Key D ('apple') in an Ordinal comparison");
  }

  [Fact]
  public void Compare_WithSingleBooleanKey_ReturnsCorrectOrder()
  {
    // Arrange
    var keyComparer = new KeyComparer();

    var keyTrue = new Key([DataValue.CreateBoolean(true)]);
    var keyFalse = new Key([DataValue.CreateBoolean(false)]);
    var keyAnotherTrue = new Key([DataValue.CreateBoolean(true)]);
    var keyFalse2 = new Key([DataValue.CreateBoolean(false)]);

    // Act
    int false_vs_true = keyComparer.Compare(keyFalse, keyTrue);
    int true_vs_false = keyComparer.Compare(keyTrue, keyFalse);
    int true_vs_true = keyComparer.Compare(keyTrue, keyAnotherTrue);
    int false_vs_false = keyComparer.Compare(keyFalse, keyFalse2);

    // Assert
    // In C#, false.CompareTo(true) is < 0
    Assert.True(false_vs_true < 0, "Key False should be less than Key True");
    Assert.True(true_vs_false > 0, "Key True should be greater than Key False");
    Assert.True(true_vs_true == 0, "Two True keys should be equal");
    Assert.True(false_vs_false == 0, "Two False keys should be equal");
  }

  [Fact]
  public void Compare_WithSingleDateTimeKey_ReturnsCorrectOrder()
  {
    // Arrange
    var keyComparer = new KeyComparer();

    var keyA = new Key([DataValue.CreateDateTime(new DateTime(2024, 1, 1, 10, 0, 0))]); // Earlier
    var keyB = new Key([DataValue.CreateDateTime(new DateTime(2024, 1, 1, 12, 0, 0))]); // Later
    var keyC = new Key([DataValue.CreateDateTime(new DateTime(2024, 1, 1, 10, 0, 0))]); // Equal to keyA

    // Act
    int a_vs_b = keyComparer.Compare(keyA, keyB);
    int b_vs_a = keyComparer.Compare(keyB, keyA);
    int a_vs_c = keyComparer.Compare(keyA, keyC);

    // Assert
    Assert.True(a_vs_b < 0, "Key A (earlier) should be less than Key B (later)");
    Assert.True(b_vs_a > 0, "Key B (later) should be greater than Key A (earlier)");
    Assert.True(a_vs_c == 0, "Two identical DateTime keys should be equal");
  }

  [Fact]
  public void Compare_WithSingleBlobKey_ReturnsCorrectLexicographicalOrder()
  {
    // Arrange
    var keyComparer = new KeyComparer();

    var keyA = new Key([DataValue.CreateBlob([0x01, 0x02, 0x03])]);
    var keyB = new Key([DataValue.CreateBlob([0x01, 0x02, 0x04])]); // Different last byte
    var keyC = new Key([DataValue.CreateBlob([0x01, 0x02, 0x03])]); // Equal to keyA
    var keyD = new Key([DataValue.CreateBlob([0x01, 0x02])]);      // Shorter prefix of A

    // Act
    int a_vs_b = keyComparer.Compare(keyA, keyB);
    int b_vs_a = keyComparer.Compare(keyB, keyA);
    int a_vs_c = keyComparer.Compare(keyA, keyC);
    int d_vs_a = keyComparer.Compare(keyD, keyA); // Shorter vs longer

    // Assert
    Assert.True(a_vs_b < 0, "Key A should be less than Key B");
    Assert.True(b_vs_a > 0, "Key B should be greater than Key A");
    Assert.True(a_vs_c == 0, "Two identical Blob keys should be equal");
    Assert.True(d_vs_a < 0, "Key D (shorter prefix) should be less than Key A");
  }

  [Fact]
  public void Compare_WithCompositeKey_ReturnsCorrectLexicographicalOrder()
  {
    // Arrange
    var keyComparer = new KeyComparer();

    // Key format: (string, int)
    var keyA = new Key([DataValue.CreateString("Sales"), DataValue.CreateInteger(101)]);
    var keyB = new Key([DataValue.CreateString("Sales"), DataValue.CreateInteger(102)]); // Same first part, greater second
    var keyC = new Key([DataValue.CreateString("Engineering"), DataValue.CreateInteger(50)]); // Lesser first part
    var keyD = new Key([DataValue.CreateString("Sales"), DataValue.CreateInteger(101)]); // Equal to keyA

    // Act
    int a_vs_b = keyComparer.Compare(keyA, keyB); // Should be < 0 (decided by second column)
    int a_vs_c = keyComparer.Compare(keyA, keyC); // Should be > 0 (decided by first column)
    int a_vs_d = keyComparer.Compare(keyA, keyD); // Should be == 0

    // Assert
    Assert.True(a_vs_b < 0, "Key A should be less than Key B based on the second column.");
    Assert.True(a_vs_c > 0, "Key A should be greater than Key C based on the first column.");
    Assert.True(a_vs_d == 0, "Key A and Key D should be equal.");
  }

  [Fact]
  public void Compare_WithThreeColumnKey_IsDecidedByThirdColumnWhenFirstTwoMatch()
  {
    // Arrange
    var keyComparer = new KeyComparer();

    // Key format: (string, int, string)
    var keyA = new Key([
        DataValue.CreateString("Sales"),
        DataValue.CreateInteger(2024),
        DataValue.CreateString("Alpha") // This is the deciding column
    ]);

    var keyB = new Key([
        DataValue.CreateString("Sales"),
        DataValue.CreateInteger(2024),
        DataValue.CreateString("Beta") // This is the deciding column
    ]);

    var keyC = new Key([
        DataValue.CreateString("Sales"),
        DataValue.CreateInteger(2025), // This column is different
        DataValue.CreateString("Alpha")
    ]);

    // Act
    int a_vs_b = keyComparer.Compare(keyA, keyB);
    int a_vs_c = keyComparer.Compare(keyA, keyC);

    // Assert
    Assert.True(a_vs_b < 0, "Key A should be less than Key B based on the third column ('Alpha' < 'Beta').");
    Assert.True(a_vs_c < 0, "Key A should be less than Key C based on the second column (2024 < 2025).");
  }

  // Add this test method and its MemberData to your KeyComparerTests.cs file

  public static IEnumerable<object[]> CompositeKeyWithNullsTestData =>
      new List<object[]>
      {
        // { Key A, Key B, expected result (<0, 0, or >0), explanation }

        // Scenario 1: NULL in the first column should be the deciding factor
        new object[] {
            new Key([DataValue.CreateNull(PrimitiveDataType.Varchar), DataValue.CreateInteger(100)]),
            new Key([DataValue.CreateString("A"), DataValue.CreateInteger(50)]),
            -1, // Expected: keyA < keyB
            "NULL in the first column should sort before any value."
        },

        // Scenario 2: First column is equal, NULL in the second column is the deciding factor
        new object[] {
            new Key([DataValue.CreateString("A"), DataValue.CreateNull(PrimitiveDataType.Int)]),
            new Key([DataValue.CreateString("A"), DataValue.CreateInteger(50)]),
            -1, // Expected: keyA < keyB
            "When first column is equal, NULL in the second column should sort before any value."
        },

        // Scenario 3: Symmetric check for scenario 2
        new object[] {
            new Key([DataValue.CreateString("A"), DataValue.CreateInteger(50)]),
            new Key([DataValue.CreateString("A"), DataValue.CreateNull(PrimitiveDataType.Int)]),
            1, // Expected: keyA > keyB
            "A non-null value in the second column should sort after a NULL."
        },

        // Scenario 4: Both keys have NULL in the same position
        new object[] {
            new Key([DataValue.CreateString("A"), DataValue.CreateNull(PrimitiveDataType.Int)]),
            new Key([DataValue.CreateString("A"), DataValue.CreateNull(PrimitiveDataType.Int)]),
            0, // Expected: keyA == keyB
            "Keys with matching values and NULLs in the same positions should be equal."
        }
      };

  [Theory]
  [MemberData(nameof(CompositeKeyWithNullsTestData))]
  public void Compare_WithCompositeKeyAndNulls_ReturnsCorrectOrder(Key keyA, Key keyB, int expectedSign, string explanation)
  {
    // Arrange
    var keyComparer = new KeyComparer();

    // Act
    int result = keyComparer.Compare(keyA, keyB);

    // Assert
    // We check the sign of the result (-1, 0, or 1) to match the expected outcome
    Assert.True(Math.Sign(expectedSign) == Math.Sign(result), explanation);
  }

  [Fact]
  public void Compare_WithMismatchedDataTypesInKeys_ThrowsInvalidOperationException()
  {
    // Arrange
    var keyComparer = new KeyComparer();

    // Create two keys where the first value has a different data type.
    // Key A has an Integer, Key B has a Varchar.
    var keyA = new Key([DataValue.CreateInteger(123)]);
    var keyB = new Key([DataValue.CreateString("123")]);

    // Act & Assert
    var ex = Assert.Throws<InvalidOperationException>(() => keyComparer.Compare(keyA, keyB));

    // Optional: Verify the exception message is helpful
    Assert.Contains("Found mismatched data types comparing keys", ex.Message);
    Assert.Contains("Cannot compare Int with Varchar", ex.Message);
  }
}