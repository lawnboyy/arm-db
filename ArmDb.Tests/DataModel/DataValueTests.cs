using ArmDb.SchemaDefinition; // For PrimitiveDataType
using ArmDb.DataModel;          // For DataValue

namespace ArmDb.UnitTests.DataModel; // Example test namespace

public class DataValueTests
{
  // --- Static Factory Method Tests ---

  [Fact]
  public void CreateInteger_SetsCorrectProperties()
  {
    // Arrange
    long value = 12345L;
    // Act
    var dataValue = DataValue.CreateInteger(value);
    // Assert
    Assert.Equal(PrimitiveDataType.Integer, dataValue.DataType);
    Assert.False(dataValue.IsNull);
    Assert.Equal(value, dataValue.Value);
    Assert.IsType<long>(dataValue.Value);
  }

  [Fact]
  public void CreateString_SetsCorrectProperties()
  {
    // Arrange
    string value = "hello world";
    // Act
    var dataValue = DataValue.CreateString(value);
    // Assert
    Assert.Equal(PrimitiveDataType.Varchar, dataValue.DataType);
    Assert.False(dataValue.IsNull);
    Assert.Equal(value, dataValue.Value);
    Assert.IsType<string>(dataValue.Value);
  }

  [Fact]
  public void CreateString_NullInput_ThrowsArgumentNullException()
  {
    // Act & Assert
    Assert.Throws<ArgumentNullException>("value", () => DataValue.CreateString(null!));
  }

  [Fact]
  public void CreateBoolean_SetsCorrectProperties()
  {
    // Arrange
    bool value = true;
    // Act
    var dataValue = DataValue.CreateBoolean(value);
    // Assert
    Assert.Equal(PrimitiveDataType.Boolean, dataValue.DataType);
    Assert.False(dataValue.IsNull);
    Assert.Equal(value, dataValue.Value);
    Assert.IsType<bool>(dataValue.Value);
  }

  [Fact]
  public void CreateDecimal_SetsCorrectProperties()
  {
    // Arrange
    decimal value = 123.45M;
    // Act
    var dataValue = DataValue.CreateDecimal(value);
    // Assert
    Assert.Equal(PrimitiveDataType.Decimal, dataValue.DataType);
    Assert.False(dataValue.IsNull);
    Assert.Equal(value, dataValue.Value);
    Assert.IsType<decimal>(dataValue.Value);
  }

  [Fact]
  public void CreateDateTime_SetsCorrectProperties()
  {
    // Arrange
    // Use Utc time for consistency if possible
    DateTime value = new DateTime(2025, 4, 23, 19, 30, 0, DateTimeKind.Utc);
    // Act
    var dataValue = DataValue.CreateDateTime(value);
    // Assert
    Assert.Equal(PrimitiveDataType.DateTime, dataValue.DataType);
    Assert.False(dataValue.IsNull);
    Assert.Equal(value, dataValue.Value);
    Assert.IsType<DateTime>(dataValue.Value);
  }

  [Fact]
  public void CreateFloat_SetsCorrectProperties()
  {
    // Arrange
    double value = 123.456;
    // Act
    var dataValue = DataValue.CreateFloat(value);
    // Assert
    Assert.Equal(PrimitiveDataType.Float, dataValue.DataType);
    Assert.False(dataValue.IsNull);
    Assert.Equal(value, dataValue.Value);
    Assert.IsType<double>(dataValue.Value);
  }

  [Fact]
  public void CreateBlob_SetsCorrectProperties()
  {
    // Arrange
    byte[] value = { 0x01, 0x02, 0x03, 0x04 };
    // Act
    var dataValue = DataValue.CreateBlob(value);
    // Assert
    Assert.Equal(PrimitiveDataType.Blob, dataValue.DataType);
    Assert.False(dataValue.IsNull);
    Assert.Equal(value, dataValue.Value); // Reference equality is okay for byte[] here
    Assert.IsType<byte[]>(dataValue.Value);
  }

  [Fact]
  public void CreateBlob_NullInput_ThrowsArgumentNullException()
  {
    // Act & Assert
    Assert.Throws<ArgumentNullException>("value", () => DataValue.CreateBlob(null!));
  }

  [Theory]
  [InlineData(PrimitiveDataType.Integer)]
  [InlineData(PrimitiveDataType.Varchar)]
  [InlineData(PrimitiveDataType.Boolean)]
  [InlineData(PrimitiveDataType.Decimal)]
  [InlineData(PrimitiveDataType.DateTime)]
  [InlineData(PrimitiveDataType.Float)]
  [InlineData(PrimitiveDataType.Blob)]
  public void CreateNull_ValidType_SetsCorrectProperties(PrimitiveDataType type)
  {
    // Act
    var dataValue = DataValue.CreateNull(type);
    // Assert
    Assert.Equal(type, dataValue.DataType);
    Assert.True(dataValue.IsNull);
    Assert.Null(dataValue.Value);
  }

  [Fact]
  public void CreateNull_UnknownType_ThrowsArgumentException()
  {
    // Act & Assert
    Assert.Throws<ArgumentException>("type", () => DataValue.CreateNull(PrimitiveDataType.Unknown));
  }

  // --- GetAs<T> Tests ---

  [Fact]
  public void GetAs_CorrectType_ReturnsValue()
  {
    // Arrange
    var intVal = DataValue.CreateInteger(42L);
    var strVal = DataValue.CreateString("test");
    var boolVal = DataValue.CreateBoolean(true);
    var decVal = DataValue.CreateDecimal(1.23M);
    var dtVal = DataValue.CreateDateTime(DateTime.UtcNow);
    var floatVal = DataValue.CreateFloat(1.23);
    var blobVal = DataValue.CreateBlob(new byte[] { 1 });

    // Act & Assert
    Assert.Equal(42L, intVal.GetAs<long>());
    Assert.Equal("test", strVal.GetAs<string>());
    Assert.True(boolVal.GetAs<bool>());
    Assert.Equal(1.23M, decVal.GetAs<decimal>());
    Assert.Equal(dtVal.Value, dtVal.GetAs<DateTime>()); // Compare DateTime values
    Assert.Equal(1.23, floatVal.GetAs<double>());
    Assert.Equal(blobVal.Value, blobVal.GetAs<byte[]>()); // Compare byte[] values
  }

  [Fact]
  public void GetAs_NullValue_ThrowsInvalidOperationException()
  {
    // Arrange
    var nullInt = DataValue.CreateNull(PrimitiveDataType.Integer);
    // Act & Assert
    var ex = Assert.Throws<InvalidOperationException>(() => nullInt.GetAs<long>());
    Assert.Contains("represents SQL NULL", ex.Message);
  }

  [Fact]
  public void GetAs_WrongType_ThrowsInvalidCastException()
  {
    // Arrange
    var intVal = DataValue.CreateInteger(42L);
    var strVal = DataValue.CreateString("test");

    // Act & Assert
    Assert.Throws<InvalidCastException>(() => intVal.GetAs<string>());
    Assert.Throws<InvalidCastException>(() => intVal.GetAs<decimal>());
    Assert.Throws<InvalidCastException>(() => strVal.GetAs<long>());
  }

  // --- ToString() Tests ---

  [Fact]
  public void ToString_NullValue_ReturnsNULLString()
  {
    // Arrange
    var dv = DataValue.CreateNull(PrimitiveDataType.Integer);
    // Act & Assert
    Assert.Equal("NULL", dv.ToString());
  }

  [Theory]
  [InlineData(123L, "123")]
  [InlineData("hello", "hello")]
  [InlineData(true, "True")] // Default bool ToString
  [InlineData(123.45, "123.45")] // Default double ToString
  [InlineData(123.45e-10, "1.2345E-08")] // Default double ToString for sci notation
  public void ToString_NonNullValue_ReturnsValueToString(object value, string expected)
  {
    // Arrange
    DataValue dv;
    if (value is long l) dv = DataValue.CreateInteger(l);
    else if (value is string s) dv = DataValue.CreateString(s);
    else if (value is bool b) dv = DataValue.CreateBoolean(b);
    else if (value is double d) dv = DataValue.CreateFloat(d);
    // Add other types if specific formatting needed, e.g., Decimal, DateTime
    else throw new ArgumentException("Unsupported test type");

    // Act
    var result = dv.ToString();

    // Assert
    Assert.Equal(expected, result);
  }

  // --- Equality Tests ---

  public static IEnumerable<object[]> EqualityTestData =>
      new List<object[]>
      {
          // Equal Cases
          // new object[] { DataValue.CreateInteger(10), DataValue.CreateInteger(10), true },
          // new object[] { DataValue.CreateString("abc"), DataValue.CreateString("abc"), true },
          // new object[] { DataValue.CreateBoolean(true), DataValue.CreateBoolean(true), true },
          // new object[] { DataValue.CreateDecimal(1.0M), DataValue.CreateDecimal(1.0M), true },
          new object[] { DataValue.CreateBlob(new byte[]{1,2}), DataValue.CreateBlob(new byte[]{1,2}), true }, // NOTE: Compares array *content* due to object.Equals usage inside DataValue.Equals
          new object[] { DataValue.CreateNull(PrimitiveDataType.Integer), DataValue.CreateNull(PrimitiveDataType.Integer), true }, // Nulls of same type are equal

          // Unequal Cases
          new object[] { DataValue.CreateInteger(10), DataValue.CreateInteger(20), false }, // Different value
          new object[] { DataValue.CreateString("abc"), DataValue.CreateString("def"), false }, // Different value
          new object[] { DataValue.CreateBoolean(true), DataValue.CreateBoolean(false), false }, // Different value
          new object[] { DataValue.CreateInteger(10), DataValue.CreateString("10"), false }, // Different type
          new object[] { DataValue.CreateInteger(10), DataValue.CreateNull(PrimitiveDataType.Integer), false }, // Value vs Null
          new object[] { DataValue.CreateNull(PrimitiveDataType.Integer), DataValue.CreateNull(PrimitiveDataType.Varchar), false }, // Nulls of different types
          new object[] { DataValue.CreateBlob(new byte[]{1,2}), DataValue.CreateBlob(new byte[]{1,3}), false }, // Different blob content
      };

  [Theory]
  [MemberData(nameof(EqualityTestData))]
  public void Equals_And_Operators_ReturnCorrectly(DataValue v1, DataValue v2, bool expected)
  {
    // Assert
    Assert.Equal(expected, v1.Equals(v2));
    Assert.Equal(expected, v1.Equals((object)v2));
    Assert.Equal(expected, v1 == v2);
    Assert.Equal(!expected, v1 != v2);
  }

  [Fact]
  public void Equals_And_Operators_WithNullObjectReference_ReturnCorrectly()
  {
    // Arrange
    DataValue? v1 = DataValue.CreateInteger(1);
    DataValue? v2 = null;
    DataValue? v3 = null;

    // Act & Assert
    Assert.False(v1.Equals(v2));
    Assert.False(v1 == v2);
    Assert.True(v1 != v2);

    Assert.False(v2 == v1); // Check symmetry
    Assert.True(v2 != v1);

    Assert.True(v2 == v3); // Both null references are equal
    Assert.False(v2 != v3);
  }

  // --- GetHashCode Tests ---

  [Theory]
  [MemberData(nameof(EqualityTestData))]
  public void GetHashCode_ForEqualObjects_ReturnsSameCode(DataValue v1, DataValue v2, bool areEqual)
  {
    if (areEqual)
    {
      Assert.Equal(v1.GetHashCode(), v2.GetHashCode());
    }
    // Cannot assert that unequal objects have different hash codes due to collisions
  }

  [Fact]
  public void GetHashCode_ForDifferentTypeNulls_ReturnsDifferentCode()
  {
    // Arrange
    var nullInt = DataValue.CreateNull(PrimitiveDataType.Integer);
    var nullStr = DataValue.CreateNull(PrimitiveDataType.Varchar);

    // Assert (highly likely to be different, relies on HashCode.Combine behavior)
    Assert.NotEqual(nullInt.GetHashCode(), nullStr.GetHashCode());
  }
}