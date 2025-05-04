using ArmDb.SchemaDefinition; // Assuming DataTypeInfo is in this namespace

namespace ArmDb.UnitTests.SchemaDefinition; // Example test namespace

public class DataTypeInfoTests
{
  // ==================
  // Constructor Tests
  // ==================

  [Theory]
  [InlineData(PrimitiveDataType.Varchar, 100, null, null)] // Varchar valid
  [InlineData(PrimitiveDataType.Blob, 8000, null, null)]    // Blob valid
  [InlineData(PrimitiveDataType.Decimal, null, 10, 2)]   // Decimal valid P,S
  [InlineData(PrimitiveDataType.Decimal, null, 18, 0)]   // Decimal valid P, default S=0
  [InlineData(PrimitiveDataType.Decimal, null, 8, 8)]    // Decimal valid P=S
  [InlineData(PrimitiveDataType.Decimal, null, null, null)]// Decimal valid no P,S
  [InlineData(PrimitiveDataType.Int, null, null, null)] // Integer valid
  [InlineData(PrimitiveDataType.Boolean, null, null, null)] // Boolean valid
  [InlineData(PrimitiveDataType.DateTime, null, null, null)]// DateTime valid
  [InlineData(PrimitiveDataType.Float, null, null, null)]   // Float valid
  public void Constructor_ValidParameters_CreatesInstance(PrimitiveDataType type, int? maxLength, int? precision, int? scale)
  {
    // Act
    var exception = Record.Exception(() => new DataTypeInfo(type, maxLength, precision, scale));

    // Assert
    Assert.Null(exception); // No exception should be thrown for valid cases
  }

  [Fact]
  public void Constructor_ValidDecimalWithPrecisionOnly_DefaultsScaleToZero()
  {
    // Act
    var dataType = new DataTypeInfo(PrimitiveDataType.Decimal, null, 10, null);

    // Assert
    Assert.Equal(PrimitiveDataType.Decimal, dataType.PrimitiveType);
    Assert.Equal(10, dataType.Precision);
    Assert.Equal(0, dataType.Scale); // Verify Scale defaults to 0
    Assert.Null(dataType.MaxLength);
  }

  [Theory]
  [InlineData(PrimitiveDataType.Varchar)] // Varchar requires MaxLength
  [InlineData(PrimitiveDataType.Blob)]    // Blob requires MaxLength
  public void Constructor_MissingRequiredMaxLength_ThrowsArgumentException(PrimitiveDataType type)
  {
    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("maxLength", () => new DataTypeInfo(type, null));
    Assert.Contains("requires a MaxLength", ex.Message);
  }

  [Theory]
  [InlineData(PrimitiveDataType.Varchar, 0)]  // Varchar MaxLength must be positive
  [InlineData(PrimitiveDataType.Varchar, -1)]
  [InlineData(PrimitiveDataType.Blob, 0)]     // Blob MaxLength must be positive
  [InlineData(PrimitiveDataType.Blob, -100)]
  public void Constructor_InvalidMaxLengthValue_ThrowsArgumentOutOfRangeException(PrimitiveDataType type, int maxLength)
  {
    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("maxLength", () => new DataTypeInfo(type, maxLength));
  }

  [Theory]
  [InlineData(PrimitiveDataType.Varchar, 100, 10, null)] // Varchar cannot have Precision
  [InlineData(PrimitiveDataType.Varchar, 100, null, 2)]  // Varchar cannot have Scale
  [InlineData(PrimitiveDataType.Blob, 1000, 5, 1)]       // Blob cannot have Precision/Scale
  public void Constructor_ExtraneousPrecisionScale_ThrowsArgumentException(PrimitiveDataType type, int? maxLength, int? precision, int? scale)
  {
    // Act & Assert
    var ex = Assert.Throws<ArgumentException>(() => new DataTypeInfo(type, maxLength, precision, scale));
    Assert.Contains("cannot have Precision or Scale", ex.Message);
  }

  [Fact]
  public void Constructor_DecimalWithMaxLength_ThrowsArgumentException()
  {
    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("maxLength", () => new DataTypeInfo(PrimitiveDataType.Decimal, 100));
    Assert.Contains("cannot have MaxLength", ex.Message);
  }

  [Theory]
  [InlineData(0)]
  [InlineData(-5)]
  public void Constructor_DecimalInvalidPrecision_ThrowsArgumentOutOfRangeException(int precision)
  {
    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("precision", () => new DataTypeInfo(PrimitiveDataType.Decimal, null, precision, 0));
  }

  [Theory]
  [InlineData(-1)]
  [InlineData(-10)]
  public void Constructor_DecimalInvalidScale_ThrowsArgumentOutOfRangeException(int scale)
  {
    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("scale", () => new DataTypeInfo(PrimitiveDataType.Decimal, null, 10, scale));
  }

  [Fact]
  public void Constructor_DecimalScaleGreaterThanPrecision_ThrowsArgumentOutOfRangeException()
  {
    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("scale", () => new DataTypeInfo(PrimitiveDataType.Decimal, null, 5, 6));
  }

  [Fact]
  public void Constructor_DecimalScaleWithoutPrecision_ThrowsArgumentException()
  {
    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("scale", () => new DataTypeInfo(PrimitiveDataType.Decimal, null, null, 2));
    Assert.Contains("cannot be specified without Precision", ex.Message);
  }

  [Theory]
  [InlineData(PrimitiveDataType.Int)]
  [InlineData(PrimitiveDataType.Boolean)]
  [InlineData(PrimitiveDataType.DateTime)]
  [InlineData(PrimitiveDataType.Float)]
  public void Constructor_SimpleTypeWithParameters_ThrowsArgumentException(PrimitiveDataType type)
  {
    // Act & Assert
    Assert.Throws<ArgumentException>(() => new DataTypeInfo(type, 100));      // Test with MaxLength
    Assert.Throws<ArgumentException>(() => new DataTypeInfo(type, null, 10)); // Test with Precision
    Assert.Throws<ArgumentException>(() => new DataTypeInfo(type, null, null, 2)); // Test with Scale
  }

  [Fact]
  public void Constructor_UnknownPrimitiveType_ThrowsArgumentException()
  {
    // Act & Assert
    Assert.Throws<ArgumentException>("primitiveType", () => new DataTypeInfo(PrimitiveDataType.Unknown));
  }

  // ==================
  // Property Tests (covered implicitly by valid constructor tests, but explicit checks are fine)
  // ==================
  [Fact]
  public void Constructor_ValidVarchar_PropertiesAreSetCorrectly()
  {
    // Arrange
    var type = PrimitiveDataType.Varchar;
    int len = 150;

    // Act
    var dt = new DataTypeInfo(type, len);

    // Assert
    Assert.Equal(type, dt.PrimitiveType);
    Assert.Equal(len, dt.MaxLength);
    Assert.Null(dt.Precision);
    Assert.Null(dt.Scale);
  }

  // Add similar explicit property tests for Decimal, Integer etc. if desired

  // ==================
  // ToString() Tests
  // ==================

  [Theory]
  [InlineData(PrimitiveDataType.Varchar, 50, null, null, "VARCHAR(50)")]
  [InlineData(PrimitiveDataType.Blob, 4000, null, null, "BLOB(4000)")]
  [InlineData(PrimitiveDataType.Decimal, null, 10, 2, "DECIMAL(10, 2)")]
  [InlineData(PrimitiveDataType.Decimal, null, 18, 0, "DECIMAL(18, 0)")] // Scale 0 should be shown
  [InlineData(PrimitiveDataType.Decimal, null, null, null, "DECIMAL")] // No precision/scale
  [InlineData(PrimitiveDataType.Int, null, null, null, "INT")]
  [InlineData(PrimitiveDataType.Boolean, null, null, null, "BOOLEAN")]
  [InlineData(PrimitiveDataType.DateTime, null, null, null, "DATETIME")]
  [InlineData(PrimitiveDataType.Float, null, null, null, "FLOAT")]
  public void ToString_ReturnsCorrectFormat(PrimitiveDataType type, int? maxLen, int? prec, int? scale, string expected)
  {
    // Arrange
    var dataType = new DataTypeInfo(type, maxLen, prec, scale);

    // Act
    var result = dataType.ToString();

    // Assert
    Assert.Equal(expected, result);
  }

  // ==================
  // Equality Tests
  // ==================

  [Fact]
  public void Equals_SameInstance_ReturnsTrue()
  {
    // Arrange
    var dt1 = new DataTypeInfo(PrimitiveDataType.Int);

    // Act & Assert (using object.Equals)
    Assert.True(dt1.Equals((object)dt1));
  }

  [Fact]
  public void Equals_NullObject_ReturnsFalse()
  {
    // Arrange
    var dt1 = new DataTypeInfo(PrimitiveDataType.Int);

    // Act & Assert (using object.Equals)
    Assert.False(dt1.Equals((object?)null));
  }

  [Fact]
  public void Equals_DifferentType_ReturnsFalse()
  {
    // Arrange
    var dt1 = new DataTypeInfo(PrimitiveDataType.Int);
    var differentObject = "INT";

    // Act & Assert (using object.Equals)
    Assert.False(dt1.Equals(differentObject));
  }


  // Use Theory for multiple equality scenarios
  public static IEnumerable<object[]> EqualityTestData =>
    new List<object[]>
    {
        // Equal cases
        new object[] { new DataTypeInfo(PrimitiveDataType.Int), new DataTypeInfo(PrimitiveDataType.Int), true },
        new object[] { new DataTypeInfo(PrimitiveDataType.Varchar, 100), new DataTypeInfo(PrimitiveDataType.Varchar, 100), true },
        new object[] { new DataTypeInfo(PrimitiveDataType.Decimal, null, 12, 3), new DataTypeInfo(PrimitiveDataType.Decimal, null, 12, 3), true },
        new object[] { new DataTypeInfo(PrimitiveDataType.Decimal, null, 10, 0), new DataTypeInfo(PrimitiveDataType.Decimal, null, 10, null), true }, // Scale defaults to 0
        new object[] { new DataTypeInfo(PrimitiveDataType.Decimal), new DataTypeInfo(PrimitiveDataType.Decimal), true }, // Both default

        // Unequal cases - Different Type
        new object[] { new DataTypeInfo(PrimitiveDataType.Int), new DataTypeInfo(PrimitiveDataType.Float), false },

        // Unequal cases - Same Type, Different Params
        new object[] { new DataTypeInfo(PrimitiveDataType.Varchar, 100), new DataTypeInfo(PrimitiveDataType.Varchar, 200), false },
        new object[] { new DataTypeInfo(PrimitiveDataType.Decimal, null, 12, 3), new DataTypeInfo(PrimitiveDataType.Decimal, null, 10, 3), false }, // Diff Precision
        new object[] { new DataTypeInfo(PrimitiveDataType.Decimal, null, 12, 3), new DataTypeInfo(PrimitiveDataType.Decimal, null, 12, 4), false }, // Diff Scale
        new object[] { new DataTypeInfo(PrimitiveDataType.Decimal, null, 10, null), new DataTypeInfo(PrimitiveDataType.Decimal, null, 10, 1), false }, // Default vs explicit scale

        // Null comparison handled by operator tests
    };

  [Theory]
  [MemberData(nameof(EqualityTestData))]
  public void Equals_DataTypeInfoSpecific_ReturnsCorrectly(DataTypeInfo dt1, DataTypeInfo dt2, bool expected)
  {
    // Act & Assert
    Assert.Equal(expected, dt1.Equals(dt2));
  }

  [Theory]
  [MemberData(nameof(EqualityTestData))]
  public void EqualityOperators_ReturnCorrectly(DataTypeInfo dt1, DataTypeInfo dt2, bool expected)
  {
    // Act & Assert
    Assert.Equal(expected, dt1 == dt2);
    Assert.Equal(!expected, dt1 != dt2);
  }

  [Fact]
  public void EqualityOperators_WithNull_ReturnCorrectly()
  {
    // Arrange
    DataTypeInfo? dt1 = new DataTypeInfo(PrimitiveDataType.Int);
    DataTypeInfo? dt2 = null;
    DataTypeInfo? dt3 = null;

    // Act & Assert
    Assert.False(dt1 == dt2);
    Assert.True(dt1 != dt2);
    Assert.False(dt2 == dt1);
    Assert.True(dt2 != dt1);
    Assert.True(dt2 == dt3);
    Assert.False(dt2 != dt3);
  }


  // ==================
  // GetHashCode Tests
  // ==================

  [Theory]
  [MemberData(nameof(EqualityTestData))]
  public void GetHashCode_ForEqualObjects_ReturnsSameCode(DataTypeInfo dt1, DataTypeInfo dt2, bool areEqual)
  {
    if (areEqual)
    {
      // Assert
      Assert.Equal(dt1.GetHashCode(), dt2.GetHashCode());
    }
    // else - Hash codes are not guaranteed to be different for unequal objects, so no assertion here.
  }
}