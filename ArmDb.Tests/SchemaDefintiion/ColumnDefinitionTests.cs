using ArmDb.SchemaDefinition; // Namespace of the classes under test

namespace ArmDb.UnitTests.SchemaDefinition; // Example test namespace

public class ColumnDefinitionTests
{
  // Helper reusable DataTypeInfo instances for tests
  private static readonly DataTypeInfo IntType = new DataTypeInfo(PrimitiveDataType.Int);
  private static readonly DataTypeInfo VarcharType = new DataTypeInfo(PrimitiveDataType.Varchar, 100);
  private static readonly DataTypeInfo DecimalType = new DataTypeInfo(PrimitiveDataType.Decimal, null, 10, 2);

  // ==================
  // Constructor Tests - Valid Cases & Property Initialization
  // ==================

  [Fact]
  public void Constructor_ValidMinimalInput_InitializesPropertiesWithDefaults()
  {
    // Arrange
    string columnName = "UserID";
    var dataType = IntType;

    // Act
    var columnDef = new ColumnDefinition(columnName, dataType);

    // Assert
    Assert.Equal(columnName, columnDef.Name);
    Assert.Same(dataType, columnDef.DataType); // Verify same instance
    Assert.True(columnDef.IsNullable); // Default is true
    Assert.Null(columnDef.DefaultValueExpression); // Default is null
  }

  [Fact]
  public void Constructor_ValidFullInput_InitializesProperties()
  {
    // Arrange
    string columnName = "ProductName";
    var dataType = VarcharType;
    bool isNullable = false;
    string defaultValue = "'Default Product'";

    // Act
    var columnDef = new ColumnDefinition(columnName, dataType, isNullable, defaultValue);

    // Assert
    Assert.Equal(columnName, columnDef.Name);
    Assert.Same(dataType, columnDef.DataType);
    Assert.False(columnDef.IsNullable);
    Assert.Equal(defaultValue, columnDef.DefaultValueExpression);
  }

  [Fact]
  public void Constructor_ValidInputWithSpaces_TrimsNameAndDefaultExpression()
  {
    // Arrange
    string columnNameWithSpaces = "  EmailAddress  ";
    string expectedName = "EmailAddress";
    var dataType = VarcharType;
    string defaultValueWithSpaces = "  'N/A'  ";
    string expectedDefaultValue = "'N/A'";

    // Act
    var columnDef = new ColumnDefinition(columnNameWithSpaces, dataType, true, defaultValueWithSpaces);

    // Assert
    Assert.Equal(expectedName, columnDef.Name);
    Assert.Equal(expectedDefaultValue, columnDef.DefaultValueExpression);
  }

  [Fact]
  public void Constructor_ValidInputWithNullDefault_SetsDefaultExpressionToNull()
  {
    // Arrange
    string columnName = "LastModified";
    var dataType = new DataTypeInfo(PrimitiveDataType.DateTime);

    // Act
    var columnDef = new ColumnDefinition(columnName, dataType, false, null); // Explicitly pass null

    // Assert
    Assert.Equal(columnName, columnDef.Name);
    Assert.Same(dataType, columnDef.DataType);
    Assert.False(columnDef.IsNullable);
    Assert.Null(columnDef.DefaultValueExpression);
  }


  // ==================
  // Constructor Tests - Invalid Cases (Exceptions)
  // ==================

  [Theory]
  [InlineData(null)]
  [InlineData("")]
  [InlineData("   ")]
  [InlineData("\t")]
  public void Constructor_InvalidName_ThrowsArgumentException(string? invalidName)
  {
    // Arrange
    var dataType = IntType;

    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("name", () => new ColumnDefinition(invalidName!, dataType));
    Assert.Contains("Column name cannot be null or whitespace", ex.Message); // Check message content
  }

  [Fact]
  public void Constructor_NullDataType_ThrowsArgumentNullException()
  {
    // Arrange
    string columnName = "ValidName";
    DataTypeInfo? nullDataType = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("dataType", () => new ColumnDefinition(columnName, nullDataType!)); // Use ! to satisfy compiler, test checks null
  }

  [Theory]
  [InlineData("")] // Empty string
  [InlineData("   ")] // Whitespace only
  [InlineData("\t")] // Tab only
  public void Constructor_InvalidDefaultValueExpression_ThrowsArgumentException(string invalidDefaultExpr)
  {
    // Arrange
    string columnName = "ValidName";
    var dataType = IntType;

    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("defaultValueExpression", () => new ColumnDefinition(columnName, dataType, true, invalidDefaultExpr));
    Assert.Contains("cannot be empty or whitespace", ex.Message);
  }
}