using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using ArmDb.Core.SchemaDefinition; // Namespace of the classes under test

namespace ArmDb.Core.UnitTests.SchemaDefinition; // Example test namespace

public class PrimaryKeyConstraintTests
{
  // --- Helper Data ---
  private const string TableName = "Products";
  private static readonly string[] SingleColumn = { "ProductID" };
  private static readonly string[] MultiColumns = { "OrderID", "LineItem" };
  private static readonly DataTypeInfo IntType = new DataTypeInfo(PrimitiveDataType.Integer);
  private static readonly DataTypeInfo StringType = new DataTypeInfo(PrimitiveDataType.Varchar, 50);


  // --- Constructor and Property Tests ---

  [Fact]
  public void Constructor_ValidInputWithExplicitName_InitializesProperties()
  {
    // Arrange
    string pkName = "PK_Products_Custom";

    // Act
    var pk = new PrimaryKeyConstraint(TableName, SingleColumn, pkName);

    // Assert
    Assert.Equal(pkName, pk.Name);
    Assert.Equal(SingleColumn, pk.ColumnNames); // Checks content equality
  }

  [Fact]
  public void Constructor_ValidInputWithoutName_DefaultsName()
  {
    // Act
    var pk = new PrimaryKeyConstraint(TableName, MultiColumns);

    // Assert
    Assert.StartsWith($"PK_{TableName}_", pk.Name); // Check default name prefix
    Assert.Equal(MultiColumns, pk.ColumnNames);
  }

  [Fact]
  public void Constructor_ValidInputMultiColumn_InitializesProperties()
  {
    // Act
    var pk = new PrimaryKeyConstraint(TableName, MultiColumns);

    // Assert
    Assert.NotNull(pk.Name); // A name should be generated
    Assert.Equal(MultiColumns, pk.ColumnNames);
    Assert.Equal(2, pk.ColumnNames.Count);
  }

  [Theory]
  [InlineData(null)]
  [InlineData("")]
  [InlineData("  ")]
  public void Constructor_InvalidTableName_ThrowsArgumentException(string? invalidTableName)
  {
    // Act & Assert
    Assert.Throws<ArgumentException>("tableName", () => new PrimaryKeyConstraint(invalidTableName!, SingleColumn));
  }

  [Fact]
  public void Constructor_NullColumnNames_ThrowsArgumentNullException()
  {
    // Arrange
    IEnumerable<string>? nullCols = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("columnNames", () => new PrimaryKeyConstraint(TableName, nullCols!));
  }

  // --- Test Data Source for Invalid Column Lists (Reusing structure from FK tests) ---
  public static IEnumerable<object[]> InvalidPkColumnNameListsTestData =>
      new List<object[]>
      {
          // Contains invalid string content
          new object[] { new string?[] { null } },      // Contains null
          new object[] { new string[] { "" } },         // Contains empty string
          new object[] { new string[] { "  " } },       // Contains whitespace string
          new object[] { new string[] { "Valid", " " } },// Contains valid and whitespace
          // Empty list
          new object[] { Array.Empty<string>() },       // Empty array
          // Duplicate items
          new object[] { new string[] { "ColA", "ColA" } }, // Duplicate simple
          new object[] { new string[] { "ColA", "colA" } }, // Duplicate case-insensitive
          new object[] { new string[] { "ColA", "ColB", "colA"} } // Duplicate case-insensitive multi
      };

  [Theory]
  [MemberData(nameof(InvalidPkColumnNameListsTestData))]
  public void Constructor_InvalidColumnNames_ContentOrDuplicateOrEmpty_ThrowsArgumentException(IEnumerable<string?> invalidCols)
  {
    // Act & Assert
    Assert.Throws<ArgumentException>("columnNames", () => new PrimaryKeyConstraint(TableName, invalidCols!));
  }


  // --- GetColumns Method Tests ---

  private TableDefinition CreateMockTable(string name, params ColumnDefinition[] columns)
  {
    var table = new TableDefinition(name);
    foreach (var col in columns)
    {
      table.AddColumn(col);
    }
    return table;
  }

  [Fact]
  public void GetColumns_ValidTable_ReturnsColumnsInOrder()
  {
    // Arrange
    var col1 = new ColumnDefinition(MultiColumns[0], IntType);
    var col2 = new ColumnDefinition(MultiColumns[1], IntType);
    var otherCol = new ColumnDefinition("Notes", StringType);
    var table = CreateMockTable(TableName, col1, otherCol, col2); // Add columns in different order
    var pk = new PrimaryKeyConstraint(TableName, MultiColumns); // PK defined with specific order

    // Act
    var result = pk.GetColumns(table).ToList();

    // Assert
    Assert.NotNull(result);
    Assert.Equal(2, result.Count);
    Assert.Same(col1, result[0]); // Verify first PK column matches
    Assert.Same(col2, result[1]); // Verify second PK column matches (order maintained)
  }

  [Fact]
  public void GetColumns_ColumnNotFoundInTable_ThrowsInvalidOperationException()
  {
    // Arrange
    var col1 = new ColumnDefinition(MultiColumns[0], IntType);
    // Missing MultiColumns[1] ("LineItem")
    var table = CreateMockTable(TableName, col1);
    var pk = new PrimaryKeyConstraint(TableName, MultiColumns);

    // Act & Assert
    var ex = Assert.Throws<InvalidOperationException>(() => pk.GetColumns(table).ToList()); // Force enumeration
    Assert.Contains($"Column '{MultiColumns[1]}' defined in primary key '{pk.Name}' not found in table '{TableName}'", ex.Message);
  }

  [Fact]
  public void GetColumns_NullTable_ThrowsArgumentNullException()
  {
    // Arrange
    var pk = new PrimaryKeyConstraint(TableName, SingleColumn);
    TableDefinition? nullTable = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("table", () => pk.GetColumns(nullTable!).ToList()); // Force enumeration
  }
}