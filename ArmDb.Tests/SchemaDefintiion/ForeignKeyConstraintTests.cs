using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using ArmDb.SchemaDefinition; // Namespace of the classes under test

namespace ArmDb.Core.UnitTests.SchemaDefinition; // Example test namespace

public class ForeignKeyConstraintTests
{
  // --- Helper Data ---
  private const string ReferencingTable = "Orders";
  private const string ReferencedTable = "Customers";
  private static readonly string[] ReferencingCols = { "CustomerID" };
  private static readonly string[] ReferencedCols = { "ID" };
  private static readonly string[] ReferencingColsMulti = { "OrderID", "ProductSku" };
  private static readonly string[] ReferencedColsMulti = { "ID", "Sku" };
  private static readonly DataTypeInfo IntType = new DataTypeInfo(PrimitiveDataType.Integer);
  private static readonly DataTypeInfo VarcharType = new DataTypeInfo(PrimitiveDataType.Varchar, 50);

  // --- Constructor and Property Tests ---

  [Fact]
  public void Constructor_ValidInput_InitializesPropertiesCorrectly()
  {
    // Arrange
    string fkName = "FK_Orders_Customers";
    var onUpdate = ReferentialAction.Cascade;
    var onDelete = ReferentialAction.SetNull;

    // Act
    var fk = new ForeignKeyConstraint(
        ReferencingTable, ReferencingCols,
        ReferencedTable, ReferencedCols,
        fkName, onUpdate, onDelete);

    // Assert
    Assert.Equal(fkName, fk.Name);
    Assert.Equal(ReferencingCols, fk.ReferencingColumnNames); // Checks content equality for arrays/lists
    Assert.Equal(ReferencedTable, fk.ReferencedTableName);
    Assert.Equal(ReferencedCols, fk.ReferencedColumnNames);
    Assert.Equal(onUpdate, fk.OnUpdateAction);
    Assert.Equal(onDelete, fk.OnDeleteAction);
  }

  [Fact]
  public void Constructor_MinimalValidInput_DefaultsNameAndActions()
  {
    // Act
    var fk = new ForeignKeyConstraint(
        ReferencingTable, ReferencingCols,
        ReferencedTable, ReferencedCols);

    // Assert
    Assert.StartsWith($"FK_{ReferencingTable}_", fk.Name); // Default name format check
    Assert.Equal(ReferencingCols, fk.ReferencingColumnNames);
    Assert.Equal(ReferencedTable, fk.ReferencedTableName);
    Assert.Equal(ReferencedCols, fk.ReferencedColumnNames);
    Assert.Equal(ReferentialAction.NoAction, fk.OnUpdateAction); // Default action
    Assert.Equal(ReferentialAction.NoAction, fk.OnDeleteAction); // Default action
  }

  [Theory]
  [InlineData(null)]
  [InlineData("")]
  [InlineData("  ")]
  public void Constructor_InvalidReferencingTableName_ThrowsArgumentException(string? invalidName)
  {
    // Act & Assert
    Assert.Throws<ArgumentException>("referencingTableName", () => new ForeignKeyConstraint(
        invalidName!, ReferencingCols, ReferencedTable, ReferencedCols));
  }

  // --- Test Data Source for Invalid Column Lists ---
  public static IEnumerable<object[]> InvalidColumnNameListsTestData =>
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
  [MemberData(nameof(InvalidColumnNameListsTestData))]
  public void Constructor_InvalidReferencingColumnNames_ContentOrDuplicateOrEmpty_ThrowsArgumentException(IEnumerable<string?> invalidCols)
  {
    // Act & Assert
    // The specific exception might be ArgumentException for content/empty/duplicate issues.
    Assert.Throws<ArgumentException>("referencingColumnNames", () => new ForeignKeyConstraint(
        ReferencingTable, invalidCols!, ReferencedTable, ReferencedCols));
  }

  // Separate test for null collection itself
  [Fact]
  public void Constructor_NullReferencingColumnNames_ThrowsArgumentNullException()
  {
    // Arrange
    IEnumerable<string>? nullCols = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("referencingColumnNames", () => new ForeignKeyConstraint(
        ReferencingTable, nullCols!, ReferencedTable, ReferencedCols));
  }


  [Theory]
  [InlineData(null)]
  [InlineData("")]
  [InlineData("  ")]
  public void Constructor_InvalidReferencedTableName_ThrowsArgumentException(string? invalidName)
  {
    // Act & Assert
    Assert.Throws<ArgumentException>("referencedTableName", () => new ForeignKeyConstraint(
        ReferencingTable, ReferencingCols, invalidName!, ReferencedCols));
  }

  // Use MemberData for referenced columns too
  [Theory]
  [MemberData(nameof(InvalidColumnNameListsTestData))]
  public void Constructor_InvalidReferencedColumnNames_ContentOrDuplicateOrEmpty_ThrowsArgumentException(IEnumerable<string?> invalidCols)
  {
    // Act & Assert
    Assert.Throws<ArgumentException>("referencedColumnNames", () => new ForeignKeyConstraint(
        ReferencingTable, ReferencingCols, ReferencedTable, invalidCols!));
  }

  // Separate test for null collection itself
  [Fact]
  public void Constructor_NullReferencedColumnNames_ThrowsArgumentNullException()
  {
    // Arrange
    IEnumerable<string>? nullCols = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("referencedColumnNames", () => new ForeignKeyConstraint(
        ReferencingTable, ReferencingCols, ReferencedTable, nullCols!));
  }


  [Fact]
  public void Constructor_MismatchedColumnCount_ThrowsArgumentException()
  {
    // Arrange
    var refing = new[] { "Col1" };
    var refed = new[] { "ID1", "ID2" }; // Different count

    // Act & Assert
    // Check might happen when comparing counts, relating to either param name potentially
    var ex = Assert.Throws<ArgumentException>(() => new ForeignKeyConstraint(
        ReferencingTable, refing, ReferencedTable, refed));
    Assert.Contains("number of referencing columns must match", ex.Message);
  }

  // --- Helper Method Tests (Using Mock/Stub Objects) ---

  private TableDefinition CreateMockTable(string name, params ColumnDefinition[] columns)
  {
    var table = new TableDefinition(name);
    foreach (var col in columns)
    {
      table.AddColumn(col);
    }
    return table;
  }

  private DatabaseSchema CreateMockSchema(string name, params TableDefinition[] tables)
  {
    var schema = new DatabaseSchema(name);
    foreach (var tbl in tables)
    {
      schema.AddTable(tbl);
    }
    return schema;
  }

  [Fact]
  public void GetReferencingColumns_ValidTable_ReturnsColumns()
  {
    // Arrange
    var col1 = new ColumnDefinition("CustomerID", IntType);
    var col2 = new ColumnDefinition("Notes", VarcharType);
    var table = CreateMockTable(ReferencingTable, col1, col2);
    var fk = new ForeignKeyConstraint(ReferencingTable, new[] { "CustomerID" }, ReferencedTable, ReferencedCols);

    // Act
    var result = fk.GetReferencingColumns(table).ToList();

    // Assert
    Assert.Single(result);
    Assert.Same(col1, result[0]);
  }

  [Fact]
  public void GetReferencingColumns_ColumnNotFound_ThrowsInvalidOperationException()
  {
    // Arrange
    var col1 = new ColumnDefinition("SomeOtherID", IntType);
    var table = CreateMockTable(ReferencingTable, col1);
    var fk = new ForeignKeyConstraint(ReferencingTable, new[] { "CustomerID" }, ReferencedTable, ReferencedCols); // FK references CustomerID

    // Act & Assert
    var ex = Assert.Throws<InvalidOperationException>(() => fk.GetReferencingColumns(table).ToList()); // Call ToList to force execution
    Assert.Contains("Column 'CustomerID' defined in foreign key", ex.Message);
    Assert.Contains($"not found in referencing table '{ReferencingTable}'", ex.Message);
  }

  [Fact]
  public void GetReferencedTable_ValidSchema_ReturnsTable()
  {
    // Arrange
    var refingTable = CreateMockTable(ReferencingTable, new ColumnDefinition("CustomerID", IntType));
    var refedTable = CreateMockTable(ReferencedTable, new ColumnDefinition("ID", IntType));
    var schema = CreateMockSchema("TestDB", refingTable, refedTable);
    var fk = new ForeignKeyConstraint(ReferencingTable, ReferencingCols, ReferencedTable, ReferencedCols);

    // Act
    var result = fk.GetReferencedTable(schema);

    // Assert
    Assert.NotNull(result);
    Assert.Same(refedTable, result);
  }

  [Fact]
  public void GetReferencedTable_TableNotFound_ThrowsInvalidOperationException()
  {
    // Arrange
    var refingTable = CreateMockTable(ReferencingTable, new ColumnDefinition("CustomerID", IntType));
    var schema = CreateMockSchema("TestDB", refingTable); // Does *not* contain ReferencedTable
    var fk = new ForeignKeyConstraint(ReferencingTable, ReferencingCols, ReferencedTable, ReferencedCols);

    // Act & Assert
    var ex = Assert.Throws<InvalidOperationException>(() => fk.GetReferencedTable(schema));
    Assert.Contains($"Referenced table '{ReferencedTable}' specified in foreign key", ex.Message);
    Assert.Contains($"not found in the database schema 'TestDB'", ex.Message);
  }

  [Fact]
  public void GetReferencedColumns_ValidSchema_ReturnsColumns()
  {
    // Arrange
    var refingCol = new ColumnDefinition("CustomerID", IntType);
    var refingTable = CreateMockTable(ReferencingTable, refingCol);
    var refedCol = new ColumnDefinition("ID", IntType); // Matching name and type
    var refedTable = CreateMockTable(ReferencedTable, refedCol);
    var schema = CreateMockSchema("TestDB", refingTable, refedTable);
    var fk = new ForeignKeyConstraint(ReferencingTable, new[] { refingCol.Name }, ReferencedTable, new[] { refedCol.Name });

    // Act
    var result = fk.GetReferencedColumns(schema).ToList();

    // Assert
    Assert.Single(result);
    Assert.Same(refedCol, result[0]);
  }

  [Fact]
  public void GetReferencedColumns_ColumnNotFound_ThrowsInvalidOperationException()
  {
    // Arrange
    var refingTable = CreateMockTable(ReferencingTable, new ColumnDefinition("CustomerID", IntType));
    var refedTable = CreateMockTable(ReferencedTable, new ColumnDefinition("SomeOtherID", IntType)); // Referenced column name mismatch
    var schema = CreateMockSchema("TestDB", refingTable, refedTable);
    var fk = new ForeignKeyConstraint(ReferencingTable, ReferencingCols, ReferencedTable, ReferencedCols); // Uses "ID"

    // Act & Assert
    var ex = Assert.Throws<InvalidOperationException>(() => fk.GetReferencedColumns(schema).ToList()); // Call ToList
    Assert.Contains($"Referenced column '{ReferencedCols[0]}' specified in foreign key", ex.Message);
    Assert.Contains($"not found in referenced table '{ReferencedTable}'", ex.Message);
  }
}