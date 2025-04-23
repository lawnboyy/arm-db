using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using ArmDb.SchemaDefinition; // Namespace of the classes under test

namespace ArmDb.Core.UnitTests.SchemaDefinition; // Example test namespace

public class TableDefinitionTests
{
  // --- Helper Data ---
  private static readonly DataTypeInfo IntType = new DataTypeInfo(PrimitiveDataType.Integer);
  private static readonly DataTypeInfo VarcharType = new DataTypeInfo(PrimitiveDataType.Varchar, 100);
  private static readonly ColumnDefinition IdCol = new ColumnDefinition("ID", IntType, isNullable: false);
  private static readonly ColumnDefinition NameCol = new ColumnDefinition("Name", VarcharType, isNullable: false);
  private static readonly ColumnDefinition EmailCol = new ColumnDefinition("Email", VarcharType, isNullable: true);
  private static readonly ColumnDefinition FkCol = new ColumnDefinition("CustomerID", IntType, isNullable: true); // Nullable FK col

  // --- Constructor Tests ---

  [Fact]
  public void Constructor_ValidName_InitializesProperties()
  {
    // Arrange
    string tableName = "Users";

    // Act
    var table = new TableDefinition(tableName);

    // Assert
    Assert.Equal(tableName, table.Name);
    Assert.NotNull(table.Columns);
    Assert.Empty(table.Columns);
    Assert.NotNull(table.Constraints);
    Assert.Empty(table.Constraints);
  }

  [Theory]
  [InlineData(null)]
  [InlineData("")]
  [InlineData("   ")]
  public void Constructor_InvalidName_ThrowsArgumentException(string? invalidName)
  {
    // Act & Assert
    Assert.Throws<ArgumentException>("name", () => new TableDefinition(invalidName!));
  }

  // --- AddColumn Tests ---

  [Fact]
  public void AddColumn_ValidColumn_AddsToListAndLookup()
  {
    // Arrange
    var table = new TableDefinition("Products");

    // Act
    table.AddColumn(IdCol);
    table.AddColumn(NameCol);

    // Assert
    Assert.Equal(2, table.Columns.Count);
    Assert.Contains(IdCol, table.Columns);
    Assert.Contains(NameCol, table.Columns);
    Assert.Same(IdCol, table.GetColumn("ID")); // Check lookup
    Assert.Same(NameCol, table.GetColumn("name")); // Check case-insensitive lookup
  }

  [Fact]
  public void AddColumn_NullColumn_ThrowsArgumentNullException()
  {
    // Arrange
    var table = new TableDefinition("Products");
    ColumnDefinition? nullColumn = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("column", () => table.AddColumn(nullColumn!));
  }

  [Fact]
  public void AddColumn_DuplicateName_ThrowsArgumentException()
  {
    // Arrange
    var table = new TableDefinition("Products");
    var idColLower = new ColumnDefinition("id", IntType); // Same name, different case
    table.AddColumn(IdCol); // Add "ID" first

    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("column", () => table.AddColumn(idColLower));
    Assert.Contains($"A column with the name '{idColLower.Name}' already exists", ex.Message);
  }

  // --- AddConstraint Tests ---

  [Fact]
  public void AddConstraint_ValidPrimaryKey_AddsConstraint()
  {
    // Arrange
    var table = new TableDefinition("Products");
    table.AddColumn(IdCol); // PK column must exist
    var pk = new PrimaryKeyConstraint(table.Name, new[] { IdCol.Name });

    // Act
    table.AddConstraint(pk);

    // Assert
    Assert.Single(table.Constraints);
    Assert.Contains(pk, table.Constraints);
    Assert.Same(pk, table.GetConstraint(pk.Name));
    Assert.Same(pk, table.GetPrimaryKeyConstraint());
  }

  [Fact]
  public void AddConstraint_ValidForeignKey_AddsConstraint()
  {
    // Arrange
    var table = new TableDefinition("Orders");
    table.AddColumn(FkCol); // Referencing column must exist
    var fk = new ForeignKeyConstraint(table.Name, new[] { FkCol.Name }, "Customers", new[] { "ID" }, "FK_Orders_Cust");

    // Act
    table.AddConstraint(fk);

    // Assert
    Assert.Single(table.Constraints);
    Assert.Contains(fk, table.Constraints);
    Assert.Same(fk, table.GetConstraint("fk_orders_cust")); // Case-insensitive lookup
    Assert.Single(table.GetForeignKeyConstraints());
    Assert.Same(fk, table.GetForeignKeyConstraints().First());
  }

  [Fact]
  public void AddConstraint_NullConstraint_ThrowsArgumentNullException()
  {
    // Arrange
    var table = new TableDefinition("Products");
    Constraint? nullConstraint = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("constraint", () => table.AddConstraint(nullConstraint!));
  }

  [Fact]
  public void AddConstraint_DuplicateName_ThrowsArgumentException()
  {
    // Arrange
    var table = new TableDefinition("Products");
    table.AddColumn(IdCol);
    var pk = new PrimaryKeyConstraint(table.Name, new[] { IdCol.Name }, "MyConstraintName");
    var pkDuplicateName = new PrimaryKeyConstraint(table.Name, new[] { IdCol.Name }, "myconstraintname"); // same name, different case
    table.AddConstraint(pk);

    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("constraint", () => table.AddConstraint(pkDuplicateName));
    Assert.Contains($"A constraint with the name '{pkDuplicateName.Name}' already exists", ex.Message);
  }

  [Fact]
  public void AddConstraint_SecondPrimaryKey_ThrowsInvalidOperationException()
  {
    // Arrange
    var table = new TableDefinition("Products");
    table.AddColumn(IdCol);
    table.AddColumn(NameCol);
    var pk1 = new PrimaryKeyConstraint(table.Name, new[] { IdCol.Name });
    var pk2 = new PrimaryKeyConstraint(table.Name, new[] { NameCol.Name }); // Different column, still second PK
    table.AddConstraint(pk1);

    // Act & Assert
    var ex = Assert.Throws<InvalidOperationException>(() => table.AddConstraint(pk2));
    Assert.Contains("already has a Primary Key defined", ex.Message);
  }

  [Fact]
  public void AddConstraint_PrimaryKeyColumnNotFound_ThrowsInvalidOperationException()
  {
    // Arrange
    var table = new TableDefinition("Products");
    // IdCol is NOT added to the table
    var pk = new PrimaryKeyConstraint(table.Name, new[] { IdCol.Name });

    // Act & Assert
    // Exception comes from pk.GetColumns(this) inside AddConstraint
    var ex = Assert.Throws<InvalidOperationException>(() => table.AddConstraint(pk));
    Assert.Contains($"Column '{IdCol.Name}' defined in primary key", ex.Message);
    Assert.Contains($"not found in table '{table.Name}'", ex.Message);
  }

  [Fact]
  public void AddConstraint_PrimaryKeyColumnNullable_ThrowsInvalidOperationException()
  {
    // Arrange
    var table = new TableDefinition("Products");
    // Add EmailCol which is nullable
    table.AddColumn(EmailCol);
    var pk = new PrimaryKeyConstraint(table.Name, new[] { EmailCol.Name }); // Try to use nullable col as PK

    // Act & Assert
    var ex = Assert.Throws<InvalidOperationException>(() => table.AddConstraint(pk));
    Assert.Contains($"Column '{EmailCol.Name}' used in Primary Key", ex.Message);
    Assert.Contains("must not be nullable", ex.Message);
  }

  [Fact]
  public void AddConstraint_ForeignKeyReferencingColumnNotFound_ThrowsInvalidOperationException()
  {
    // Arrange
    var table = new TableDefinition("Orders");
    // FkCol ("CustomerID") is NOT added
    var fk = new ForeignKeyConstraint(table.Name, new[] { FkCol.Name }, "Customers", new[] { "ID" });

    // Act & Assert
    // Exception comes from fk.GetReferencingColumns(this) inside AddConstraint
    var ex = Assert.Throws<InvalidOperationException>(() => table.AddConstraint(fk));
    Assert.Contains($"Column '{FkCol.Name}' defined in foreign key", ex.Message);
    Assert.Contains($"not found in referencing table '{table.Name}'", ex.Message);
  }

  // --- GetColumn Tests ---

  [Fact]
  public void GetColumn_ExistingColumn_ReturnsColumn()
  {
    // Arrange
    var table = new TableDefinition("Products");
    table.AddColumn(IdCol);

    // Act
    var result = table.GetColumn("ID");
    var resultCase = table.GetColumn("id");

    // Assert
    Assert.NotNull(result);
    Assert.Same(IdCol, result);
    Assert.NotNull(resultCase);
    Assert.Same(IdCol, resultCase);
  }

  [Fact]
  public void GetColumn_NonExistingColumn_ReturnsNull()
  {
    // Arrange
    var table = new TableDefinition("Products");
    table.AddColumn(IdCol);

    // Act
    var result = table.GetColumn("NonExistent");

    // Assert
    Assert.Null(result);
  }

  [Fact] // Test specifically for null
  public void GetColumn_NullName_ThrowsArgumentNullException()
  {
    // Arrange
    var table = new TableDefinition("Products");
    string? nullName = null;

    // Act & Assert
    // Expect ArgumentNullException specifically
    var ex = Assert.Throws<ArgumentNullException>("name", () => table.GetColumn(nullName!));
  }

  [Theory] // Test specifically for empty/whitespace
  [InlineData("")]
  [InlineData("  ")]
  [InlineData("\t")]
  public void GetColumn_EmptyOrWhitespaceName_ThrowsArgumentException(string invalidName)
  {
    // Arrange
    var table = new TableDefinition("Products");

    // Act & Assert
    // Expect ArgumentException specifically (and check param name)
    var ex = Assert.Throws<ArgumentException>("name", () => table.GetColumn(invalidName));
  }

  // --- GetConstraint Tests --- (Similar structure to GetColumn)

  [Fact]
  public void GetConstraint_ExistingConstraint_ReturnsConstraint()
  {
    // Arrange
    var table = new TableDefinition("Products");
    table.AddColumn(IdCol);
    var pk = new PrimaryKeyConstraint(table.Name, new[] { IdCol.Name }, "MyPK");
    table.AddConstraint(pk);

    // Act
    var result = table.GetConstraint("MyPK");
    var resultCase = table.GetConstraint("mypk");

    // Assert
    Assert.NotNull(result);
    Assert.Same(pk, result);
    Assert.NotNull(resultCase);
    Assert.Same(pk, resultCase);
  }

  [Fact]
  public void GetConstraint_NonExistingConstraint_ReturnsNull()
  {
    // Arrange
    var table = new TableDefinition("Products");

    // Act
    var result = table.GetConstraint("NonExistentConstraint");

    // Assert
    Assert.Null(result);
  }

  // --- GetPrimaryKeyConstraint / GetForeignKeyConstraints Tests ---

  [Fact]
  public void GetPrimaryKeyConstraint_WhenExists_ReturnsPK()
  {
    // Arrange
    var table = new TableDefinition("Products");
    table.AddColumn(IdCol);
    var pk = new PrimaryKeyConstraint(table.Name, new[] { IdCol.Name });
    table.AddConstraint(pk);

    // Act
    var result = table.GetPrimaryKeyConstraint();

    // Assert
    Assert.NotNull(result);
    Assert.Same(pk, result);
  }

  [Fact]
  public void GetPrimaryKeyConstraint_WhenNotExists_ReturnsNull()
  {
    // Arrange
    var table = new TableDefinition("Products");
    table.AddColumn(IdCol); // Has column but no PK constraint

    // Act
    var result = table.GetPrimaryKeyConstraint();

    // Assert
    Assert.Null(result);
  }

  [Fact]
  public void GetForeignKeyConstraints_WhenMultipleExist_ReturnsAll()
  {
    // Arrange
    var table = new TableDefinition("OrderLines");
    var col1 = new ColumnDefinition("OrderID", IntType);
    var col2 = new ColumnDefinition("ProductID", IntType);
    table.AddColumn(col1);
    table.AddColumn(col2);
    var fk1 = new ForeignKeyConstraint(table.Name, new[] { col1.Name }, "Orders", new[] { "ID" }, "FK_Order");
    var fk2 = new ForeignKeyConstraint(table.Name, new[] { col2.Name }, "Products", new[] { "ID" }, "FK_Product");
    table.AddConstraint(fk1);
    table.AddConstraint(fk2);

    // Act
    var result = table.GetForeignKeyConstraints().ToList();

    // Assert
    Assert.NotNull(result);
    Assert.Equal(2, result.Count);
    Assert.Contains(fk1, result);
    Assert.Contains(fk2, result);
  }

  [Fact]
  public void GetForeignKeyConstraints_WhenNoneExist_ReturnsEmpty()
  {
    // Arrange
    var table = new TableDefinition("Products");
    table.AddColumn(IdCol);
    var pk = new PrimaryKeyConstraint(table.Name, new[] { IdCol.Name });
    table.AddConstraint(pk); // Has PK but no FK

    // Act
    var result = table.GetForeignKeyConstraints().ToList();

    // Assert
    Assert.NotNull(result);
    Assert.Empty(result);
  }
}