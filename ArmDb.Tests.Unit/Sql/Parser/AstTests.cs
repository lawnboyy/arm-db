using System.Collections.Generic;
using ArmDb.SchemaDefinition;
using ArmDb.Sql.Ast; // Assuming this namespace based on project structure
using Xunit;

namespace ArmDb.Tests.Unit.Sql.Parser;

public partial class AstTests
{
  [Fact]
  public void CreateDatabaseStatement_ConstructsCorrectly()
  {
    // SQL: CREATE DATABASE "mydb";

    // Arrange & Act
    string dbName = "mydb";
    var createDbStmt = new CreateDatabaseStatement(dbName);

    // Assert
    Assert.NotNull(createDbStmt);
    Assert.Equal(dbName, createDbStmt.DatabaseName);
  }

  [Fact]
  public void CreateTableStatement_SimpleStructure_ConstructsCorrectly()
  {
    // SQL: CREATE TABLE simple_users (id INT, username VARCHAR(255));

    // Arrange & Act
    // We are manually constructing the AST to verify the structure matches our design contract.
    // Later, the Parser tests will verify that "parsing string -> this structure" works.

    var tableName = new ObjectIdentifier("simple_users", "mydb");

    var columns = new List<ColumnDefinition>
        {
            new ColumnDefinition(
                "id",
                new DataTypeInfo(PrimitiveDataType.Int),
                isNullable: true // Default SQL nullable behavior if not specified
            ),
            new ColumnDefinition(
                "username",
                new DataTypeInfo(PrimitiveDataType.Varchar, 255),
                isNullable: true
            )
        };

    // No constraints for this simple example
    // Updated type from TableConstraint to Constraint based on user feedback
    var constraints = new List<Constraint>();

    var createStmt = new CreateTableStatement(tableName, columns, constraints);

    // Assert
    Assert.NotNull(createStmt);

    // Verify Table Name
    Assert.Equal("simple_users", createStmt.Table.Name);
    Assert.Equal("mydb", createStmt.Table.DatabaseName);

    // Verify Columns
    Assert.Equal(2, createStmt.Columns.Count);

    // Column 1: id
    Assert.Equal("id", createStmt.Columns[0].Name);
    Assert.Equal(PrimitiveDataType.Int, createStmt.Columns[0].DataType.PrimitiveType);

    // Column 2: username
    Assert.Equal("username", createStmt.Columns[1].Name);
    Assert.Equal(PrimitiveDataType.Varchar, createStmt.Columns[1].DataType.PrimitiveType);
    Assert.Equal(255, createStmt.Columns[1].DataType.MaxLength);
  }
}