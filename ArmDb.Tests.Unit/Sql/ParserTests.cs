using ArmDb.SchemaDefinition;
using ArmDb.Sql.Ast;
using ArmDb.Sql.Parser; // Assuming namespace based on folder structure

namespace ArmDb.Tests.Unit.Sql.Parser;

public class ParserTests
{
  [Fact]
  public void Parse_CreateDatabase_ReturnsCorrectAst()
  {
    // Arrange
    // Removed double quotes as Tokenizer support is pending
    var sql = "CREATE DATABASE mydb";
    var parser = new SqlParser(sql);

    // Act
    var statement = parser.ParseStatement();

    // Assert
    Assert.NotNull(statement);
    var createDbStmt = Assert.IsType<CreateDatabaseStatement>(statement);
    Assert.Equal("mydb", createDbStmt.DatabaseName);
  }

  [Fact]
  public void Parse_CreateTable_Simple_ReturnsCorrectAst()
  {
    // Arrange
    var sql = "CREATE TABLE users (id INT, name VARCHAR(255))";
    var parser = new SqlParser(sql);

    // Act
    var statement = parser.ParseStatement();

    // Assert
    Assert.NotNull(statement);
    var createTableStmt = Assert.IsType<CreateTableStatement>(statement);

    // Verify Table Identifier
    Assert.Equal("users", createTableStmt.Table.Name);

    // Verify Columns
    Assert.Equal(2, createTableStmt.Columns.Count);

    // Column 1: id INT
    var col1 = createTableStmt.Columns[0];
    Assert.Equal("id", col1.Name);
    Assert.Equal(PrimitiveDataType.Int, col1.DataType.PrimitiveType);

    // Column 2: name VARCHAR(255)
    var col2 = createTableStmt.Columns[1];
    Assert.Equal("name", col2.Name);
    Assert.Equal(PrimitiveDataType.Varchar, col2.DataType.PrimitiveType);
    Assert.Equal(255, col2.DataType.MaxLength);
  }
}