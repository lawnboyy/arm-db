using ArmDb.Sql.Ast;
using ArmDb.Sql.Parser; // Assuming namespace based on folder structure

namespace ArmDb.Tests.Unit.Sql.Parser;

public partial class ParserTests
{
  [Fact]
  public void Parse_SelectStatement_Wildcard_ReturnsCorrectAst()
  {
    // Scenario: Simple SELECT * FROM table
    // Arrange
    var sql = "SELECT * FROM mydb.users";
    var parser = new SqlParser(sql);

    // Act
    var statement = parser.ParseStatement();

    // Assert
    Assert.NotNull(statement);
    var selectStmt = Assert.IsType<SelectStatement>(statement);

    // Verify From
    Assert.Equal("users", selectStmt.FromTable.Name);
    Assert.Equal("mydb", selectStmt.FromTable.DatabaseName);

    // Verify Columns
    Assert.Single(selectStmt.Columns);
    var col1 = selectStmt.Columns[0];
    Assert.Null(col1.Alias);

    // Expect WildcardExpression instead of ColumnExpression("*")
    Assert.IsType<WildcardExpression>(col1.Expression);

    // Verify Where
    Assert.Null(selectStmt.WhereClause);
  }

  [Fact]
  public void Parse_SelectStatement_SpecificColumns_ReturnsCorrectAst()
  {
    // Scenario: SELECT id, username FROM users
    // Arrange
    var sql = "SELECT id, username FROM mydb.users";
    var parser = new SqlParser(sql);

    // Act
    var statement = parser.ParseStatement();

    // Assert
    Assert.NotNull(statement);
    var selectStmt = Assert.IsType<SelectStatement>(statement);

    Assert.Equal("users", selectStmt.FromTable.Name);
    Assert.Equal("mydb", selectStmt.FromTable.DatabaseName);

    Assert.Equal(2, selectStmt.Columns.Count);

    // Col 1: id
    var col1 = Assert.IsType<ColumnExpression>(selectStmt.Columns[0].Expression);
    Assert.Equal("id", col1.Name);

    // Col 2: username
    var col2 = Assert.IsType<ColumnExpression>(selectStmt.Columns[1].Expression);
    Assert.Equal("username", col2.Name);
  }
}