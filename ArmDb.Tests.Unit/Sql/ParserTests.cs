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

  [Fact]
  public void Parse_CreateTable_AllTypes_ReturnsCorrectAst()
  {
    // Arrange
    var sql = @"
      CREATE TABLE all_types (
        col_int INT,
        col_bigint BIGINT,
        col_varchar VARCHAR(100),
        col_bool BOOLEAN,
        col_decimal DECIMAL(10, 2),
        col_datetime DATETIME,
        col_float FLOAT,
        col_blob BLOB(1024)
      )";
    var parser = new SqlParser(sql);

    // Act
    var statement = parser.ParseStatement();

    // Assert
    Assert.NotNull(statement);
    var createTableStmt = Assert.IsType<CreateTableStatement>(statement);

    Assert.Equal("all_types", createTableStmt.Table.Name);
    Assert.Equal(8, createTableStmt.Columns.Count);

    // 1. INT
    Assert.Equal("col_int", createTableStmt.Columns[0].Name);
    Assert.Equal(PrimitiveDataType.Int, createTableStmt.Columns[0].DataType.PrimitiveType);

    // 2. BIGINT
    Assert.Equal("col_bigint", createTableStmt.Columns[1].Name);
    Assert.Equal(PrimitiveDataType.BigInt, createTableStmt.Columns[1].DataType.PrimitiveType);

    // 3. VARCHAR(100)
    Assert.Equal("col_varchar", createTableStmt.Columns[2].Name);
    Assert.Equal(PrimitiveDataType.Varchar, createTableStmt.Columns[2].DataType.PrimitiveType);
    Assert.Equal(100, createTableStmt.Columns[2].DataType.MaxLength);

    // 4. BOOLEAN
    Assert.Equal("col_bool", createTableStmt.Columns[3].Name);
    Assert.Equal(PrimitiveDataType.Boolean, createTableStmt.Columns[3].DataType.PrimitiveType);

    // 5. DECIMAL(10, 2)
    Assert.Equal("col_decimal", createTableStmt.Columns[4].Name);
    Assert.Equal(PrimitiveDataType.Decimal, createTableStmt.Columns[4].DataType.PrimitiveType);
    Assert.Equal(10, createTableStmt.Columns[4].DataType.Precision);
    Assert.Equal(2, createTableStmt.Columns[4].DataType.Scale);

    // 6. DATETIME
    Assert.Equal("col_datetime", createTableStmt.Columns[5].Name);
    Assert.Equal(PrimitiveDataType.DateTime, createTableStmt.Columns[5].DataType.PrimitiveType);

    // 7. FLOAT
    Assert.Equal("col_float", createTableStmt.Columns[6].Name);
    Assert.Equal(PrimitiveDataType.Float, createTableStmt.Columns[6].DataType.PrimitiveType);

    // 8. BLOB(1024)
    Assert.Equal("col_blob", createTableStmt.Columns[7].Name);
    Assert.Equal(PrimitiveDataType.Blob, createTableStmt.Columns[7].DataType.PrimitiveType);
    Assert.Equal(1024, createTableStmt.Columns[7].DataType.MaxLength);
  }
}