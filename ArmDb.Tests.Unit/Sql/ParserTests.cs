using ArmDb.SchemaDefinition;
using ArmDb.Sql.Ast;
using ArmDb.Sql.Parser; // Assuming namespace based on folder structure

namespace ArmDb.Tests.Unit.Sql.Parser;

public partial class ParserTests
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

  [Fact]
  public void Parse_InsertStatement_ReturnsCorrectAst()
  {
    // Arrange
    var sql = @"
      INSERT INTO mydb.users(
        email, first_name, last_name, is_onboarded, last_updated)
      VALUES ('bob@mail.com', 'Bob', 'Whiley', true, '11-18-2025')";
    var parser = new SqlParser(sql);

    // Act
    var statement = parser.ParseStatement();

    // Assert
    Assert.NotNull(statement);
    var insertStmt = Assert.IsType<InsertStatement>(statement);

    // Verify Table
    Assert.Equal("users", insertStmt.Table.Name);
    Assert.Equal("mydb", insertStmt.Table.DatabaseName);

    // Verify Columns
    Assert.Equal(5, insertStmt.Columns.Count);
    Assert.Equal("email", insertStmt.Columns[0]);
    Assert.Equal("first_name", insertStmt.Columns[1]);
    Assert.Equal("last_name", insertStmt.Columns[2]);
    Assert.Equal("is_onboarded", insertStmt.Columns[3]);
    Assert.Equal("last_updated", insertStmt.Columns[4]);

    // Verify Values
    Assert.Equal(5, insertStmt.Values.Count);

    var val1 = Assert.IsType<LiteralExpression>(insertStmt.Values[0]);
    Assert.Equal("bob@mail.com", val1.Value);
    Assert.Equal(PrimitiveDataType.Varchar, val1.Type);

    var val2 = Assert.IsType<LiteralExpression>(insertStmt.Values[1]);
    Assert.Equal("Bob", val2.Value);

    var val3 = Assert.IsType<LiteralExpression>(insertStmt.Values[2]);
    Assert.Equal("Whiley", val3.Value);

    var val4 = Assert.IsType<LiteralExpression>(insertStmt.Values[3]);
    Assert.Equal(true, val4.Value);
    Assert.Equal(PrimitiveDataType.Boolean, val4.Type);

    var val5 = Assert.IsType<LiteralExpression>(insertStmt.Values[4]);
    Assert.Equal("11-18-2025", val5.Value);
    Assert.Equal(PrimitiveDataType.Varchar, val5.Type);
  }

  [Fact]
  public void Parse_InsertStatement_WithBigInt_ReturnsCorrectAst()
  {
    // Arrange
    // Value 3000000000 exceeds Int32.MaxValue (2,147,483,647)
    var sql = "INSERT INTO mydb.logs (id, message) VALUES (3000000000, 'Log Entry')";
    var parser = new SqlParser(sql);

    // Act
    var statement = parser.ParseStatement();

    // Assert
    Assert.NotNull(statement);
    var insertStmt = Assert.IsType<InsertStatement>(statement);

    Assert.Equal(2, insertStmt.Values.Count);

    // Verify BigInt detection
    // The parser should automatically detect that 3000000000 > Int32.MaxValue and use BigInt
    var val1 = Assert.IsType<LiteralExpression>(insertStmt.Values[0]);
    Assert.Equal(3000000000L, val1.Value);
    Assert.Equal(PrimitiveDataType.BigInt, val1.Type);

    // Verify String
    var val2 = Assert.IsType<LiteralExpression>(insertStmt.Values[1]);
    Assert.Equal("Log Entry", val2.Value);
    Assert.Equal(PrimitiveDataType.Varchar, val2.Type);
  }

  [Fact]
  public void Parse_InsertStatement_NoColumns_ReturnsCorrectAst()
  {
    // Scenario: INSERT INTO table VALUES (...) without specifying column names.
    // Arrange
    var sql = "INSERT INTO mydb.users VALUES (1, 'user')";
    var parser = new SqlParser(sql);

    // Act
    var statement = parser.ParseStatement();

    // Assert
    Assert.NotNull(statement);
    var insertStmt = Assert.IsType<InsertStatement>(statement);

    Assert.Empty(insertStmt.Columns); // Should be empty list
    Assert.Equal(2, insertStmt.Values.Count);
  }

  [Fact]
  public void Parse_InsertStatement_WithNull_ReturnsCorrectAst()
  {
    // Scenario: Inserting NULL keyword
    // Arrange
    var sql = "INSERT INTO mydb.users (id, name) VALUES (1, NULL)";
    var parser = new SqlParser(sql);

    // Act
    var statement = parser.ParseStatement();

    // Assert
    Assert.NotNull(statement);
    var insertStmt = Assert.IsType<InsertStatement>(statement);

    Assert.Equal(2, insertStmt.Values.Count);

    // Value 2: NULL
    var val2 = Assert.IsType<LiteralExpression>(insertStmt.Values[1]);
    Assert.Null(val2.Value);
    // Assuming PrimitiveDataType.Unknown or similar for generic NULL, unless inferred.
    // Based on previous design discussions, we map NULL token to a literal with null value.
  }

  // TODO: Add support for negative numbers
  // https://github.com/lawnboyy/arm-db/issues/11
  // [Fact]
  // public void Parse_InsertStatement_WithNegativeNumbers_ReturnsCorrectAst()
  // {
  //   // Scenario: Inserting negative integers and decimals
  //   // Arrange
  //   var sql = "INSERT INTO weather (temp_c, temp_f) VALUES (-5, -45.5)";
  //   var parser = new SqlParser(sql);

  //   // Act
  //   var statement = parser.ParseStatement();

  //   // Assert
  //   Assert.NotNull(statement);
  //   var insertStmt = Assert.IsType<InsertStatement>(statement);

  //   Assert.Equal(2, insertStmt.Values.Count);

  //   // Value 1: -5 (Int)
  //   var val1 = Assert.IsType<LiteralExpression>(insertStmt.Values[0]);
  //   Assert.Equal(-5, val1.Value);
  //   Assert.Equal(PrimitiveDataType.Int, val1.Type);

  //   // Value 2: -45.5 (Decimal)
  //   var val2 = Assert.IsType<LiteralExpression>(insertStmt.Values[1]);
  //   Assert.Equal(-45.5m, val2.Value);
  //   Assert.Equal(PrimitiveDataType.Decimal, val2.Type);
  // }
}