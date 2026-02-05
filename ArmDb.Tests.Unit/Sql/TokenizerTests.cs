using ArmDb.Sql;
using ArmDb.Sql.Exceptions;
using ArmDb.Sql.Parser;

namespace ArmDb.Tests.Unit.Sql.Parser;

public class TokenizerTests
{
  [Theory]
  [InlineData("SELECT", TokenType.Select)]
  [InlineData("select", TokenType.Select)]
  [InlineData("SeLeCt", TokenType.Select)]
  [InlineData("FROM", TokenType.From)]
  [InlineData("WHERE", TokenType.Where)]
  [InlineData("INSERT", TokenType.Insert)]
  [InlineData("INTO", TokenType.Into)]
  [InlineData("VALUES", TokenType.Values)]
  [InlineData("UPDATE", TokenType.Update)]
  [InlineData("SET", TokenType.Set)]
  [InlineData("DELETE", TokenType.Delete)]
  [InlineData("CREATE", TokenType.Create)]
  [InlineData("TABLE", TokenType.Table)]
  [InlineData("DATABASE", TokenType.Database)]
  [InlineData("DROP", TokenType.Drop)]
  [InlineData("CONSTRAINT", TokenType.Constraint)]
  [InlineData("PRIMARY", TokenType.Primary)]
  [InlineData("KEY", TokenType.Key)]
  [InlineData("FOREIGN", TokenType.Foreign)]
  [InlineData("REFERENCES", TokenType.References)]
  [InlineData("UNIQUE", TokenType.Unique)]
  [InlineData("INDEX", TokenType.Index)]
  [InlineData("DEFAULT", TokenType.Default)]
  [InlineData("NULL", TokenType.Null)]
  [InlineData("NOT", TokenType.Not)]
  [InlineData("AND", TokenType.And)]
  [InlineData("OR", TokenType.Or)]
  [InlineData("AS", TokenType.As)]
  [InlineData("ON", TokenType.On)]
  [InlineData("IN", TokenType.In)]
  [InlineData("IS", TokenType.Is)]
  [InlineData("INT", TokenType.Int)]
  [InlineData("INTEGER", TokenType.Integer)]
  [InlineData("BIGINT", TokenType.BigInt)]
  [InlineData("VARCHAR", TokenType.Varchar)]
  [InlineData("CHAR", TokenType.Char)]
  [InlineData("CHARACTER", TokenType.Character)]
  [InlineData("BOOLEAN", TokenType.Boolean)]
  [InlineData("DECIMAL", TokenType.Decimal)]
  [InlineData("FLOAT", TokenType.Float)]
  [InlineData("DOUBLE", TokenType.Double)]
  [InlineData("DATETIME", TokenType.DateTime)]
  [InlineData("BLOB", TokenType.Blob)]
  [InlineData("TEXT", TokenType.Text)]
  public void Tokenize_SingleKeyword_ReturnsCorrectToken(string input, TokenType expectedType)
  {
    // Arrange
    var tokenizer = new Tokenizer(input);

    // Act
    var token = tokenizer.GetNextToken();

    // Assert
    Assert.NotNull(token);
    Assert.Equal(expectedType, token.Type);
    Assert.Equal(input, token.Value); // Value should preserve original case? Or normalized? Usually original.
    Assert.Equal(0, token.Position);

    // Verify next is EOF
    var eof = tokenizer.GetNextToken();
    Assert.Equal(TokenType.EndOfFile, eof.Type);
  }

  [Fact]
  public void Tokenize_KeywordNextToSymbol_ReturnsCorrectTokens()
  {
    // Scenario: SELECT*FROM -> [SELECT, Star, FROM]
    // This validates that the keyword scanner stops correctly at a symbol delimiter.

    var tokenizer = new Tokenizer("SELECT*FROM");

    // 1. SELECT
    var t1 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Select, t1.Type);

    // 2. *
    var t2 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Star, t2.Type);

    // 3. FROM
    var t3 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.From, t3.Type);
  }

  [Fact]
  public void Tokenize_Symbols_ReturnsCorrectTokens()
  {
    // Scenario: * , ; ( ) . = != <> > < >= <= + - /
    // Note: Spaces added to simplify separation, though tokenizer should handle adjacent symbols too if logic is robust.

    var input = "* , ; ( ) . = != <> > < >= <= + - /";
    var tokenizer = new Tokenizer(input);

    var t1 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Star, t1.Type);
    Assert.Equal("*", t1.Value);

    var t2 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Comma, t2.Type);
    Assert.Equal(",", t2.Value);

    var t3 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Semicolon, t3.Type);
    Assert.Equal(";", t3.Value);

    var t4 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.OpenParen, t4.Type);
    Assert.Equal("(", t4.Value);

    var t5 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.CloseParen, t5.Type);
    Assert.Equal(")", t5.Value);

    var t6 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Dot, t6.Type);
    Assert.Equal(".", t6.Value);

    var t7 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Equal, t7.Type);
    Assert.Equal("=", t7.Value);

    var t8 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.NotEqual, t8.Type); // !=
    Assert.Equal("!=", t8.Value);

    var t9 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.NotEqual, t9.Type); // <>
    Assert.Equal("<>", t9.Value);

    var t10 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.GreaterThan, t10.Type);
    Assert.Equal(">", t10.Value);

    var t11 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.LessThan, t11.Type);
    Assert.Equal("<", t11.Value);

    var t12 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.GreaterThanOrEqual, t12.Type);
    Assert.Equal(">=", t12.Value);

    var t13 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.LessThanOrEqual, t13.Type);
    Assert.Equal("<=", t13.Value);

    var t14 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Plus, t14.Type);
    Assert.Equal("+", t14.Value);

    var t15 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Minus, t15.Type);
    Assert.Equal("-", t15.Value);

    var t16 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Slash, t16.Type);
    Assert.Equal("/", t16.Value);
  }

  [Fact]
  public void Tokenize_NumericLiterals_ReturnsCorrectTokens()
  {
    // Scenario: 123 45.67 -10 -> [NumericLiteral, NumericLiteral, Symbol(-), NumericLiteral]
    // Note: Lexers often tokenize negative numbers as [Minus, Number] and leave it to the Parser to combine them.
    // We will assume that behavior here.

    var input = "123 45.67";
    var tokenizer = new Tokenizer(input);

    // 1. 123
    var t1 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.NumericLiteral, t1.Type);
    Assert.Equal("123", t1.Value);

    // 2. 45.67
    var t2 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.NumericLiteral, t2.Type);
    Assert.Equal("45.67", t2.Value);
  }

  [Fact]
  public void Tokenize_StringLiterals_ReturnsCorrectTokens()
  {
    // Scenario: 'hello' '' 'O''Connor' 'hello world' -> 4 String Literals

    var input = "'hello' '' 'O''Connor' 'hello world'";
    var tokenizer = new Tokenizer(input);

    // 1. 'hello'
    var t1 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.StringLiteral, t1.Type);
    Assert.Equal("hello", t1.Value); // Quotes stripped

    // 2. '' (Empty)
    var t2 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.StringLiteral, t2.Type);
    Assert.Equal("", t2.Value);

    // 3. 'O''Connor' (Escaped quote)
    var t3 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.StringLiteral, t3.Type);
    Assert.Equal("O'Connor", t3.Value); // Double quote becomes single

    // 4. 'hello world' (Spaces preserved)
    var t4 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.StringLiteral, t4.Type);
    Assert.Equal("hello world", t4.Value);
  }

  [Fact]
  public void Tokenize_Identifiers_ReturnsCorrectTokens()
  {
    // Scenario: users "My Table" "select" _hidden -> [Identifier, Identifier]

    var input = "users _hidden";
    var tokenizer = new Tokenizer(input);

    // 1. users (Simple)
    var t1 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, t1.Type);
    Assert.Equal("users", t1.Value);

    // 4. _hidden (Starts with underscore)
    var t4 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, t4.Type);
    Assert.Equal("_hidden", t4.Value);
  }

  [Fact]
  public void Tokenize_UnterminatedString_ThrowsException()
  {
    // Scenario: 'hello world (Missing closing quote)

    var input = "'hello world";
    var tokenizer = new Tokenizer(input);

    // Should throw when trying to find the end of the string
    Assert.ThrowsAny<UnterminatedStringLiteralException>(() => tokenizer.GetNextToken());
  }

  [Fact]
  public void Tokenize_FullSelectStatement_ReturnsCorrectSequence()
  {
    // SQL: SELECT * FROM sys_tables WHERE id = 1;
    var sql = "SELECT * FROM sys_tables WHERE id = 1;";
    var tokenizer = new Tokenizer(sql);

    // 1. SELECT
    Assert.Equal(TokenType.Select, tokenizer.GetNextToken().Type);

    // 2. *
    Assert.Equal(TokenType.Star, tokenizer.GetNextToken().Type);

    // 3. FROM
    Assert.Equal(TokenType.From, tokenizer.GetNextToken().Type);

    // 4. sys_tables
    var table = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, table.Type);
    Assert.Equal("sys_tables", table.Value);

    // 5. WHERE
    Assert.Equal(TokenType.Where, tokenizer.GetNextToken().Type);

    // 6. id
    var col = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, col.Type);
    Assert.Equal("id", col.Value);

    // 7. =
    Assert.Equal(TokenType.Equal, tokenizer.GetNextToken().Type);

    // 8. 1
    var val = tokenizer.GetNextToken();
    Assert.Equal(TokenType.NumericLiteral, val.Type);
    Assert.Equal("1", val.Value);

    // 9. ;
    Assert.Equal(TokenType.Semicolon, tokenizer.GetNextToken().Type);

    // 10. EOF
    Assert.Equal(TokenType.EndOfFile, tokenizer.GetNextToken().Type);
  }

  [Fact]
  public void Tokenize_CreateTableStatement_ReturnsCorrectSequence()
  {
    // SQL: CREATE TABLE users (id INT, name VARCHAR);
    var sql = "CREATE TABLE users (id INT, name VARCHAR);";
    var tokenizer = new Tokenizer(sql);

    Assert.Equal(TokenType.Create, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.Table, tokenizer.GetNextToken().Type);

    var tableName = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, tableName.Type);
    Assert.Equal("users", tableName.Value);

    Assert.Equal(TokenType.OpenParen, tokenizer.GetNextToken().Type);

    var col1 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, col1.Type);
    Assert.Equal("id", col1.Value);

    // INT is a keyword (DataType)
    Assert.Equal(TokenType.Int, tokenizer.GetNextToken().Type);

    Assert.Equal(TokenType.Comma, tokenizer.GetNextToken().Type);

    var col2 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, col2.Type);
    Assert.Equal("name", col2.Value);

    // VARCHAR is a keyword
    Assert.Equal(TokenType.Varchar, tokenizer.GetNextToken().Type);

    Assert.Equal(TokenType.CloseParen, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.Semicolon, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.EndOfFile, tokenizer.GetNextToken().Type);
  }

  [Fact]
  public void Tokenize_KitchenSink_ReturnsCorrectSequence()
  {
    // Complex scenario with multiple statements, operators, parentheses, and types.
    var sql = @"
          SELECT * FROM orders WHERE (total > 100.50 AND status != 'pending') OR is_vip = TRUE;
          INSERT INTO logs (msg) VALUES ('Error: 404');";

    var tokenizer = new Tokenizer(sql);

    // Statement 1: SELECT
    Assert.Equal(TokenType.Select, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.Star, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.From, tokenizer.GetNextToken().Type);

    var table = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, table.Type);
    Assert.Equal("orders", table.Value);

    Assert.Equal(TokenType.Where, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.OpenParen, tokenizer.GetNextToken().Type);

    var col1 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, col1.Type);
    Assert.Equal("total", col1.Value);

    Assert.Equal(TokenType.GreaterThan, tokenizer.GetNextToken().Type);

    var num = tokenizer.GetNextToken();
    Assert.Equal(TokenType.NumericLiteral, num.Type);
    Assert.Equal("100.50", num.Value);

    Assert.Equal(TokenType.And, tokenizer.GetNextToken().Type);

    var col2 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, col2.Type);
    Assert.Equal("status", col2.Value);

    Assert.Equal(TokenType.NotEqual, tokenizer.GetNextToken().Type);

    var str = tokenizer.GetNextToken();
    Assert.Equal(TokenType.StringLiteral, str.Type);
    Assert.Equal("pending", str.Value);

    Assert.Equal(TokenType.CloseParen, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.Or, tokenizer.GetNextToken().Type);

    var col3 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, col3.Type);
    Assert.Equal("is_vip", col3.Value);

    Assert.Equal(TokenType.Equal, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.BooleanLiteral, tokenizer.GetNextToken().Type); // TRUE is mapped to BooleanLiteral
    Assert.Equal(TokenType.Semicolon, tokenizer.GetNextToken().Type);

    // Statement 2: INSERT
    Assert.Equal(TokenType.Insert, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.Into, tokenizer.GetNextToken().Type);

    var table2 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, table2.Type);
    Assert.Equal("logs", table2.Value);

    Assert.Equal(TokenType.OpenParen, tokenizer.GetNextToken().Type);

    var col4 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.Identifier, col4.Type);
    Assert.Equal("msg", col4.Value);

    Assert.Equal(TokenType.CloseParen, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.Values, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.OpenParen, tokenizer.GetNextToken().Type);

    var str2 = tokenizer.GetNextToken();
    Assert.Equal(TokenType.StringLiteral, str2.Type);
    Assert.Equal("Error: 404", str2.Value);

    Assert.Equal(TokenType.CloseParen, tokenizer.GetNextToken().Type);
    Assert.Equal(TokenType.Semicolon, tokenizer.GetNextToken().Type);

    Assert.Equal(TokenType.EndOfFile, tokenizer.GetNextToken().Type);
  }
}