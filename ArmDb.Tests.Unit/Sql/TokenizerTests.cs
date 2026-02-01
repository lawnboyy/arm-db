using ArmDb.Sql;
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
}