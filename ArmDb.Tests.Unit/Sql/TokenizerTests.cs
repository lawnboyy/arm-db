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
}