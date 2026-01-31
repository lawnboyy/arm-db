using System.Runtime.CompilerServices;

namespace ArmDb.Sql.Parser;

public class Tokenizer
{
  private int _position;
  private readonly string _sql;

  public Tokenizer(string sql)
  {
    _position = 0;
    _sql = sql;
  }

  public Token GetNextToken()
  {
    if (_position == _sql.Length)
      return new Token("", _position, TokenType.EndOfFile);

    int startPos = _position;

    // First see what the current token begins with.
    if (IsValidIdentifierStartingChar(_sql[startPos]))
    {
      var currentPosition = startPos + 1;
      var currentChar = _sql[currentPosition];
      // Capture the current token by reading ahead until the stopping condition: delimiter, whitespace, or end of SQL statement
      while (!IsDelimiter(currentChar) && !char.IsWhiteSpace(currentChar) && currentPosition < _sql.Length)
      {
        currentPosition++;
      }

      // Now slice out the token
      var tokenValue = _sql.AsSpan().Slice(startPos, currentPosition - startPos).ToString();

      // If we haven't reached the end of the SQL string, move the position back 1.
      _position = (currentPosition < _sql.Length) ? currentPosition - 1 : currentPosition;

      return new Token(tokenValue, startPos, TokenType.Select);
    }

    throw new NotSupportedException($"SQL token not supported!");
  }

  private static bool IsValidIdentifierStartingChar(char c)
  {
    return (c >= 'a' && c <= 'z') ||
           (c >= 'A' && c <= 'Z') ||
           c == '_';
  }

  private static bool IsDelimiter(char c)
  {
    return char.IsWhiteSpace(c) ||
           c == '(' || c == ')' ||
           c == ',' || c == ';' ||
           c == '=' || c == '*' ||
           c == '.' || c == '+' ||
           c == '-' || c == '/' ||
           c == '<' || c == '>' ||
           c == '!';
  }
}