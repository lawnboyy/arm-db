
using System.Text;
using ArmDb.Sql.Exceptions;
using static ArmDb.Sql.Parser.TokenizerUtilities;

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
    {
      return new Token("", _position, TokenType.EndOfFile);
    }

    // Ignore all whitespace
    while (char.IsWhiteSpace(_sql[_position]))
    {
      // Ignore whitespace; advance the position
      _position++;
    }

    int startPos = _position;

    // Check if the next character is a symbol.
    var nextChar = _sql[startPos];
    var nextCharStr = nextChar.ToString();
    // Handle multi-character symbols (e.g. <=, >=, !=)
    var next2Chars = startPos < _sql.Length - 1 ? _sql.AsSpan().Slice(startPos, 2).ToString() : "";

    // First check if the first character of the next token is a number. If so, then the entire token must be a number
    // or it's invalid since identifiers and keywords cannot start with a number.    
    if (char.IsNumber(nextChar))
    {
      var currentChar = nextChar;
      var currentPosition = startPos;
      var decimalCount = 0;
      while ((char.IsNumber(currentChar) || currentChar == '.') && currentPosition < _sql.Length)
      {
        if (currentChar == '.')
        {
          decimalCount++;
          if (decimalCount > 1)
            throw new ArgumentException("Invalid number value found with multiple decimals.");
        }

        currentPosition++;
        if (currentPosition < _sql.Length)
          currentChar = _sql[currentPosition];
      }

      // Now slice out the token
      var tokenValue = _sql.AsSpan().Slice(startPos, currentPosition - startPos).ToString();

      // If we haven't reached the end of the SQL string, move the position back 1.
      _position = currentPosition;

      return new Token(tokenValue, startPos, TokenType.NumericLiteral);
    }
    else if (nextChar == '\'')
    {
      var literal = ParseStringLiteral(_sql.AsSpan(), ref _position);
      return new Token(literal, startPos, TokenType.StringLiteral);
    }
    // Check the double character symbols first so we don't incorrectly match a single character symbol...
    else if (DoubleCharSymbolLookup.ContainsKey(next2Chars))
    {
      var token = new Token(next2Chars, startPos, DoubleCharSymbolLookup[next2Chars]);
      _position += 2;
      return token;
    }
    else if (SingleCharSymbolLookup.ContainsKey(nextCharStr))
    {
      var token = new Token(nextCharStr, startPos, SingleCharSymbolLookup[nextCharStr]);
      _position++;
      return token;
    }
    // See if the next token starts with a valid character for an identifier...
    else if (IsValidIdentifierStartingChar(_sql[startPos]))
    {
      var currentPosition = startPos + 1;
      var currentChar = _sql[currentPosition];
      // Capture the current token by reading ahead until the stopping condition: delimiter, whitespace, or end of SQL statement
      while (!IsDelimiter(currentChar) && !char.IsWhiteSpace(currentChar) && !SingleCharSymbolLookup.ContainsKey(currentChar.ToString()) && currentPosition < _sql.Length)
      {
        currentPosition++;
        if (currentPosition < _sql.Length)
          currentChar = _sql[currentPosition];
      }

      // Now slice out the token
      var tokenValue = _sql.AsSpan().Slice(startPos, currentPosition - startPos).ToString();

      // If we haven't reached the end of the SQL string, move the position back 1.
      _position = currentPosition;

      // See if this is a keyword...
      if (KeywordLookup.ContainsKey(tokenValue))
        return new Token(tokenValue, startPos, KeywordLookup[tokenValue]);

      // The remaining option is an identifier...
      return new Token(tokenValue, startPos, TokenType.Identifier);
    }
    else
    {
      throw new NotSupportedException($"SQL token not supported!");
    }
  }

  private string ParseStringLiteral(ReadOnlySpan<char> sql, ref int position)
  {
    if (sql.Slice(position, 2).ToString() == "''")
    {
      // Advance the position past the empty string...
      position += 2;
      return "";
    }

    var startPos = position;

    var currentPosition = position;
    var currentChar = sql[currentPosition];
    // The first character should be a single quote
    if (currentChar != '\'')
      throw new ArgumentException("Attempted to parse a string literally that was not wrapped in single quotes!");

    currentPosition++;

    if (currentPosition < sql.Length)
      currentChar = sql[currentPosition];
    else
      throw new ArgumentException("Invalid string literal!");

    while (currentChar != '\'' && currentPosition < sql.Length)
    {
      currentPosition++;
      if (currentPosition < sql.Length)
        currentChar = sql[currentPosition];

      // If we encounter 2 single quotes in a row, it is an escaped single quote and we ignore it.
      var nextPos = currentPosition + 1;
      if (currentChar == '\'' && nextPos < sql.Length && sql[nextPos] == '\'')
      {
        currentPosition++;
        if (nextPos + 1 < sql.Length)
          currentChar = sql[nextPos + 1];
      }
    }

    if (currentPosition == sql.Length)
      throw new UnterminatedStringLiteralException($"Unterminated string literal at position: {startPos}");

    // Advance the position...
    _position = currentPosition + 1;

    // Return the string with the outer quotes stripped.
    var value = _sql.AsSpan().Slice(startPos + 1, currentPosition - startPos - 1);
    return UnescapeStringLiteral(value);
  }

  private string UnescapeStringLiteral(ReadOnlySpan<char> literal)
  {
    if (literal.IndexOf("''") < 0)
      return literal.ToString();

    var sb = new StringBuilder(literal.Length);

    for (int i = 0; i < literal.Length; i++)
    {
      char c = literal[i];
      sb.Append(c);

      // If we hit a quote and the next one is also a quote, skip the next one
      if (c == '\'' && i + 1 < literal.Length && literal[i + 1] == '\'')
      {
        i++; // Skip the escaping quote
      }
    }

    return sb.ToString();
  }
}