
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
    // TODO: Handle multi-character symbols (e.g. <=, >=, !=)
    var nextChar = _sql[startPos];
    var nextCharStr = nextChar.ToString();
    var next2Chars = startPos < _sql.Length - 1 ? _sql.AsSpan().Slice(startPos, 2).ToString() : "";

    // Check the double character symbols first so we don't incorrectly match a single character symbol...
    if (DoubleCharSymbolLookup.ContainsKey(next2Chars))
    {
      var token = new Token(nextChar, startPos, DoubleCharSymbolLookup[next2Chars]);
      _position += 2;
      return token;
    }
    else if (SingleCharSymbolLookup.ContainsKey(nextCharStr))
    {
      var token = new Token(nextChar, startPos, SingleCharSymbolLookup[nextCharStr]);
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

      throw new NotSupportedException($"SQL token not supported!");
    }
    else
    {
      throw new NotSupportedException($"SQL token not supported!");
    }
  }
}