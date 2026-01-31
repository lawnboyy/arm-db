using System.Runtime.CompilerServices;

namespace ArmDb.Sql.Parser;

public class Tokenizer
{
  private int _position;
  private readonly string _sql;
  private readonly Dictionary<string, TokenType> _keywordLookup;
  private readonly Dictionary<string, TokenType> _singleCharSymbolLookup;
  private readonly Dictionary<string, TokenType> _doubleCharSymbolLookup;

  public Tokenizer(string sql)
  {
    _position = 0;
    _sql = sql;
    _keywordLookup = BuildKeywordsDictionary();
    _singleCharSymbolLookup = BuildSingleCharSymbolsDictionary();
    _doubleCharSymbolLookup = BuildDoubleCharSymbolsDictionary();
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
    if (_doubleCharSymbolLookup.ContainsKey(next2Chars))
    {
      var token = new Token(nextChar, startPos, _doubleCharSymbolLookup[next2Chars]);
      _position += 2;
      return token;
    }
    else if (_singleCharSymbolLookup.ContainsKey(nextCharStr))
    {
      var token = new Token(nextChar, startPos, _singleCharSymbolLookup[nextCharStr]);
      _position++;
      return token;
    }
    // See if the next token starts with a valid character for an identifier...
    else if (IsValidIdentifierStartingChar(_sql[startPos]))
    {
      var currentPosition = startPos + 1;
      var currentChar = _sql[currentPosition];
      // Capture the current token by reading ahead until the stopping condition: delimiter, whitespace, or end of SQL statement
      while (!IsDelimiter(currentChar) && !char.IsWhiteSpace(currentChar) && !_singleCharSymbolLookup.ContainsKey(currentChar.ToString()) && currentPosition < _sql.Length)
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
      var tokenLower = tokenValue.ToLower();
      if (_keywordLookup.ContainsKey(tokenLower))
        return new Token(tokenValue, startPos, _keywordLookup[tokenLower]);

      throw new NotSupportedException($"SQL token not supported!");
    }
    else
    {
      throw new NotSupportedException($"SQL token not supported!");
    }
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

  private Dictionary<string, TokenType> BuildSingleCharSymbolsDictionary()
  {
    var dict = new Dictionary<string, TokenType>();

    // Single-character symbols
    dict["*"] = TokenType.Star;
    dict[","] = TokenType.Comma;
    dict[";"] = TokenType.Semicolon;
    dict["("] = TokenType.OpenParen;
    dict[")"] = TokenType.CloseParen;
    dict["."] = TokenType.Dot;
    dict["="] = TokenType.Equal;
    dict[">"] = TokenType.GreaterThan;
    dict["<"] = TokenType.LessThan;
    dict["+"] = TokenType.Plus;
    dict["-"] = TokenType.Minus;
    dict["/"] = TokenType.Slash;

    return dict;
  }

  private static Dictionary<string, TokenType> BuildDoubleCharSymbolsDictionary()
  {
    var dict = new Dictionary<string, TokenType>();

    // Multi-character symbols first (greedy matching)
    dict[">="] = TokenType.GreaterThanOrEqual;
    dict["<="] = TokenType.LessThanOrEqual;
    dict["!="] = TokenType.NotEqual;
    dict["<>"] = TokenType.NotEqual;

    return dict;
  }

  private static Dictionary<string, TokenType> BuildKeywordsDictionary()
  {
    var dictionary = new Dictionary<string, TokenType>(StringComparer.OrdinalIgnoreCase);

    // Explicitly map keywords. 
    // We could use reflection over the Enum, but explicit mapping is safer for versioning 
    // and allows handling "multi-word" keywords if we ever needed them (though tokenizers usually split them).

    // DML
    dictionary["select"] = TokenType.Select;
    dictionary["insert"] = TokenType.Insert;
    dictionary["update"] = TokenType.Update;
    dictionary["delete"] = TokenType.Delete;
    dictionary["from"] = TokenType.From;
    dictionary["where"] = TokenType.Where;
    dictionary["into"] = TokenType.Into;
    dictionary["values"] = TokenType.Values;
    dictionary["set"] = TokenType.Set;

    // DDL
    dictionary["create"] = TokenType.Create;
    dictionary["drop"] = TokenType.Drop;
    dictionary["table"] = TokenType.Table;
    dictionary["database"] = TokenType.Database;
    dictionary["constraint"] = TokenType.Constraint;
    dictionary["primary"] = TokenType.Primary;
    dictionary["key"] = TokenType.Key;
    dictionary["foreign"] = TokenType.Foreign;
    dictionary["references"] = TokenType.References;
    dictionary["unique"] = TokenType.Unique;
    dictionary["index"] = TokenType.Index;
    dictionary["default"] = TokenType.Default;
    dictionary["null"] = TokenType.Null;
    dictionary["not"] = TokenType.Not;

    // General
    dictionary["and"] = TokenType.And;
    dictionary["or"] = TokenType.Or;
    dictionary["as"] = TokenType.As;
    dictionary["on"] = TokenType.On;
    dictionary["in"] = TokenType.In;
    dictionary["is"] = TokenType.Is;

    // Data Types
    dictionary["int"] = TokenType.Int;
    dictionary["integer"] = TokenType.Integer;
    dictionary["bigint"] = TokenType.BigInt;
    dictionary["varchar"] = TokenType.Varchar;
    dictionary["char"] = TokenType.Char;
    dictionary["character"] = TokenType.Character;
    dictionary["boolean"] = TokenType.Boolean;
    dictionary["decimal"] = TokenType.Decimal;
    dictionary["float"] = TokenType.Float;
    dictionary["double"] = TokenType.Double;
    dictionary["datetime"] = TokenType.DateTime;
    dictionary["blob"] = TokenType.Blob;
    dictionary["text"] = TokenType.Text;

    // Boolean Literals (treated as keywords for parsing)
    dictionary["true"] = TokenType.BooleanLiteral;
    dictionary["false"] = TokenType.BooleanLiteral;

    return dictionary;
  }
}