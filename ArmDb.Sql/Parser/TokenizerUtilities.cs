using System.Collections.Concurrent;

namespace ArmDb.Sql.Parser;

public static class TokenizerUtilities
{
  public static readonly ConcurrentDictionary<string, TokenType> KeywordLookup = BuildKeywordsDictionary();
  public static readonly ConcurrentDictionary<string, TokenType> SingleCharSymbolLookup = BuildSingleCharSymbolsDictionary();
  public static readonly ConcurrentDictionary<string, TokenType> DoubleCharSymbolLookup = BuildDoubleCharSymbolsDictionary();

  private static readonly object _lookupLockObj = new object();

  public static bool IsValidIdentifierStartingChar(char c)
  {
    return (c >= 'a' && c <= 'z') ||
           (c >= 'A' && c <= 'Z') ||
           c == '_';
  }

  public static bool IsDelimiter(char c)
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

  public static ConcurrentDictionary<string, TokenType> BuildSingleCharSymbolsDictionary()
  {
    lock (_lookupLockObj)
    {
      if (SingleCharSymbolLookup != null)
        return SingleCharSymbolLookup;

      var dict = new ConcurrentDictionary<string, TokenType>();

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
  }

  private static ConcurrentDictionary<string, TokenType> BuildDoubleCharSymbolsDictionary()
  {
    lock (_lookupLockObj)
    {
      if (DoubleCharSymbolLookup != null)
        return DoubleCharSymbolLookup;

      var dict = new ConcurrentDictionary<string, TokenType>();

      // Multi-character symbols first (greedy matching)
      dict[">="] = TokenType.GreaterThanOrEqual;
      dict["<="] = TokenType.LessThanOrEqual;
      dict["!="] = TokenType.NotEqual;
      dict["<>"] = TokenType.NotEqual;

      return dict;
    }
  }

  private static ConcurrentDictionary<string, TokenType> BuildKeywordsDictionary()
  {
    lock (_lookupLockObj)
    {
      if (KeywordLookup != null)
        return KeywordLookup;

      var dictionary = new ConcurrentDictionary<string, TokenType>(StringComparer.OrdinalIgnoreCase);

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
}