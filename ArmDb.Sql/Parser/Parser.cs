using ArmDb.SchemaDefinition;
using ArmDb.Sql.Ast;
using ArmDb.Sql.Exceptions;

namespace ArmDb.Sql.Parser;

public class SqlParser
{
  private readonly Tokenizer _tokenizer;

  public SqlParser(string sql)
  {
    _tokenizer = new Tokenizer(sql);
  }

  public SqlStatement ParseStatement()
  {
    var currentToken = _tokenizer.GetNextToken();

    switch (currentToken.Type)
    {
      case TokenType.Create:
        {
          return ParseCreateStatement();
        }
      default:
        throw new InvalidSqlException("Unexpected token type!");
    }
    throw new NotImplementedException();
  }

  private SqlStatement ParseCreateStatement()
  {
    // The first token is the CREATE keyword. The next token must be a
    // reserved keyword to indicate what is to be created. Otherwise, we
    // have an invalid SQL statement.
    // Currently, we just support DATABASE, TABLE, and INDEX.
    // Get the next token.
    var token = _tokenizer.GetNextToken();
    switch (token.Type)
    {
      case TokenType.Database:
        // The next token must be the database name.
        token = _tokenizer.GetNextToken();
        return new CreateDatabaseStatement(token.Value);
      case TokenType.Index:
      case TokenType.Table:
        return ParseCreateTableStatement();

      default:
        throw new InvalidSqlException($"Invalid token found parsing CREATE statement: {token.Type}");
    }
  }

  private SqlStatement ParseCreateTableStatement()
  {
    // The next token must be the table name.
    var token = _tokenizer.GetNextToken();
    if (token.Type != TokenType.Identifier)
      throw new InvalidSqlException($"Expected table name but found: {token.Value}");

    ObjectIdentifier table = new ObjectIdentifier(token.Value, "mydb");

    // Now we must parse the column definitions...
    var columns = ParseCreateTableColumns();
    var constraints = new List<Constraint>();
    return new CreateTableStatement(table, columns, constraints);
  }

  private IReadOnlyList<ColumnDefinition> ParseCreateTableColumns()
  {
    var columns = new List<ColumnDefinition>();

    // The next token must be an opening parenthesis to start the
    // column definition list.
    var token = _tokenizer.GetNextToken();
    if (token.Type != TokenType.OpenParen)
      throw new InvalidSqlException($"Invalid syntax for CREATE TABLE columns at line: {token.Position}");

    // Now parse the column definitions until we reach the end of the content within the parentheses.
    // Push the opening parenthesis on the stack so we can determine when we've hit the closing parenthesis.
    var stack = new Stack<char>();
    stack.Push(token.Value[0]);
    // Column identifier
    var columnName = "";
    while (stack.Count > 0)
    {
      token = _tokenizer.GetNextToken();
      switch (token.Type)
      {
        case TokenType.Identifier:
          columnName = token.Value;
          break;
        case TokenType.Int:
          var columnDef = new ColumnDefinition(columnName, new DataTypeInfo(PrimitiveDataType.Int));
          columns.Add(columnDef);
          break;
        case TokenType.Varchar:
          var varChar = ParseVarCharDefinition(columnName);
          columns.Add(varChar);
          break;
        case TokenType.OpenParen:
          stack.Push(token.Value[0]);
          break;
        case TokenType.CloseParen:
          stack.Pop();
          break;
        case TokenType.Comma:
          break;
      }
    }

    return columns;
  }

  private ColumnDefinition ParseVarCharDefinition(string columnName)
  {
    // VARCHAR should be in the form VARCHAR(255), so we need to parse out the max length.
    // The next token must be the opening parenthesis for the max length.
    var token = _tokenizer.GetNextToken();
    if (token.Type != TokenType.OpenParen)
      throw new InvalidSqlException($"Syntax error: expected '(' but found {token.Value} at {token.Position}");

    // The next token should be the number value for the max length.
    token = _tokenizer.GetNextToken();
    if (token.Type != TokenType.NumericLiteral)
      throw new InvalidSqlException($"Parse error: expected number but found {token.Value} at {token.Position}");

    var maxLength = int.Parse(token.Value);
    // Discard the closing parenthesis...
    token = _tokenizer.GetNextToken();
    if (token.Type != TokenType.CloseParen)
      throw new InvalidSqlException($"Syntax error: expected ')' but found {token.Value}");

    var varChar = new ColumnDefinition(columnName, new DataTypeInfo(PrimitiveDataType.Varchar, maxLength));
    return varChar;
  }
}