namespace ArmDb.Sql.Parser;

public record Token(string Value, int Position, TokenType Type);