namespace ArmDb.Sql.Parser;

public record Token(object Value, int Position, TokenType Type);