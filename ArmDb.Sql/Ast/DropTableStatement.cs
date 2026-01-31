namespace ArmDb.Sql.Ast;

public record DropTableStatement(
    ObjectIdentifier Table
) : SqlStatement;