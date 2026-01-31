namespace ArmDb.Sql.Ast;

public record InsertStatement(
    ObjectIdentifier Table,
    IReadOnlyList<string> Columns,
    IReadOnlyList<SqlExpression> Values
) : SqlStatement;