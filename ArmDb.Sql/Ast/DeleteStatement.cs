namespace ArmDb.Sql.Ast;

public record DeleteStatement(
    ObjectIdentifier FromTable,
    SqlExpression? WhereClause
) : SqlStatement;