namespace ArmDb.Sql.Ast;

public record SelectStatement(
    List<SelectColumn> Columns,
    ObjectIdentifier FromTable,
    SqlExpression? WhereClause
) : SqlStatement;

public record SelectColumn(
    SqlExpression Expression,
    string? Alias
) : SqlNode;