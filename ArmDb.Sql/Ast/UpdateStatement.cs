namespace ArmDb.Sql.Ast;

public record UpdateStatement(
    ObjectIdentifier Table,
    List<UpdateAssignment> Assignments,
    SqlExpression? WhereClause
) : SqlStatement;

public record UpdateAssignment(
    string ColumnName,
    SqlExpression Value
) : SqlNode;