using ArmDb.SchemaDefinition;

namespace ArmDb.Sql.Ast;

// Literals
public record LiteralExpression(object? Value, PrimitiveDataType Type) : SqlExpression;

// Identifiers (Column References)
public record ColumnExpression(string Name, string? TableAlias = null) : SqlExpression;

// Binary Operations (WHERE id = 1)
public record BinaryExpression(
    SqlExpression Left,
    BinaryOperator Operator,
    SqlExpression Right
) : SqlExpression;

public enum BinaryOperator
{
  Equal, NotEqual, GreaterThan, LessThan, And, Or, Add, Subtract
}

// Function Calls (nextval('...'))
public record FunctionCallExpression(
    string FunctionName,
    IReadOnlyList<SqlExpression> Arguments
) : SqlExpression;