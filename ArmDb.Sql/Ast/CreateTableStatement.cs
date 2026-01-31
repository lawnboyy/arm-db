using ArmDb.SchemaDefinition;

namespace ArmDb.Sql.Ast;

public record CreateTableStatement(
    ObjectIdentifier Table,
    IReadOnlyList<ColumnDefinition> Columns,
    IReadOnlyList<Constraint> Constraints
) : SqlStatement;