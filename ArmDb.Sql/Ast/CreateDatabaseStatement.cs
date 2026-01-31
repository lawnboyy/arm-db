using ArmDb.SchemaDefinition;

namespace ArmDb.Sql.Ast;

public record CreateDatabaseStatement(
    string DatabaseName
) : SqlStatement;