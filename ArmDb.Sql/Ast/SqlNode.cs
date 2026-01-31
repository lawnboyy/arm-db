namespace ArmDb.Sql.Ast;

public abstract record SqlNode;

public abstract record SqlStatement : SqlNode;  // Top-level commands (SELECT, INSERT...)
public abstract record SqlExpression : SqlNode; // Value producers (1, 'bob', a = b) 