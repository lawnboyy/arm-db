namespace ArmDb.Sql.Ast;


/// <summary>
/// Represents an object in our database could include tables, views, and stored procedures/functions. Currently,
/// ArmDb's hierarchy is just database->table/view/function. Later we could add a schema level to the hierarchy
/// such as database->schema->table/view/function.
/// </summary>
/// <param name="Name">Name of the object</param>
/// <param name="DatabaseName">The database this object belongs to (could be extended to include schema later)</param>
public record ObjectIdentifier(string Name, string DatabaseName);