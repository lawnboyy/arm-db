namespace ArmDb.Sql.Exceptions;

[Serializable]
public class InvalidSqlException : Exception
{
  public InvalidSqlException()
  {
  }

  public InvalidSqlException(string? message) : base(message)
  {
  }

  public InvalidSqlException(string? message, Exception? innerException) : base(message, innerException)
  {
  }
}