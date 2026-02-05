namespace ArmDb.Sql.Exceptions;

[Serializable]
public class UnterminatedStringLiteralException : Exception
{
  public UnterminatedStringLiteralException()
  {
  }

  public UnterminatedStringLiteralException(string? message) : base(message)
  {
  }

  public UnterminatedStringLiteralException(string? message, Exception? innerException) : base(message, innerException)
  {
  }
}