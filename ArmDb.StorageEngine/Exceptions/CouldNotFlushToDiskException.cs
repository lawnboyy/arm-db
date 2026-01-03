namespace ArmDb.Storage.Exceptions;

[Serializable]
public class CouldNotFlushToDiskException : Exception
{
  public CouldNotFlushToDiskException()
  {
  }

  public CouldNotFlushToDiskException(string message)
    : base(message)
  {
  }

  public CouldNotFlushToDiskException(string message, Exception innerException)
    : base(message, innerException)
  {
  }
}