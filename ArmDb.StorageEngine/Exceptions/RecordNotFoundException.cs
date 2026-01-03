namespace ArmDb.Storage.Exceptions;

[Serializable]
public class RecordNotFoundException : Exception
{
  public RecordNotFoundException()
  {
  }

  public RecordNotFoundException(string message)
    : base(message)
  {
  }

  public RecordNotFoundException(string message, Exception innerException)
    : base(message, innerException)
  {
  }
}