namespace ArmDb.Storage.Exceptions;

[Serializable]
public class TableDoesNotExistOnDiskException : Exception
{
  public TableDoesNotExistOnDiskException()
  {
  }

  public TableDoesNotExistOnDiskException(string message)
    : base(message)
  {
  }

  public TableDoesNotExistOnDiskException(string message, Exception innerException)
    : base(message, innerException)
  {
  }
}