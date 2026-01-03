namespace ArmDb.Storage.Exceptions;

[Serializable]
public class CouldNotSetChildPointerException : Exception
{
  public CouldNotSetChildPointerException()
  {
  }

  public CouldNotSetChildPointerException(string message)
    : base(message)
  {
  }

  public CouldNotSetChildPointerException(string message, Exception innerException)
    : base(message, innerException)
  {
  }
}