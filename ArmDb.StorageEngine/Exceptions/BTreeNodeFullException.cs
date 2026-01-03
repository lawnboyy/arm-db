namespace ArmDb.Storage.Exceptions;

[Serializable]
internal class BTreeNodeFullException : Exception
{
  public BTreeNodeFullException()
  {
  }

  public BTreeNodeFullException(string? message) : base(message)
  {
  }

  public BTreeNodeFullException(string? message, Exception? innerException) : base(message, innerException)
  {
  }
}