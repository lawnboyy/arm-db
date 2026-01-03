namespace ArmDb.Storage.Exceptions;

[Serializable]
internal class BufferPoolFullException : Exception
{
  public BufferPoolFullException()
  {
  }

  public BufferPoolFullException(string? message) : base(message)
  {
  }

  public BufferPoolFullException(string? message, Exception? innerException) : base(message, innerException)
  {
  }
}