[Serializable]
internal class CouldNotLoadPageFromDiskException : Exception
{
  public CouldNotLoadPageFromDiskException()
  {
  }

  public CouldNotLoadPageFromDiskException(string? message) : base(message)
  {
  }

  public CouldNotLoadPageFromDiskException(string? message, Exception? innerException) : base(message, innerException)
  {
  }
}