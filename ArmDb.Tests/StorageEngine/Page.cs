namespace ArmDb.StorageEngine;

/// <summary>
/// Represents an in-memory copy of a fixed-size page from disk.
/// Provides structured access to the underlying byte data.
/// (Implementation details will be added incrementally)
/// </summary>
public sealed class Page
{
  /// <summary>
  /// Defines the standard size of a page in bytes (e.g., 8KB). Potentially move this to a constants file,
  /// or a configuration setting for flexibility.
  /// </summary>
  public const int Size = 8192;

  /// <summary>
  /// The underlying memory buffer holding the page's data. Typically an 8KB slice.
  /// </summary>
  private readonly Memory<byte> _memory;

  /// <summary>
  /// The unique identifier of this page within the database instance.
  /// </summary>
  private readonly long _pageId;

  /// <summary>
  /// Initializes a new instance of the <see cref="Page"/> class.
  /// Should be called by components managing page buffers (like BufferPoolManager).
  /// </summary>
  /// <param name="pageId">The unique, non-negative identifier for this page.</param>
  /// <param name="memory">The Memory<byte> slice representing the page's data buffer. Its length MUST match Page.Size.</param>
  /// <exception cref="ArgumentOutOfRangeException">Thrown if pageId is negative.</exception>
  /// <exception cref="ArgumentException">Thrown if the provided memory slice length does not match Page.Size.</exception>
  internal Page(long pageId, Memory<byte> memory)
  {
    // Validate Page ID
    ArgumentOutOfRangeException.ThrowIfNegative(pageId);

    // Validate Memory buffer size
    if (memory.Length != Size)
    {
      throw new ArgumentException($"Memory buffer size provided ({memory.Length} bytes) must exactly match the required Page.Size ({Size} bytes).", nameof(memory));
    }

    // Assign the readonly fields
    _pageId = pageId;
    _memory = memory;
  }
}