using System.Runtime.InteropServices;

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
  /// We are usin Memory<byte> here for performance. Memory<T> represents a contiguous
  /// block of memory which can be sliced without copying the data. It is heap allocated,
  /// unlike Span<T>, so it can be used for async operations and can be passed by reference
  /// to other methods.
  /// </summary>
  private readonly Memory<byte> _memory;

  /// <summary>
  /// The unique identifier of this page within the database instance.
  /// </summary>
  private readonly long _pageId;

  /// <summary>
  /// Gets the unique, non-negative identifier of this page.
  /// </summary>
  public long Id => _pageId;

  /// <summary>
  /// Gets the underlying memory buffer holding the page's data.
  /// Use Data.Span for direct synchronous manipulation.
  /// </summary>
  public Memory<byte> Data => _memory;

  /// <summary>
  /// Gets a Span<byte> view over the page's memory buffer for synchronous access.
  /// Provides direct, efficient access to the page data.
  /// </summary>
  public Span<byte> Span => _memory.Span;

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

  /// <summary>
  /// Writes a single byte to the page at the specified offset.
  /// </summary>
  /// <param name="offset">The zero-based page (byte) offset to write to.</param>
  /// <param name="value">The byte value to write.</param>
  /// <exception cref="ArgumentOutOfRangeException"></exception>
  public void WriteByte(int offset, byte value)
  {
    if ((uint)offset >= Size)
    {
      throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) is not within the range [0..{Size - 1}]");
    }

    var pageSpan = _memory.Span;
    pageSpan[offset] = value;
  }

  /// <summary>
  /// Writes a 32-bit signed integer (int) to the page at the specified offset
  /// using little-endian format. Corresponds to the INT primitive type.
  /// </summary>
  /// <param name="offset">The zero-based byte offset within the page to write to.</param>
  /// <param name="value">The int value to write.</param>
  /// <exception cref="ArgumentOutOfRangeException">
  /// Thrown if the offset is negative or if writing 4 bytes would exceed the page size.
  /// </exception>
  public void WriteInt32(int offset, int value)
  {
    const int valueSize = sizeof(int); // 4 bytes

    // 1. Bounds Check
    if ((uint)offset > (Size - valueSize)) // Efficient check for offset < 0 or offset + valueSize > Size
    {
      if (offset < 0)
        throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) cannot be negative.");
      else
        throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) plus value size ({valueSize}) exceeds page size ({Size}).");
    }

    // 2. Get Span and Write
    Span<byte> destination = _memory.Span.Slice(offset, valueSize);
    BinaryUtilities.WriteInt32LittleEndian(destination, value);
  }

  /// <summary>
  /// Writes a 64-bit signed integer (long) to the page at the specified offset
  /// using little-endian format. Useful for internal IDs, LSNs, DateTime ticks, or a BIGINT SQL type.
  /// </summary>
  /// <param name="offset">The zero-based byte offset within the page to write to.</param>
  /// <param name="value">The long value to write.</param>
  /// <exception cref="ArgumentOutOfRangeException">
  /// Thrown if the offset is negative or if writing 8 bytes would exceed the page size.
  /// </exception>
  public void WriteInt64(int offset, long value)
  {
    const int valueSize = sizeof(long); // 8 bytes

    // 1. Bounds Check
    if ((uint)offset > (Size - valueSize)) // Efficient combined check
    {
      if (offset < 0)
        throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) cannot be negative.");
      else
        throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) plus value size ({valueSize}) exceeds page size ({Size}).");
    }

    // 2. Get Span and Write
    Span<byte> destination = _memory.Span.Slice(offset, valueSize);
    BinaryUtilities.WriteInt64LittleEndian(destination, value);
  }
}