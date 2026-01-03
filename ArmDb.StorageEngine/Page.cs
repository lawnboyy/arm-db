namespace ArmDb.Storage;

/// <summary>
/// Represents an in-memory copy of a fixed-size page from disk.
/// Provides structured access to the underlying byte data.
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
  private readonly PageId _pageId;

  /// <summary>
  /// Gets the unique identifier of this page which is a combination of the table ID and page index.
  /// </summary>
  public PageId Id => _pageId;

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
  /// <param name="pageId">The unique page identifier consistenting of the table ID and the zero-based page index.</param>
  /// <param name="memory">The Memory<byte> slice representing the page's data buffer. Its length MUST match Page.Size.</param>
  /// <exception cref="ArgumentOutOfRangeException">Thrown if pageId is negative.</exception>
  /// <exception cref="ArgumentException">Thrown if the provided memory slice length does not match Page.Size.</exception>
  internal Page(PageId pageId, Memory<byte> memory)
  {
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
  /// Gets a read-only span representing a slice of the page's data.
  /// This provides efficient, zero-copy access to the underlying page memory.
  /// </summary>
  /// <param name="offset">The zero-based byte offset within the page to start the slice from.</param>
  /// <param name="length">The desired length of the slice in bytes.</param>
  /// <returns>A ReadOnlySpan<byte> viewing the specified portion of the page data.</returns>
  /// <exception cref="ArgumentOutOfRangeException">
  /// Thrown if the offset or length are negative, or if the combination of offset and length
  /// references memory outside the bounds of the page.
  /// </exception>
  public ReadOnlySpan<byte> GetReadOnlySpan(int offset, int length)
  {
    // Combined bounds check:
    // 1. Checks offset < 0 (uint cast makes it large)
    // 2. Checks length < 0 (uint cast makes it large)
    // 3. Checks offset + length > Size (including overflow cases if offset+length wraps)
    // This single check correctly validates all boundary conditions.
    if ((uint)offset > Size || (uint)length > (uint)(Size - offset))
    {
      // Provide a detailed error message
      if (offset < 0 || length < 0) // Check explicitly for negative inputs
        throw new ArgumentOutOfRangeException(offset < 0 ? nameof(offset) : nameof(length), "Offset and length must be non-negative.");
      // If inputs are non-negative, the failure must be due to exceeding page size
      else
        throw new ArgumentOutOfRangeException(nameof(length), $"The combination of offset ({offset}) and length ({length}) exceeds the page size ({Size}).");
    }

    // If length is 0, return an empty span. Slicing with 0 length is valid.
    if (length == 0)
    {
      return ReadOnlySpan<byte>.Empty;
    }

    // Return the requested slice of the underlying memory's span.
    // The conversion from Span<byte> to ReadOnlySpan<byte> is implicit and safe.
    return _memory.Span.Slice(offset, length);
  }

  /// <summary>
  /// Reads a single byte from the page at the specified offset.
  /// </summary>
  /// <param name="offset">The zero-based byte offset within the page to read from.</param>
  /// <returns>The byte value read.</returns>
  /// <exception cref="ArgumentOutOfRangeException">
  /// Thrown if the offset is negative or greater than or equal to the page size.
  /// </exception>
  public byte ReadByte(int offset)
  {
    // Bounds Check: Ensure offset is a valid index [0..Size-1]
    if ((uint)offset >= Size) // Same efficient check as WriteByte
    {
      throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) must be within the range [0..{Size - 1}] for a 1-byte read.");
    }

    // Read directly from the span at the index
    return _memory.Span[offset];
  }

  /// <summary>
  /// Reads a 32-bit signed integer (int) from the page at the specified offset,
  /// interpreting the bytes using little-endian format.
  /// </summary>
  /// <param name="offset">The zero-based byte offset within the page to read from.</param>
  /// <returns>The int value read.</returns>
  /// <exception cref="ArgumentOutOfRangeException">
  /// Thrown if the offset is negative or if reading 4 bytes would exceed the page size.
  /// </exception>
  public int ReadInt32(int offset)
  {
    const int valueSize = sizeof(int);

    if ((uint)offset > (Size - valueSize))
    {
      if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) cannot be negative.");
      else throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) plus value size ({valueSize}) exceeds page size ({Size}).");
    }

    ReadOnlySpan<byte> sourceSpan = _memory.Span.Slice(offset, valueSize);
    return BinaryUtilities.ReadInt32LittleEndian(sourceSpan);
  }

  /// <summary>
  /// Reads a 64-bit signed integer (int) from the page at the specified offset,
  /// interpreting the bytes using little-endian format.
  /// </summary>
  /// <param name="offset">The zero-based byte offset within the page to read from.</param>
  /// <returns>The int value read.</returns>
  /// <exception cref="ArgumentOutOfRangeException">
  /// Thrown if the offset is negative or if reading 4 bytes would exceed the page size.
  /// </exception>
  public long ReadInt64(int offset)
  {
    const int valueSize = sizeof(long);

    if ((uint)offset > (Size - valueSize))
    {
      if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) cannot be negative.");
      else throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) plus value size ({valueSize}) exceeds page size ({Size}).");
    }

    ReadOnlySpan<byte> sourceSpan = _memory.Span.Slice(offset, valueSize);
    return BinaryUtilities.ReadInt64LittleEndian(sourceSpan);
  }

  /// <summary>
  /// Writes a single boolean (as a byte) to the page at the specified offset.
  /// </summary>
  /// <param name="offset">The zero-based offset to write to.</param>
  /// <param name="value">The boolean value to write.</param>
  /// <exception cref="ArgumentOutOfRangeException"></exception>
  public void WriteBoolean(int offset, bool value)
  {
    WriteByte(offset, value ? (byte)1 : (byte)0);
  }

  /// <summary>
  /// Writes a DateTime value to the page at the specified offset.
  /// </summary>
  /// <param name="offset">The zero-based offset to write to.</param>
  /// <param name="value">The DateTime value to write.</param>
  public void WriteDateTime(int offset, DateTime value)
  {
    // Use the ToBinary method to maintain the DateTimeKind information.
    WriteInt64(offset, value.ToBinary());
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
  /// Writes a span of bytes to the page at the specified offset.
  /// </summary>
  /// <param name="offset">The zero-based offset within the page to write to.</param>
  /// <param name="source">The source span of bytes to write.</param>
  /// <exception cref="ArgumentOutOfRangeException"></exception>
  public void WriteBytes(int offset, ReadOnlySpan<byte> source)
  {
    if ((uint)offset > (Size - source.Length)) // Efficient check for offset < 0 or offset + source.Length > Size
    {
      if (offset < 0)
        throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) cannot be negative.");
      else
        throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) plus source length ({source.Length}) exceeds page size ({Size}).");
    }

    Span<byte> destination = _memory.Span.Slice(offset, source.Length);
    source.CopyTo(destination);
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