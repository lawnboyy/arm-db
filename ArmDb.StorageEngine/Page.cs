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
    if ((uint)offset > (uint)(Size - valueSize)) // Efficient check for offset < 0 or offset + valueSize > Size
    {
      if (offset < 0)
        throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) cannot be negative.");
      else
        throw new ArgumentOutOfRangeException(nameof(offset), $"Offset ({offset}) plus value size ({valueSize}) exceeds page size ({Size}).");
    }

    // 2. Get Span and Write
    Span<byte> destination = _memory.Span.Slice(offset, valueSize);
    WriteInt32LittleEndian(destination, value);
  }

  private void WriteInt32LittleEndian(Span<byte> destination, int value)
  {
    const int intSize = sizeof(int);
    if (intSize > destination.Length)
    {
      throw new ArgumentOutOfRangeException(nameof(destination), $"Destination span length ({destination.Length}) is less than the size of an int ({intSize}).");
    }

    int valueToWrite = value;

    // This won't get called on Windows since it's already little-endian.
    if (!BitConverter.IsLittleEndian)
    {
      // If the system is not little-endian, reverse the endianness of the value.
      valueToWrite = ReverseEndianness(valueToWrite); // Reverse the endianness of the int value.
    }

    MemoryMarshal.Write(destination, in valueToWrite); // This will write the 4 bytes in the correct order.
  }

  private static int ReverseEndianness(int value)
  {
    // Work with unsigned integer to guarantee logical right shifts
    uint uval = (uint)value;

    // Isolate and shift each byte to its new position
    // 1. LSB (Byte 0) -> becomes MSB (Byte 3)
    uint byte0 = (uval & 0x000000FF) << 24;

    // 2. Byte 1 -> becomes Byte 2
    uint byte1 = (uval & 0x0000FF00) << 8; // Isolate byte 1 and shift left 8

    // 3. Byte 2 -> becomes Byte 1
    uint byte2 = (uval & 0x00FF0000) >> 8; // Isolate byte 2 and shift right 8

    // 4. MSB (Byte 3) -> becomes LSB (Byte 0)
    uint byte3 = (uval & 0xFF000000) >> 24; // Isolate byte 3 and shift right 24                                            

    // Combine the rearranged bytes using bitwise OR
    uint result = byte0 | byte1 | byte2 | byte3;

    // Cast back to signed int for the final result
    return (int)result;
  }
}