using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ArmDb.StorageEngine;

public static class BinaryUtilities
{
  /// <summary>
  /// Reads a 32 bit integer from the source span. It will only read the first 4 bytes.
  /// </summary>
  /// <param name="source"></param>
  /// <returns></returns>
  /// <exception cref="ArgumentOutOfRangeException"></exception>
  public static int ReadInt32LittleEndian(ReadOnlySpan<byte> source)
  {
    const int intSize = sizeof(int);
    if (source.Length < intSize)
    {
      throw new ArgumentOutOfRangeException(nameof(source), $"Source size ({source.Length}) is less than the size of an int ({intSize}).");
    }

    int value = MemoryMarshal.Read<int>(source);
    if (!BitConverter.IsLittleEndian)
    {
      value = ReverseEndianness(value);
    }

    return value;
  }

  /// <summary>
  /// Reads a 64 bit integer from the source span. It will only read the first 8 bytes.
  /// </summary>
  /// <param name="source"></param>
  /// <returns></returns>
  /// <exception cref="ArgumentOutOfRangeException"></exception>
  public static long ReadInt64LittleEndian(ReadOnlySpan<byte> source)
  {
    const int longSize = sizeof(long);
    if (source.Length < longSize)
    {
      throw new ArgumentOutOfRangeException(nameof(source), $"Source size ({source.Length}) is less than the size of an int ({longSize}).");
    }

    var value = MemoryMarshal.Read<long>(source);
    if (!BitConverter.IsLittleEndian)
    {
      value = ReverseEndianness(value);
    }

    return value;
  }

  public static void WriteInt32LittleEndian(Span<byte> destination, int value)
  {
    const int intSize = sizeof(int);
    if (intSize > destination.Length)
    {
      throw new ArgumentOutOfRangeException(nameof(destination), $"Destination span length ({destination.Length}) is less than the size of an int ({intSize}).");
    }

    int valueToWrite = value;
    if (!BitConverter.IsLittleEndian)
    {
      valueToWrite = ReverseEndianness(valueToWrite); // Calls internal method
    }
    MemoryMarshal.Write(destination, in valueToWrite);
  }

  public static void WriteInt64LittleEndian(Span<byte> destination, long value)
  {
    const int longSize = sizeof(long);
    if (longSize > destination.Length)
    {
      throw new ArgumentOutOfRangeException(nameof(destination), $"Destination span length ({destination.Length}) is less than the size of a long ({longSize}).");
    }

    long valueToWrite = value;
    if (!BitConverter.IsLittleEndian)
    {
      valueToWrite = ReverseEndianness(valueToWrite); // Calls internal method
    }
    MemoryMarshal.Write(destination, in valueToWrite);
  }

  /// <summary>
  /// Reverses the byte order (endianness) of a 32-bit signed integer.
  /// Marked internal for testing accessibility via InternalsVisibleTo.
  /// </summary>
  internal static int ReverseEndianness(int value)
  {
    // Work with unsigned integer to guarantee logical right shifts
    uint uval = (uint)value;

    // Isolate and shift each byte to its new position
    uint byte3 = (uval >> 24);              // Byte 3 -> Byte 0 position
    uint byte2 = (uval >> 8) & 0x0000FF00;  // Byte 2 -> Byte 1 position
    uint byte1 = (uval << 8) & 0x00FF0000;  // Byte 1 -> Byte 2 position
    uint byte0 = (uval << 24);              // Byte 0 -> Byte 3 position

    // Combine the rearranged bytes using bitwise OR
    uint result = byte0 | byte1 | byte2 | byte3;

    // Cast back to signed int for the final result
    return (int)result;
  }

  /// <summary>
  /// Reverses the byte order (endianness) of a 64-bit signed integer.
  /// Marked internal for testing accessibility via InternalsVisibleTo.
  /// Includes correction for handling all 8 bytes.
  /// </summary>
  internal static long ReverseEndianness(long value)
  {
    // Work with unsigned long to guarantee logical right shifts
    ulong uval = (ulong)value;

    // Isolate and shift each byte to its new position
    // Use UL suffix for hex constants to ensure ulong operations
    ulong byte7 = uval >> 56;                         // Byte 7 -> Byte 0
    ulong byte6 = (uval >> 40) & 0x000000000000FF00UL; // Byte 6 -> Byte 1
    ulong byte5 = (uval >> 24) & 0x0000000000FF0000UL; // Byte 5 -> Byte 2
    ulong byte4 = (uval >> 8) & 0x00000000FF000000UL;  // Byte 4 -> Byte 3
    ulong byte3 = (uval << 8) & 0x000000FF00000000UL;  // Byte 3 -> Byte 4
    ulong byte2 = (uval << 24) & 0x0000FF0000000000UL; // Byte 2 -> Byte 5
    ulong byte1 = (uval << 40) & 0x00FF000000000000UL; // Byte 1 -> Byte 6
    ulong byte0 = uval << 56;                         // Byte 0 -> Byte 7

    // Combine the rearranged bytes using bitwise OR
    ulong result = byte0 | byte1 | byte2 | byte3 | byte4 | byte5 | byte6 | byte7;

    // Cast back to signed long for the final result
    return (long)result;
  }

  /// <summary>
  /// Converts a 16-byte read-only span, representing a decimal in little-endian format,
  /// into a decimal value.
  /// </summary>
  /// <param name="source">A read-only span of bytes. Must be at least 16 bytes long.</param>
  /// <returns>The reconstructed decimal value.</returns>
  /// <exception cref="ArgumentException">Thrown if source span is shorter than 16 bytes.</exception>
  internal static decimal ConvertSpanToDecimal(ReadOnlySpan<byte> source)
  {
    const int decimalSize = sizeof(decimal); // 16 bytes
    if (source.Length < decimalSize)
    {
      throw new ArgumentException($"Source span must be at least {decimalSize} bytes long to convert to a decimal.", nameof(source));
    }

    // A decimal is composed of four 32-bit integers. We read each one,
    // assuming they were written in little-endian order.
    int lo = ReadInt32LittleEndian(source.Slice(0, 4));
    int mid = ReadInt32LittleEndian(source.Slice(4, 4));
    int hi = ReadInt32LittleEndian(source.Slice(8, 4));
    int flags = ReadInt32LittleEndian(source.Slice(12, 4));

    // Use the constructor that takes the four parts in an array.
    return new decimal([lo, mid, hi, flags]);
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  internal static bool ToBoolean(ReadOnlySpan<byte> value)
  {
    if (value.Length < sizeof(byte))
      ArgumentOutOfRangeException.ThrowIfLessThan(value.Length, sizeof(byte));
    return Unsafe.ReadUnaligned<byte>(ref MemoryMarshal.GetReference(value)) != 0;
  }
}