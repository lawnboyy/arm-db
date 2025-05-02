using System.Runtime.InteropServices;

namespace ArmDb.StorageEngine;

public static class BinaryUtilities
{
  public static void WriteInt32LittleEndian(Span<byte> destination, int value)
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

  public static void WriteInt64LittleEndian(Span<byte> destination, long value)
  {
    const int longSize = sizeof(long);
    if (longSize > destination.Length)
    {
      throw new ArgumentOutOfRangeException(nameof(destination), $"Destination span length ({destination.Length}) is less than the size of a long ({longSize}).");
    }

    long valueToWrite = value;

    // This won't get called on Windows since it's already little-endian.
    if (!BitConverter.IsLittleEndian)
    {
      // If the system is not little-endian, reverse the endianness of the value.
      valueToWrite = ReverseEndianness(valueToWrite); // Reverse the endianness of the long value.
    }

    MemoryMarshal.Write(destination, in valueToWrite); // This will write the 8 bytes in the correct order.
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

  private static long ReverseEndianness(long value)
  {
    // Work with unsigned long to guarantee logical right shifts
    ulong uval = (ulong)value;

    // Isolate and shift each byte to its new position
    // 1. LSB (Byte 0) -> becomes MSB (Byte 7)
    ulong byte0 = (uval & 0x00000000000000FF) << 56;

    // 2. Byte 1 -> becomes Byte 6
    ulong byte1 = (uval & 0x000000000000FF00) << 40; // Isolate byte 1 and shift left 40

    // 3. Byte 2 -> becomes Byte 5
    ulong byte2 = (uval & 0x00000000FF000000) << 24; // Isolate byte 2 and shift left 24

    // 4. Byte 3 -> becomes Byte 4
    ulong byte3 = (uval & 0x00FF000000000000) << 8; // Isolate byte 3 and shift left 8

    // 5. MSB (Byte 7) -> becomes LSB (Byte 0)
    ulong byte7 = (uval & 0xFF00000000000000) >> 56; // Isolate byte 7 and shift right 56

    // Combine the rearranged bytes using bitwise OR
    ulong result = byte0 | byte1 | byte2 | byte3 | byte7;

    // Cast back to signed long for the final result
    return (long)result;
  }
}