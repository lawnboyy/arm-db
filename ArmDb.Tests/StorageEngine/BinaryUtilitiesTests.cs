using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public class BinaryUtilitiesTests
{
  [Theory]
  // Input Value          Expected Reversed Value (Hex literals used for clarity)
  [InlineData(0x12345678, 0x78563412)] // Standard pattern
  [InlineData(0, 0)]                   // Zero
  [InlineData(-1, -1)]                 // All bits set (0xFFFFFFFF -> 0xFFFFFFFF)
  [InlineData(int.MaxValue, unchecked((int)0xFFFFFF7F))] // 0x7FFFFFFF -> 0xFFFFFF7F
  [InlineData(int.MinValue, 0x00000080)]                 // 0x80000000 -> 0x00000080
  [InlineData(0x00000001, 0x01000000)]                   // LSB only set
  [InlineData(0x01000000, 0x00000001)]                   // Byte 1 only set (becomes LSB)
  [InlineData(unchecked((int)0xABCDEF01), 0x01EFCDAB)]   // Negative pattern
  [InlineData(0x00FF00FF, unchecked((int)0xFF00FF00))]   // Alternating bytes
  public void ReverseEndianness_Int32_ReversesBytesCorrectly(int inputValue, int expectedValue)
  {
    // Act
    int result = BinaryUtilities.ReverseEndianness(inputValue);

    // Assert
    Assert.Equal(expectedValue, result);
    // Hex strings for clear failure messages.
    Assert.Equal($"{expectedValue:X8}", $"{result:X8}");
  }

  [Theory]
  // Input Value                    Expected Reversed Value (Hex literals used for clarity)
  [InlineData(0x1122334455667788L, unchecked((long)0x8877665544332211L))] // Standard pattern
  [InlineData(0L, 0L)]                                                    // Zero
  [InlineData(-1L, -1L)]                                                  // All bits set (0xFFFF...FFFF)
  [InlineData(long.MaxValue, unchecked((long)0xFFFFFFFFFFFFFF7FL))]      // 0x7FFF...FFFF -> 0xFFFF...FF7F
  [InlineData(long.MinValue, 0x0000000000000080L)]                        // 0x8000...0000 -> 0x0000...0080
  [InlineData(0x0000000000000001L, unchecked((long)0x0100000000000000L))]  // LSB only set
  [InlineData(unchecked((long)0x0100000000000000L), 0x0000000000000001L)]  // Byte 1 only set (becomes LSB)
  [InlineData(unchecked((long)0xAABBCCDDEEFF0011L), unchecked((long)0x1100FFEEDDCCBBAAL))] // Negative pattern previously discussed
  [InlineData(0x00FF00FF00FF00FFL, unchecked((long)0xFF00FF00FF00FF00L))] // Alternating bytes
  public void ReverseEndianness_Int64_ReversesBytesCorrectly(long inputValue, long expectedValue)
  {
    // Act
    // Accessing internal static method directly because InternalsVisibleTo is configured
    long result = BinaryUtilities.ReverseEndianness(inputValue);

    // Assert
    Assert.Equal(expectedValue, result);
    // Hex strings for clear failure messages.
    Assert.Equal($"{expectedValue:X16}", $"{result:X16}");
  }
}