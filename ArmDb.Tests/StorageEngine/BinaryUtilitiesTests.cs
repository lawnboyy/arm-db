using ArmDb.Storage;

namespace ArmDb.UnitTests.Storage;

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

  [Theory]
  // Expected Value        Input Little-Endian Bytes
  [InlineData(0x12345678, new byte[] { 0x78, 0x56, 0x34, 0x12 })]
  [InlineData(0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
  [InlineData(-1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
  [InlineData(int.MaxValue, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })] // 0x7FFFFFFF
  [InlineData(int.MinValue, new byte[] { 0x00, 0x00, 0x00, 0x80 })] // 0x80000000
  [InlineData(unchecked((int)0xABCDEF01), new byte[] { 0x01, 0xEF, 0xCD, 0xAB })] // Test specific negative pattern
  [InlineData(1, new byte[] { 0x01, 0x00, 0x00, 0x00 })]
  [InlineData(256, new byte[] { 0x00, 0x01, 0x00, 0x00 })] // 0x0100
  public void ReadInt32LittleEndian_ValidSpan_ReturnsCorrectValue(int expectedValue, byte[] sourceBytes)
  {
    // Arrange
    ReadOnlySpan<byte> sourceSpan = sourceBytes.AsSpan();

    // Act
    // Assumes the method correctly handles endianness internally if needed
    int result = BinaryUtilities.ReadInt32LittleEndian(sourceSpan);

    // Assert
    Assert.Equal(expectedValue, result);
  }

  [Theory]
  [InlineData(0)] // Empty span
  [InlineData(1)] // Too short
  [InlineData(2)] // Too short
  [InlineData(3)] // Too short
  public void ReadInt32LittleEndian_SpanTooShort_ThrowsArgumentOutOfRangeException(int length)
  {
    // Arrange
    byte[] sourceBytes = new byte[length];

    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("source", () => BinaryUtilities.ReadInt32LittleEndian(sourceBytes));
  }

  [Fact]
  public void ReadInt32LittleEndian_ReadsFromCorrectSpanLocation()
  {
    // Arrange
    // Create a buffer larger than needed, place data in the middle
    byte[] buffer = { 0xFF, 0xFF, 0x78, 0x56, 0x34, 0x12, 0xFF, 0xFF };
    int expectedValue = 0x12345678;
    int offset = 2; // Start reading from index 2
    int length = sizeof(int);
    ReadOnlySpan<byte> sourceSpan = buffer.AsSpan(offset, length); // Pass only the relevant slice

    // Act
    int result = BinaryUtilities.ReadInt32LittleEndian(sourceSpan);

    // Assert
    Assert.Equal(expectedValue, result);
  }

  [Theory]
  // Expected Value                       Input Little-Endian Bytes (8 bytes)
  [InlineData(0x1122334455667788L, new byte[] { 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11 })]
  [InlineData(0L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
  [InlineData(-1L, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
  [InlineData(long.MaxValue, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
  [InlineData(long.MinValue, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80 })]
  [InlineData(unchecked((long)0xAABBCCDDEEFF0011L), new byte[] { 0x11, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA })]
  [InlineData(1L, new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
  [InlineData(256L, new byte[] { 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
  [InlineData(65536L, new byte[] { 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00 })]
  public void ReadInt64LittleEndian_ValidSpan_ReturnsCorrectValue(long expectedValue, byte[] sourceBytes)
  {
    // Arrange
    ReadOnlySpan<byte> sourceSpan = sourceBytes.AsSpan();

    // Act
    long result = BinaryUtilities.ReadInt64LittleEndian(sourceSpan);

    // Assert
    Assert.Equal(expectedValue, result);
  }

  [Theory]
  [InlineData(0)] // Empty span
  [InlineData(1)] // Too short
  [InlineData(4)] // Too short
  [InlineData(7)] // Too short
  public void ReadInt64LittleEndian_SpanTooShort_ThrowsArgumentOutOfRangeException(int length)
  {
    // Arrange
    byte[] sourceBytes = new byte[length];

    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("source", () => BinaryUtilities.ReadInt64LittleEndian(sourceBytes));
  }

  [Fact]
  public void ReadInt64LittleEndian_ReadsFromCorrectSpanLocation()
  {
    // Arrange
    byte[] buffer = { 0xFF, 0xFF, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0xFF, 0xFF };
    long expectedValue = 0x1122334455667788L;
    int offset = 2;
    int length = sizeof(long);
    ReadOnlySpan<byte> sourceSpan = buffer.AsSpan(offset, length); // Pass only the relevant slice

    // Act
    long result = BinaryUtilities.ReadInt64LittleEndian(sourceSpan);

    // Assert
    Assert.Equal(expectedValue, result);
  }
}