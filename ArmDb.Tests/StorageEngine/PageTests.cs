using ArmDb.StorageEngine; // Target namespace

namespace ArmDb.UnitTests.StorageEngine; // Test namespace

public class PageTests
{
  // Helper to create a test page with a writeable buffer
  private static (Page page, byte[] buffer) CreateTestPage(long pageId = 0)
  {
    var buffer = new byte[Page.Size]; // Standard size
    var memory = new Memory<byte>(buffer);
    var page = new Page(pageId, memory);
    return (page, buffer);
  }

  [Theory]
  // Offset, Value, Expected Bytes (Little Endian)
  [InlineData(0, 0x12345678, new byte[] { 0x78, 0x56, 0x34, 0x12 })] // Simple positive
  [InlineData(100, -1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]      // Simple negative (-1)
  [InlineData(0, 0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]          // Zero
  [InlineData(2048, int.MaxValue, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })] // Max Value
  [InlineData(4000, int.MinValue, new byte[] { 0x00, 0x00, 0x00, 0x80 })] // Min Value
  [InlineData(Page.Size - 4, -1412567295 /* 0xABCDEF01 */, new byte[] { 0x01, 0xEF, 0xCD, 0xAB })] // At the very end
  public void WriteInt32_ValidOffsetAndValue_WritesCorrectBytesLittleEndian(int offset, int valueToWrite, byte[] expectedBytes)
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    // Assumption check for test validity (most platforms are little-endian)
    Assert.True(BitConverter.IsLittleEndian, "Test assumes Little Endian platform for byte comparison.");

    // Act
    page.WriteInt32(offset, valueToWrite);

    // Assert
    // Extract the 4 bytes written from the buffer
    var writtenBytes = buffer.AsSpan(offset, sizeof(int)).ToArray();
    Assert.Equal(expectedBytes, writtenBytes); // Compare written bytes with expected bytes
  }

  [Fact]
  public void WriteInt32_OverwritesExistingData()
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    int offset = 50;
    // Pre-fill buffer with non-zero data
    Array.Fill(buffer, (byte)0xFF);
    int valueToWrite = 0x11223344;
    byte[] expectedBytes = { 0x44, 0x33, 0x22, 0x11 }; // Little Endian

    // Act
    page.WriteInt32(offset, valueToWrite);

    // Assert
    // Check surrounding bytes are untouched
    Assert.Equal((byte)0xFF, buffer[offset - 1]);
    Assert.Equal((byte)0xFF, buffer[offset + sizeof(int)]);
    // Check written bytes
    var writtenBytes = buffer.AsSpan(offset, sizeof(int)).ToArray();
    Assert.Equal(expectedBytes, writtenBytes);
  }

  [Theory]
  [InlineData(-1)] // Negative offset
  [InlineData(Page.Size - 3)] // Offset too large (needs 4 bytes, only 3 remain)
  [InlineData(Page.Size)] // Offset exactly at the end (needs 4 bytes, 0 remain)
  [InlineData(Page.Size + 1)] // Offset beyond the end
  [InlineData(int.MinValue)] // Extreme negative
  public void WriteInt32_InvalidOffset_ThrowsArgumentOutOfRangeException(int invalidOffset)
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    int valueToWrite = 123;

    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("offset", () => page.WriteInt32(invalidOffset, valueToWrite));
  }

  [Theory]
  // Offset, Value, Expected Bytes (Little Endian for long)
  [InlineData(0, 0x1122334455667788L, new byte[] { 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11 })]
  [InlineData(500, -1L, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
  [InlineData(0, 0L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
  [InlineData(1024, long.MaxValue, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F })]
  [InlineData(2048, long.MinValue, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80 })]
  // Write at the last possible valid offset for an 8-byte value
  [InlineData(Page.Size - 8, -6144092013047381999 /* Hex: 0xAABBCCDDEEFF0011 */, new byte[] { 0x11, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA })]
  public void WriteInt64_ValidOffsetAndValue_WritesCorrectBytesLittleEndian(int offset, long valueToWrite, byte[] expectedBytes)
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    Assert.True(BitConverter.IsLittleEndian, "Test assumes Little Endian platform for byte comparison.");

    // Act
    page.WriteInt64(offset, valueToWrite);

    // Assert
    // Extract the 8 bytes written from the buffer
    var writtenBytes = buffer.AsSpan(offset, sizeof(long)).ToArray();
    Assert.Equal(expectedBytes, writtenBytes); // Compare written bytes with expected bytes
  }

  [Fact]
  public void WriteInt64_OverwritesExistingData()
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    int offset = 120;
    // Pre-fill buffer with non-zero data
    Array.Fill(buffer, (byte)0xCC);
    long valueToWrite = 0x0102030405060708L;
    byte[] expectedBytes = { 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01 }; // Little Endian

    // Act
    page.WriteInt64(offset, valueToWrite);

    // Assert
    // Check surrounding bytes are untouched
    Assert.Equal((byte)0xCC, buffer[offset - 1]);
    Assert.Equal((byte)0xCC, buffer[offset + sizeof(long)]); // Check byte after the written long
                                                             // Check written bytes
    var writtenBytes = buffer.AsSpan(offset, sizeof(long)).ToArray();
    Assert.Equal(expectedBytes, writtenBytes);
  }

  [Theory]
  [InlineData(-1)] // Negative offset
  [InlineData(Page.Size - 7)] // Offset too large (needs 8 bytes, only 7 remain)
  [InlineData(Page.Size - 1)] // Offset too large (needs 8 bytes, only 1 remains)
  [InlineData(Page.Size)]     // Offset exactly at the end (needs 8 bytes, 0 remain)
  [InlineData(Page.Size + 1)] // Offset beyond the end
  [InlineData(int.MinValue)]  // Extreme negative offset
  public void WriteInt64_InvalidOffset_ThrowsArgumentOutOfRangeException(int invalidOffset)
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    long valueToWrite = 9876543210L;

    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>("offset", () => page.WriteInt64(invalidOffset, valueToWrite));
  }
}