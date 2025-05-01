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
}