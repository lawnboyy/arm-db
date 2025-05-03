using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class PageTests
{
  [Theory]
  // Offset, Value to Write
  [InlineData(0, (byte)0xCA)]         // Write at the beginning
  [InlineData(1024, (byte)0xFE)]      // Write somewhere in the middle
  [InlineData(Page.Size - 1, (byte)0x42)] // Write at the very last valid position
  [InlineData(500, (byte)0x00)]       // Write a zero byte
  [InlineData(600, byte.MaxValue)] // Write max byte value
  public void WriteByte_ValidOffset_WritesCorrectByte(int offset, byte valueToWrite)
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    // Optional: Fill buffer with a known different value first
    Array.Fill(buffer, (byte)0xEE);

    // Act
    page.WriteByte(offset, valueToWrite);

    // Assert
    // Check that the specific byte at the offset was updated correctly
    Assert.Equal(valueToWrite, buffer[offset]);
  }

  [Fact]
  public void WriteByte_OverwritesExistingData_DoesNotAffectNeighbors()
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    int offset = 200;
    byte initialNeighborBefore = 0xAA;
    byte initialValue = 0xBB;
    byte initialNeighborAfter = 0xCC;
    byte valueToWrite = 0xDD; // The new value

    buffer[offset - 1] = initialNeighborBefore;
    buffer[offset] = initialValue;
    buffer[offset + 1] = initialNeighborAfter;

    // Act
    page.WriteByte(offset, valueToWrite);

    // Assert
    Assert.Equal(valueToWrite, buffer[offset]);        // Verify the byte was written
    Assert.Equal(initialNeighborBefore, buffer[offset - 1]); // Verify byte before is unchanged
    Assert.Equal(initialNeighborAfter, buffer[offset + 1]);  // Verify byte after is unchanged
  }

  [Theory]
  // Invalid Offset Values
  [InlineData(-1)]           // Negative offset
  [InlineData(Page.Size)]    // Exactly one byte beyond the end (invalid index)
  [InlineData(Page.Size + 100)]// Far beyond the end
  [InlineData(int.MinValue)] // Extreme negative
  public void WriteByte_InvalidOffset_ThrowsArgumentOutOfRangeException(int invalidOffset)
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    byte valueToWrite = 0x99;

    // Act & Assert
    var ex = Assert.Throws<ArgumentOutOfRangeException>("offset", () => page.WriteByte(invalidOffset, valueToWrite));
    // Optional: Check message for clarity
    Assert.Contains($"Offset ({invalidOffset}) is not within the range [0..{Page.Size - 1}]", ex.Message);
  }

  // --- Test Data for Valid WriteBytes ---
  public static IEnumerable<object[]> ValidWriteBytesData =>
      new List<object[]>
      {
            // Offset, Source Data, Expected Slice (should match source)
            new object[] { 0, new byte[] { 0xDE, 0xAD }, new byte[] { 0xDE, 0xAD } }, // Write at start
            new object[] { 500, new byte[] { 1, 2, 3, 4, 5 }, new byte[] { 1, 2, 3, 4, 5 } }, // Write in middle
            new object[] { Page.Size - 3, new byte[] { 0xAA, 0xBB, 0xCC }, new byte[] { 0xAA, 0xBB, 0xCC } }, // Write exactly at end
            new object[] { 0, new byte[Page.Size], new byte[Page.Size] }, // Write full page (check length mainly)
            new object[] { 0, Array.Empty<byte>(), Array.Empty<byte>() }, // Write empty array at start (no-op)
            new object[] { Page.Size / 2, Array.Empty<byte>(), Array.Empty<byte>() }, // Write empty array in middle (no-op)
            new object[] { Page.Size, Array.Empty<byte>(), Array.Empty<byte>() }, // Write empty array *at* end (offset == Size is valid for zero length)
      };

  [Theory]
  [MemberData(nameof(ValidWriteBytesData))]
  public void WriteBytes_ValidOffsetAndData_WritesCorrectBytes(int offset, byte[] sourceData, byte[] expectedSlice)
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    var bufferOriginal = buffer.ToArray(); // For checking no-op cases

    // Act
    page.WriteBytes(offset, sourceData.AsSpan());

    // Assert
    if (expectedSlice.Length > 0)
    {
      // Extract the slice from the buffer where data should have been written
      var writtenSlice = buffer.AsSpan(offset, sourceData.Length).ToArray();
      Assert.Equal(expectedSlice, writtenSlice); // Compare byte content
    }
    else
    {
      // If source was empty, ensure buffer is unchanged
      Assert.Equal(bufferOriginal, buffer);
    }
  }

  [Fact]
  public void WriteBytes_OverwritesExistingData_DoesNotAffectNeighbors()
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    int offset = 400;
    byte[] sourceData = { 0x11, 0x22, 0x33, 0x44 };
    byte initialBefore = 0xAA;
    byte initialAfter = 0xBB;

    // Pre-fill surrounding area
    Array.Fill(buffer, (byte)0xFF); // Fill page first
    buffer[offset - 1] = initialBefore; // Set byte before target area
    buffer[offset + sourceData.Length] = initialAfter; // Set byte after target area

    // Act
    page.WriteBytes(offset, sourceData.AsSpan());

    // Assert
    // Check written data
    var writtenSlice = buffer.AsSpan(offset, sourceData.Length).ToArray();
    Assert.Equal(sourceData, writtenSlice);
    // Check neighbors are untouched
    Assert.Equal(initialBefore, buffer[offset - 1]);
    Assert.Equal(initialAfter, buffer[offset + sourceData.Length]);
  }

  // --- Test Data for Invalid WriteBytes ---
  public static IEnumerable<object[]> InvalidWriteBytesData =>
     new List<object[]>
     {
            // Offset, Source Data Length
            new object[] { -1, 10 },             // Negative offset
            new object[] { 0, Page.Size + 1 },   // Data longer than page
            new object[] { 1, Page.Size },       // Offset + Data longer than page
            new object[] { Page.Size - 5, 6 },   // Starts ok, extends exactly 1 byte beyond end
            new object[] { Page.Size, 1 },       // Starts exactly at end (needs 1 byte, 0 available)
            new object[] { Page.Size + 1, 1 },   // Starts beyond end
            new object[] { int.MinValue, 1 },    // Extreme negative offset
            new object[] { Page.Size + 1, 0 }    // Writing zero bytes PAST Page.Size is invalid offset
     };


  [Theory]
  [MemberData(nameof(InvalidWriteBytesData))]
  public void WriteBytes_InvalidOffsetOrLength_ThrowsArgumentOutOfRangeException(int offset, int length)
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    // Create dummy source data - content doesn't matter, only length
    // Ensure length is not negative for array creation itself
    byte[] sourceData = new byte[Math.Max(0, length)];

    // Act & Assert
    // The exception should be ArgumentOutOfRangeException based on our bounds check logic
    Assert.Throws<ArgumentOutOfRangeException>(() => page.WriteBytes(offset, sourceData.AsSpan()));
    // We won't check ParamName as the exception source might be complex (offset check, length check, slice)
  }

  [Fact]
  public void WriteBytes_SourceLengthLargerThanPageSize_ThrowsArgumentOutOfRangeException()
  {
    // Arrange
    var (page, buffer) = CreateTestPage();
    int offset = 10; // A valid starting offset
    // Create source data that is explicitly larger than the entire page
    byte[] sourceData = new byte[Page.Size + 100]; // e.g., 8192 + 100 = 8292 bytes

    // Act & Assert
    Assert.Throws<ArgumentOutOfRangeException>(() => page.WriteBytes(offset, sourceData.AsSpan()));
  }

  [Fact]
  public void WriteBoolean_CorrectlyWritesBooleanAtOffset()
  {
    var (page, buffer) = CreateTestPage();

    // Write true at offset 10
    int offset = 10;
    bool valueToWrite = true;

    page.WriteBoolean(offset, valueToWrite);
    Assert.Equal((byte)1, buffer[offset]);

    // Write false at offset 20
    offset = 20;
    valueToWrite = false;
    page.WriteBoolean(offset, valueToWrite);
    Assert.Equal((byte)0, buffer[offset]);

    // Overwrite existing value at offset 10 with false
    offset = 10;
    valueToWrite = false;
    page.WriteBoolean(offset, valueToWrite);
    Assert.Equal((byte)0, buffer[offset]);
  }
}