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
}