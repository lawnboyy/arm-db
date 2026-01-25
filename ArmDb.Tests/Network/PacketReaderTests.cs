using ArmDb.Network;

namespace ArmDb.UnitTests.Network;

public class PacketReaderTests
{
  [Fact]
  public async Task ReadPacketAsync_ConnectPacket_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var originalPacket = new ConnectPacket(12345);

    // Write to stream
    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0; // Rewind for reading

    // Act
    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    Assert.IsType<ConnectPacket>(readPacket);
    var connectPacket = (ConnectPacket)readPacket;
    Assert.Equal(originalPacket.ProtocolVersion, connectPacket.ProtocolVersion);
  }

  [Fact]
  public async Task ReadPacketAsync_AuthenticationOkPacket_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var originalPacket = new AuthenticationOkPacket();

    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0;

    // Act
    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    Assert.IsType<AuthenticationOkPacket>(readPacket);
  }

  [Fact]
  public async Task ReadPacketAsync_ReadyForQueryPacket_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var originalPacket = new ReadyForQueryPacket(TransactionStatus: 'I');

    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0;

    // Act
    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    Assert.IsType<ReadyForQueryPacket>(readPacket);
    var readyPacket = (ReadyForQueryPacket)readPacket;
    Assert.Equal(originalPacket.TransactionStatus, readyPacket.TransactionStatus);
  }

  [Fact]
  public async Task ReadPacketAsync_TerminatePacket_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var originalPacket = new TerminatePacket();

    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0;

    // Act
    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    Assert.IsType<TerminatePacket>(readPacket);
  }
}