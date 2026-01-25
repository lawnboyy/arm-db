using System.Buffers.Binary;
using System.Net;
using System.Text;
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

  [Fact]
  public async Task ReadPacketAsync_CommandCompletePacket_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var originalPacket = new CommandCompletePacket(Tag: "INSERT 1");

    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0;

    // Act
    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    Assert.IsType<CommandCompletePacket>(readPacket);
    var commandCompletePacket = (CommandCompletePacket)readPacket;
    Assert.Equal(originalPacket.Tag, commandCompletePacket.Tag);
  }

  [Fact]
  public async Task ReadPacketAsync_MissingNullTerminator_ThrowsProtocolViolationException()
  {
    // Arrange
    var stream = new MemoryStream();

    // Manually construct a malformed packet (CommandComplete)
    // 1. Type 'C'
    stream.WriteByte((byte)PacketType.CommandComplete);

    // 2. Payload: "Tag" (3 bytes) WITHOUT null terminator
    byte[] tagBytes = Encoding.UTF8.GetBytes("Tag");

    // 3. Length = 4 (for length field itself) + 3 (payload) = 7
    byte[] lengthBytes = new byte[4];
    BinaryPrimitives.WriteInt32BigEndian(lengthBytes, 3); // Payload Length
    stream.Write(lengthBytes);

    // 4. Write Payload (Missing \0)
    stream.Write(tagBytes);

    stream.Position = 0;
    var reader = new PacketReader(stream);

    // Act & Assert
    await Assert.ThrowsAsync<ProtocolViolationException>(() => reader.ReadPacketAsync());
  }

  [Fact]
  public async Task ReadPacketAsync_DataRowPacket_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);

    // Construct expected values
    var val1 = new byte[4]; BinaryPrimitives.WriteInt32BigEndian(val1, 42); // Int
    var val3 = Encoding.UTF8.GetBytes("TestString"); // String (Writer handles encoding, but here we prep bytes)

    var values = new List<byte[]?> { val1, null, val3 };
    var originalPacket = new DataRowPacket(values);

    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0;

    // Act
    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    Assert.IsType<DataRowPacket>(readPacket);
    var dataRowPacket = (DataRowPacket)readPacket;

    Assert.Equal(3, dataRowPacket.Values.Count);

    // Value 1: Int
    Assert.NotNull(dataRowPacket.Values[0]);
    Assert.Equal(val1, dataRowPacket.Values[0]);

    // Value 2: NULL
    Assert.Null(dataRowPacket.Values[1]);

    // Value 3: String
    Assert.NotNull(dataRowPacket.Values[2]);
    Assert.Equal(val3, dataRowPacket.Values[2]);
  }
}