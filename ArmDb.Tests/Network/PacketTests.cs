using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using ArmDb.Network;
using Xunit;

namespace ArmDb.UnitTests.Network;

public class PacketTests
{
  [Fact]
  public async Task WritePacketAsync_ConnectPacket_WritesCorrectBytes()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var packet = new ConnectPacket(1);

    // Act
    await writer.WritePacketAsync(packet);

    // Assert
    stream.Position = 0;
    var buffer = stream.ToArray();

    // 1. Check Total Length (4 bytes for Protocol Version + 5 bytes header = 9 bytes)
    Assert.Equal(9, buffer.Length);

    // 2. Check Type
    Assert.Equal((byte)PacketType.Connect, buffer[0]);

    // 3. Check Length Field (Big Endian Int32)
    int length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1, 4));
    Assert.Equal(4, length);

    // 4. Check Payload (Protocol Version - Big Endian Int32)
    int version = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(5, 4));
    Assert.Equal(1, version);
  }
}