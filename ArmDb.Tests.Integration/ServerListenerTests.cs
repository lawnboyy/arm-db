using System;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;
using ArmDb.Common.Utils;
using ArmDb.Network;
using Xunit;

namespace ArmDb.IntegrationTests;

public class ServerTests
{
  [Fact]
  public async Task Server_Handshake_ReturnsReadyForQuery()
  {
    // Arrange
    // Create the real file system (or a mock if you preferred isolation)
    var fileSystem = new FileSystem();

    await using var server = new TestServer(fileSystem);
    await server.StartAsync();

    using var client = new TcpClient();
    await client.ConnectAsync("127.0.0.1", server.Port);
    var stream = client.GetStream();

    var writer = new PacketWriter(stream);
    var reader = new PacketReader(stream);

    // Act: Send Connect Packet
    // Protocol Version 1
    await writer.WritePacketAsync(new ConnectPacket(1));

    // Assert: Expect AuthenticationOk -> ReadyForQuery

    // 1. Read Auth OK
    var packet1 = await reader.ReadPacketAsync();
    Assert.IsType<AuthenticationOkPacket>(packet1);

    // 2. Read ReadyForQuery
    var packet2 = await reader.ReadPacketAsync();
    var readyPacket = Assert.IsType<ReadyForQueryPacket>(packet2);

    // Initial status should be 'I' (Idle)
    Assert.Equal('I', readyPacket.TransactionStatus);
  }
}