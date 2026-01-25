using System.Net.Sockets;
using ArmDb.Common.Abstractions;
using ArmDb.Network;

namespace ArmDb.Server;

internal class ConnectionHandler
{
  private readonly TcpClient _tcpClient;
  private readonly IStorageEngine _storageEngine;

  // TODO: Add a logger at some point.
  internal ConnectionHandler(TcpClient client, IStorageEngine storageEngine)
  {
    _tcpClient = client;
    _storageEngine = storageEngine;
  }

  internal async Task RunAsync(CancellationToken ct = default)
  {
    try
    {
      var stream = _tcpClient.GetStream();
      var reader = new PacketReader(stream);
      var writer = new PacketWriter(stream);

      var packet = await reader.ReadPacketAsync(ct);

      if (packet is not ConnectPacket)
      {
        // TODO: Return an error here...
      }
      else
      {
        // TODO: Check the protocol version to make sure we are compatible...
        await writer.WritePacketAsync(new AuthenticationOkPacket());
        await writer.WritePacketAsync(new ReadyForQueryPacket('I'));
      }

      while (!ct.IsCancellationRequested)
      {
        packet = await reader.ReadPacketAsync(ct);
        // TODO: Handle queries...
      }
    }
    catch (Exception)
    {

    }

  }
}