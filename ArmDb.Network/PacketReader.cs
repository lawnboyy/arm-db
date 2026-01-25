using System.Buffers.Binary;
using System.Dynamic;
using System.Text;
using ArmDb.Common.Utils;

namespace ArmDb.Network;

public class PacketReader
{
  private readonly Stream _stream;
  private readonly byte[] _headerBuffer = new byte[5];

  public PacketReader(Stream stream)
  {
    _stream = stream;
  }

  public async Task<Packet> ReadPacketAsync(CancellationToken ct = default)
  {
    // First read in the header...
    await _stream.ReadExactlyAsync(_headerBuffer, ct);
    var packetType = (PacketType)_headerBuffer[0];
    int payloadLength = BinaryUtilities.ReadInt32BigEndian(_headerBuffer.AsSpan().Slice(1, 4));

    switch (packetType)
    {
      case PacketType.AuthenticationOk:
        return new AuthenticationOkPacket();

      case PacketType.CommandComplete:
        return await ReadCommandCompletePacketAsync(payloadLength, ct);

      case PacketType.Connect:
        return await ReadConnectPacketAsync(ct);

      case PacketType.ReadyForQuery:
        return ReadReadyForQueryPacket();

      case PacketType.Terminate:
        return new TerminatePacket();

      default:
        throw new NotSupportedException($"Packet type {packetType} is not supported!");
    }
  }

  private async Task<CommandCompletePacket> ReadCommandCompletePacketAsync(int length, CancellationToken ct = default)
  {
    var tagBuffer = new byte[length];
    await _stream.ReadExactlyAsync(tagBuffer, ct);

    // Verify that null termination character...
    if (tagBuffer[^1] != 0)
    {
      throw new Exception("Character string payload was not properly terminated. The data may have been corrupted!");
    }

    var tag = Encoding.UTF8.GetString(tagBuffer[..^1]);
    return new CommandCompletePacket(tag);
  }

  private async Task<ConnectPacket> ReadConnectPacketAsync(CancellationToken ct = default)
  {
    var versionBuffer = new byte[4];
    await _stream.ReadExactlyAsync(versionBuffer, ct);
    int version = BinaryUtilities.ReadInt32BigEndian(versionBuffer);
    return new ConnectPacket(version);
  }

  private ReadyForQueryPacket ReadReadyForQueryPacket()
  {
    // Read the transaction status byte...
    var transactionStatus = (char)_stream.ReadByte();
    return new ReadyForQueryPacket(transactionStatus);
  }
}