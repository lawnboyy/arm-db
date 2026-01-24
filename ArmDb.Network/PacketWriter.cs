using System.Text;
using ArmDb.Common.Utils;

namespace ArmDb.Network;

public class PacketWriter
{
  private readonly Stream _stream;

  public PacketWriter(Stream stream)
  {
    _stream = stream;
  }

  /// <summary>
  /// Accepts a packet and writes it to the member stream (memory or network stream).
  /// </summary>
  /// <param name="packet"></param>
  /// <param name="ct"></param>
  /// <returns></returns>
  /// <exception cref="ArgumentException"></exception>
  public async Task WritePacketAsync(Packet packet, CancellationToken ct = default)
  {
    // All packets will be in the format [Type: char] [Length: int] [Payload]
    byte packetType = (byte)packet.Type;

    switch (packet.Type)
    {
      case PacketType.Connect:
        ConnectPacket connectPacket = (ConnectPacket)packet;
        await WriteConnectPacketAsync(connectPacket, ct);
        break;

      case PacketType.Query:
        QueryPacket queryPacket = (QueryPacket)packet;
        await WriteQueryPacketAsync(queryPacket, ct);
        break;

      case PacketType.Terminate:
        TerminatePacket terminatePacket = (TerminatePacket)packet;
        await WriteTerminatePacketAsync(terminatePacket, ct);
        break;

      default:
        throw new ArgumentException("Unknown packet type!");
    }
  }

  private async Task WriteConnectPacketAsync(ConnectPacket connectPacket, CancellationToken ct = default)
  {
    byte packetType = (byte)connectPacket.Type;

    // TODO: Consider using RecyclableMemoryStream package to reduce GC pressure here...
    using (var payloadBuffer = new MemoryStream())
    {
      // Serialize the payload in Big Endian to write to the temporary payload buffer...
      Span<byte> payloadSpan = stackalloc byte[4];
      BinaryUtilities.WriteInt32BigEndian(payloadSpan, connectPacket.ProtocolVersion);
      payloadBuffer.Write(payloadSpan);

      // Write the header to the stream...
      Span<byte> headerBuffer = stackalloc byte[5];
      headerBuffer[0] = packetType;
      BinaryUtilities.WriteInt32BigEndian(headerBuffer.Slice(1), (int)payloadBuffer.Length);
      await _stream.WriteAsync(headerBuffer.ToArray(), 0, headerBuffer.Length, ct);

      // Now write the payload to the stream...
      payloadBuffer.Position = 0;
      await _stream.WriteAsync(payloadBuffer.ToArray(), 0, (int)payloadBuffer.Length, ct);
    }
  }

  private async Task WriteQueryPacketAsync(QueryPacket queryPacket, CancellationToken ct = default)
  {
    byte packetType = (byte)queryPacket.Type;

    // TODO: Consider using RecyclableMemoryStream package to reduce GC pressure here...
    using (var payloadBuffer = new MemoryStream())
    {
      // Write the Sql string to the temporary buffer so we can determine the length; include null terminator at the end.
      payloadBuffer.Write([.. Encoding.UTF8.GetBytes(queryPacket.Sql), 0]);

      // Write the header to the stream...
      Span<byte> headerBuffer = stackalloc byte[5];
      headerBuffer[0] = packetType;
      BinaryUtilities.WriteInt32BigEndian(headerBuffer.Slice(1), (int)payloadBuffer.Length);
      await _stream.WriteAsync(headerBuffer.ToArray(), 0, headerBuffer.Length, ct);

      // Now write the payload to the stream...
      payloadBuffer.Position = 0;
      await _stream.WriteAsync(payloadBuffer.ToArray(), 0, (int)payloadBuffer.Length, ct);
    }
  }

  private async Task WriteTerminatePacketAsync(TerminatePacket terminatePacket, CancellationToken ct = default)
  {
    byte packetType = (byte)terminatePacket.Type;

    // There is no payload for the terminate packet so we don't need to calculate the length.

    // Write the header to the stream...
    Span<byte> headerBuffer = stackalloc byte[5];
    headerBuffer[0] = packetType;
    BinaryUtilities.WriteInt32BigEndian(headerBuffer.Slice(1), 0);
    await _stream.WriteAsync(headerBuffer.ToArray(), 0, headerBuffer.Length, ct);
  }
}