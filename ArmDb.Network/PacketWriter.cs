using System.Buffers.Binary;

namespace ArmDb.Network;

public class PacketWriter
{
  private readonly Stream _stream;

  public PacketWriter(Stream stream)
  {
    _stream = stream;
  }

  public async Task WritePacketAsync(Packet packet, CancellationToken ct = default)
  {
    // All packets will be in the format [Type: char] [Length: int] [Payload]
    byte packetType = (byte)packet.Type;

    switch (packet.Type)
    {
      case PacketType.Connect:
        var connectPacket = (ConnectPacket)packet;

        using (var payloadBuffer = new MemoryStream())
        {
          // Serialize the payload in Big Endian to write to the temporary payload buffer...
          Span<byte> payloadSpan = stackalloc byte[4];
          BinaryPrimitives.WriteInt32BigEndian(payloadSpan, connectPacket.ProtocolVersion);
          payloadBuffer.Write(payloadSpan);
          // await payloadBuffer.WriteAsync(payloadSpan.ToArray(), ct);

          // Now write the header to the stream...
          // Write the packet type to the stream...
          await _stream.WriteAsync([packetType], 0, 1);
          // Write the total payload length to the stream...
          Span<byte> lengthBuffer = stackalloc byte[4];
          BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, (int)payloadBuffer.Length);
          await _stream.WriteAsync(lengthBuffer.ToArray(), 0, lengthBuffer.Length, ct);

          // Now write the payload to the stream...
          payloadBuffer.Position = 0;
          await _stream.WriteAsync(payloadBuffer.ToArray(), 0, (int)payloadBuffer.Length, ct);
        }
        break;
      default:
        throw new ArgumentException("Unknown packet type!");
    }
  }
}