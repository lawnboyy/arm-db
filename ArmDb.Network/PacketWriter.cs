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
    switch (packet.Type)
    {
      case PacketType.Connect:
        await WriteConnectPacketAsync((ConnectPacket)packet, ct);
        break;

      case PacketType.Query:
        await WriteQueryPacketAsync((QueryPacket)packet, ct);
        break;

      case PacketType.Terminate:
        await WriteTerminatePacketAsync((TerminatePacket)packet, ct);
        break;

      case PacketType.AuthenticationOk:
        await WriteAuthenticationOkPacketAsync((AuthenticationOkPacket)packet, ct);
        break;

      case PacketType.Error:
        await WriteErrorPacketAsync((ErrorPacket)packet, ct);
        break;

      case PacketType.RowDescription:
        await WriteRowDescriptionPacketAsync((RowDescriptionPacket)packet, ct);
        break;

      case PacketType.DataRow:
        await WriteDataRowPacketAsync((DataRowPacket)packet, ct);
        break;

      case PacketType.CommandComplete:
        await WriteCommandCompletePacketAsync((CommandCompletePacket)packet, ct);
        break;

      case PacketType.ReadyForQuery:
        await WriteReadyForQueryPacketAsync((ReadyForQueryPacket)packet, ct);
        break;

      default:
        throw new ArgumentException($"Unknown packet type: {packet.Type}!");
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

      await WriteHeaderAndPayloadAsync(packetType, payloadBuffer, ct);
    }
  }

  private async Task WriteQueryPacketAsync(QueryPacket queryPacket, CancellationToken ct = default)
  {
    byte packetType = (byte)queryPacket.Type;

    using (var payloadBuffer = new MemoryStream())
    {
      // Write the Sql string + null terminator
      payloadBuffer.Write([.. Encoding.UTF8.GetBytes(queryPacket.Sql), 0]);

      await WriteHeaderAndPayloadAsync(packetType, payloadBuffer, ct);
    }
  }

  private async Task WriteTerminatePacketAsync(TerminatePacket terminatePacket, CancellationToken ct = default)
  {
    byte packetType = (byte)terminatePacket.Type;

    // No payload
    Span<byte> headerBuffer = stackalloc byte[5];
    headerBuffer[0] = packetType;
    BinaryUtilities.WriteInt32BigEndian(headerBuffer.Slice(1), 0);
    await _stream.WriteAsync(headerBuffer.ToArray(), 0, headerBuffer.Length, ct);
  }

  private async Task WriteAuthenticationOkPacketAsync(AuthenticationOkPacket packet, CancellationToken ct = default)
  {
    byte packetType = (byte)packet.Type;

    // No payload
    Span<byte> headerBuffer = stackalloc byte[5];
    headerBuffer[0] = packetType;
    BinaryUtilities.WriteInt32BigEndian(headerBuffer.Slice(1), 0);
    await _stream.WriteAsync(headerBuffer.ToArray(), 0, headerBuffer.Length, ct);
  }

  private async Task WriteErrorPacketAsync(ErrorPacket packet, CancellationToken ct = default)
  {
    byte packetType = (byte)packet.Type;

    using (var payloadBuffer = new MemoryStream())
    {
      // Format: [Severity (1 byte)] [Message (String + Null)]
      payloadBuffer.WriteByte(packet.Severity);
      payloadBuffer.Write([.. Encoding.UTF8.GetBytes(packet.Message), 0]);

      await WriteHeaderAndPayloadAsync(packetType, payloadBuffer, ct);
    }
  }

  private async Task WriteRowDescriptionPacketAsync(RowDescriptionPacket packet, CancellationToken ct = default)
  {
    byte packetType = (byte)packet.Type;

    using (var payloadBuffer = new MemoryStream())
    {
      // Format: [FieldCount (2 bytes)] + For Each: [Name (String)] [Type (1 byte)]

      Span<byte> countSpan = stackalloc byte[2];
      BinaryUtilities.WriteInt16BigEndian(countSpan, (short)packet.Fields.Count);
      payloadBuffer.Write(countSpan);

      foreach (var field in packet.Fields)
      {
        payloadBuffer.Write([.. Encoding.UTF8.GetBytes(field.Name), 0]);
        payloadBuffer.WriteByte((byte)field.DataType);
      }

      await WriteHeaderAndPayloadAsync(packetType, payloadBuffer, ct);
    }
  }

  private async Task WriteDataRowPacketAsync(DataRowPacket packet, CancellationToken ct = default)
  {
    byte packetType = (byte)packet.Type;

    using (var payloadBuffer = new MemoryStream())
    {
      // Format: [ValueCount (2 bytes)] + For Each: [Length (4 bytes)] [Data (N bytes)]

      Span<byte> countSpan = stackalloc byte[2];
      BinaryUtilities.WriteInt16BigEndian(countSpan, (short)packet.Values.Count);
      payloadBuffer.Write(countSpan);
      Span<byte> lenSpan = stackalloc byte[4];

      foreach (var value in packet.Values)
      {
        lenSpan.Clear();
        if (value == null)
        {
          // NULL is represented by length -1
          BinaryUtilities.WriteInt32BigEndian(lenSpan, -1);
          payloadBuffer.Write(lenSpan);
        }
        else
        {
          BinaryUtilities.WriteInt32BigEndian(lenSpan, value.Length);
          payloadBuffer.Write(lenSpan);
          payloadBuffer.Write(value);
        }
      }

      await WriteHeaderAndPayloadAsync(packetType, payloadBuffer, ct);
    }
  }

  private async Task WriteCommandCompletePacketAsync(CommandCompletePacket packet, CancellationToken ct = default)
  {
    byte packetType = (byte)packet.Type;

    using (var payloadBuffer = new MemoryStream())
    {
      // Format: [Tag (String + Null)]
      payloadBuffer.Write([.. Encoding.UTF8.GetBytes(packet.Tag), 0]);

      await WriteHeaderAndPayloadAsync(packetType, payloadBuffer, ct);
    }
  }

  private async Task WriteReadyForQueryPacketAsync(ReadyForQueryPacket packet, CancellationToken ct = default)
  {
    byte packetType = (byte)packet.Type;

    using (var payloadBuffer = new MemoryStream())
    {
      // Format: [TransactionStatus (1 byte)]
      payloadBuffer.WriteByte((byte)packet.TransactionStatus);

      await WriteHeaderAndPayloadAsync(packetType, payloadBuffer, ct);
    }
  }

  // Helper to standardize Header + Payload writing logic
  private async Task WriteHeaderAndPayloadAsync(byte packetType, MemoryStream payloadBuffer, CancellationToken ct)
  {
    Span<byte> headerBuffer = stackalloc byte[5];
    headerBuffer[0] = packetType;
    BinaryUtilities.WriteInt32BigEndian(headerBuffer.Slice(1), (int)payloadBuffer.Length);

    await _stream.WriteAsync(headerBuffer.ToArray(), 0, headerBuffer.Length, ct);

    payloadBuffer.Position = 0;
    await _stream.WriteAsync(payloadBuffer.ToArray(), 0, (int)payloadBuffer.Length, ct);
  }
}