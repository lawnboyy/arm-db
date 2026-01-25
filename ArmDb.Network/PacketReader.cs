using System.Buffers;
using System.Net;
using System.Text;
using ArmDb.Common.Utils;
using ArmDb.SchemaDefinition;
using static ArmDb.Network.RowDescriptionPacket;

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

      case PacketType.DataRow:
        return await ReadDataRowPacketAsync(payloadLength, ct);

      case PacketType.Error:
        return await ReadErrorPacket(payloadLength, ct);

      case PacketType.Query:
        return await ReadQueryPacketAsync(payloadLength, ct);

      case PacketType.ReadyForQuery:
        return ReadReadyForQueryPacket();

      case PacketType.RowDescription:
        return await ReadRowDescriptionPacketAsync(payloadLength, ct);

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
      throw new ProtocolViolationException("Character string payload was not properly terminated. The data may have been corrupted!");
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

  private async Task<RowDescriptionPacket> ReadRowDescriptionPacketAsync(int length, CancellationToken ct)
  {
    // Format: [FieldCount (2 bytes)] + For Each: [Name (String)] [Type (1 byte)]
    // Rent a payload buffer from the shared array pool...
    var payloadBuffer = ArrayPool<byte>.Shared.Rent(length);
    try
    {
      // Slice our memory buffer since the rented buffer is only guaranteed to be at least the length we provided, but could be greater.
      // In order to read the payload length exactly, the buffer size must be exactly the payload size. Otherwise, we risk trying
      // to read past the end of the stream which throw an exception.
      var memoryBuffer = payloadBuffer.AsMemory(0, length);
      // Read in the entire payload from the stream...
      await _stream.ReadExactlyAsync(memoryBuffer, ct);
      // Read the 16 bit int that represents the value count...
      var fieldCount = BinaryUtilities.ReadInt16BigEndian(memoryBuffer.Slice(0, 2).Span);

      int offset = 2;

      // Loop through the rest of the buffer and parse out the fields.
      var fields = new List<FieldDescription>();
      for (var i = 0; i < fieldCount; i++)
      {
        // Read the field name by reading until we find a null terminator.
        byte currentByte = memoryBuffer.Span[offset];
        List<byte> nameBytes = new();
        while (currentByte != 0)
        {
          nameBytes.Add(currentByte);
          offset++;
          currentByte = memoryBuffer.Span[offset];
        }

        // Get the name string...
        var name = Encoding.UTF8.GetString(nameBytes.ToArray());

        // Advance the offset to skip the null terminator
        offset++;

        // Now get the byte that represents the data type...
        PrimitiveDataType fieldType = (PrimitiveDataType)memoryBuffer.Span[offset];

        fields.Add(new FieldDescription(name, fieldType));

        // Advance the offset...
        offset++;
      }

      return new RowDescriptionPacket(fields);
    }
    finally
    {
      // Return our buffer, now that we no longer need it...
      ArrayPool<byte>.Shared.Return(payloadBuffer);
    }
  }

  private async Task<DataRowPacket> ReadDataRowPacketAsync(int length, CancellationToken ct = default)
  {
    // Format: [ValueCount (2 bytes)] + For Each: [Length (4 bytes)] [Data (N bytes)]
    // Rent a payload buffer from the shared array pool...
    var payloadBuffer = ArrayPool<byte>.Shared.Rent(length);
    try
    {
      // Slice our memory buffer since the rented buffer is only guaranteed to be at least the length we provided, but could be greater.
      // In order to read the payload length exactly, the buffer size must be exactly the payload size. Otherwise, we risk trying
      // to read past the end of the stream which throw an exception.
      var memoryBuffer = payloadBuffer.AsMemory(0, length);
      // Read in the entire payload from the stream...
      await _stream.ReadExactlyAsync(memoryBuffer, ct);
      // Read the 16 bit int that represents the value count...
      var valueCount = BinaryUtilities.ReadInt16BigEndian(memoryBuffer.Slice(0, 2).Span);
      int offset = 2;

      // Loop through the rest of the buffer and parse out the values.
      var values = new List<byte[]?>();
      for (var i = 0; i < valueCount; i++)
      {
        // Read the length from the current offset...
        var valueLength = BinaryUtilities.ReadInt32BigEndian(memoryBuffer.Slice(offset, sizeof(int)).Span);
        offset += sizeof(int);
        if (valueLength == -1)
        {
          // Value is null...
          values.Add(null);
        }
        else
        {
          // Read in the value based on the length.
          var bytes = new byte[valueLength];
          memoryBuffer
            .Slice(offset, valueLength)
            .CopyTo(bytes);
          values.Add(bytes);
          offset += valueLength;
        }
      }

      return new DataRowPacket(values);
    }
    finally
    {
      // Return our buffer, now that we no longer need it...
      ArrayPool<byte>.Shared.Return(payloadBuffer);
    }
  }

  private async Task<ErrorPacket> ReadErrorPacket(int length, CancellationToken ct = default)
  {
    // Get the severity byte
    byte severity = (byte)_stream.ReadByte();
    var errorBuffer = new byte[length - 1];
    await _stream.ReadExactlyAsync(errorBuffer, ct);

    // Verify that null termination character...
    if (errorBuffer[^1] != 0)
    {
      throw new ProtocolViolationException("Character string payload was not properly terminated. The data may have been corrupted!");
    }

    var error = Encoding.UTF8.GetString(errorBuffer[..^1]);
    return new ErrorPacket(severity, error);
  }

  private async Task<QueryPacket> ReadQueryPacketAsync(int length, CancellationToken ct = default)
  {
    var queryBuffer = new byte[length];
    await _stream.ReadExactlyAsync(queryBuffer, ct);

    // Verify that null termination character...
    if (queryBuffer[^1] != 0)
    {
      throw new ProtocolViolationException("Character string payload was not properly terminated. The data may have been corrupted!");
    }

    var query = Encoding.UTF8.GetString(queryBuffer[..^1]);
    return new QueryPacket(query);
  }

  private ReadyForQueryPacket ReadReadyForQueryPacket()
  {
    // Read the transaction status byte...
    var transactionStatus = (char)_stream.ReadByte();
    return new ReadyForQueryPacket(transactionStatus);
  }
}