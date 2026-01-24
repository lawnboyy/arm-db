using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using ArmDb.Network;
using ArmDb.SchemaDefinition;
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

  [Fact]
  public async Task WritePacketAsync_QueryPacket_WritesCorrectBytes()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    string sql = "SELECT * FROM sys_tables";
    var packet = new QueryPacket(Sql: sql);

    // Act
    await writer.WritePacketAsync(packet);

    // Assert
    stream.Position = 0;
    var buffer = stream.ToArray();

    // 1. Calculate Expected Sizes
    // Payload = UTF-8 Bytes + 1 Null Terminator
    int expectedPayloadLength = Encoding.UTF8.GetByteCount(sql) + 1;
    int expectedTotalSize = 1 + 4 + expectedPayloadLength; // Type + LengthHeader + Payload

    // 2. Check Total Wire Size
    Assert.Equal(expectedTotalSize, buffer.Length);

    // 3. Check Type
    Assert.Equal((byte)PacketType.Query, buffer[0]);

    // 4. Check Length Field (Payload Only)
    int length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1, 4));
    Assert.Equal(expectedPayloadLength, length);

    // 5. Check Payload Content
    // Extract string from buffer [5 .. End-1]
    // We exclude the last byte which should be null terminator
    string actualSql = Encoding.UTF8.GetString(buffer.AsSpan(5, expectedPayloadLength - 1));
    Assert.Equal(sql, actualSql);

    // 6. Check Null Terminator
    Assert.Equal(0, buffer[^1]);
  }

  [Fact]
  public async Task WritePacketAsync_TerminatePacket_WritesCorrectBytes()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var packet = new TerminatePacket();

    // Act
    await writer.WritePacketAsync(packet);

    // Assert
    stream.Position = 0;
    var buffer = stream.ToArray();

    // 1. Check Total Wire Size
    // Header (1 Type + 4 Length) + Payload (0) = 5 bytes
    Assert.Equal(5, buffer.Length);

    // 2. Check Type
    Assert.Equal((byte)PacketType.Terminate, buffer[0]);

    // 3. Check Length Field (Payload Only)
    // Payload is 0 bytes.
    int length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1, 4));
    Assert.Equal(0, length);
  }

  [Fact]
  public async Task WritePacketAsync_AuthenticationOkPacket_WritesCorrectBytes()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var packet = new AuthenticationOkPacket();

    // Act
    await writer.WritePacketAsync(packet);

    // Assert
    stream.Position = 0;
    var buffer = stream.ToArray();

    Assert.Equal(5, buffer.Length); // 1 Type + 4 Length + 0 Payload
    Assert.Equal((byte)PacketType.AuthenticationOk, buffer[0]);

    int length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1, 4));
    Assert.Equal(0, length);
  }

  [Fact]
  public async Task WritePacketAsync_ErrorPacket_WritesCorrectBytes()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    byte severity = 2;
    string message = "Syntax Error";
    var packet = new ErrorPacket(Severity: severity, Message: message);

    // Act
    await writer.WritePacketAsync(packet);

    // Assert
    stream.Position = 0;
    var buffer = stream.ToArray();

    // Payload: [Severity (1 byte)] + [Message (String + Null)]
    int expectedPayloadLen = 1 + Encoding.UTF8.GetByteCount(message) + 1;

    Assert.Equal(1 + 4 + expectedPayloadLen, buffer.Length);
    Assert.Equal((byte)PacketType.Error, buffer[0]);

    int length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1, 4));
    Assert.Equal(expectedPayloadLen, length);

    // Check Severity
    Assert.Equal(severity, buffer[5]);

    // Check Message
    string actualMessage = Encoding.UTF8.GetString(buffer.AsSpan(6, expectedPayloadLen - 2)); // -1 for severity, -1 for null
    Assert.Equal(message, actualMessage);
    Assert.Equal(0, buffer[^1]);
  }

  [Fact]
  public async Task WritePacketAsync_CommandCompletePacket_WritesCorrectBytes()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    string tag = "INSERT 1";
    var packet = new CommandCompletePacket(Tag: tag);

    // Act
    await writer.WritePacketAsync(packet);

    // Assert
    stream.Position = 0;
    var buffer = stream.ToArray();

    int expectedPayloadLen = Encoding.UTF8.GetByteCount(tag) + 1;
    Assert.Equal(1 + 4 + expectedPayloadLen, buffer.Length);
    Assert.Equal((byte)PacketType.CommandComplete, buffer[0]);

    string actualTag = Encoding.UTF8.GetString(buffer.AsSpan(5, expectedPayloadLen - 1));
    Assert.Equal(tag, actualTag);
  }

  [Fact]
  public async Task WritePacketAsync_ReadyForQueryPacket_WritesCorrectBytes()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var packet = new ReadyForQueryPacket(TransactionStatus: 'I');

    // Act
    await writer.WritePacketAsync(packet);

    // Assert
    stream.Position = 0;
    var buffer = stream.ToArray();

    // Payload: 1 byte (char)
    Assert.Equal(1 + 4 + 1, buffer.Length);
    Assert.Equal((byte)PacketType.ReadyForQuery, buffer[0]);

    Assert.Equal((byte)'I', buffer[5]);
  }

  [Fact]
  public async Task WritePacketAsync_RowDescriptionPacket_WritesCorrectBytes()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var fields = new List<RowDescriptionPacket.FieldDescription>
        {
            new("id", PrimitiveDataType.Int),
            new("name", PrimitiveDataType.Varchar)
        };
    var packet = new RowDescriptionPacket(fields);

    // Act
    await writer.WritePacketAsync(packet);

    // Assert
    stream.Position = 0;
    var buffer = stream.ToArray();

    // Format: [FieldCount (2 bytes)] + For Each: [Name (String)] [Type (1 byte)]
    // Field Count
    Assert.Equal((byte)PacketType.RowDescription, buffer[0]);
    short fieldCount = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(5, 2));
    Assert.Equal(2, fieldCount);

    // We need to parse sequentially to verify because strings are variable length
    int offset = 7;

    // Field 1: "id"
    // Find null terminator for string
    int nullIndex1 = Array.IndexOf(buffer, (byte)0, offset);
    string name1 = Encoding.UTF8.GetString(buffer.AsSpan(offset, nullIndex1 - offset));
    Assert.Equal("id", name1);
    offset = nullIndex1 + 1;
    Assert.Equal((byte)PrimitiveDataType.Int, buffer[offset]); // Type
    offset++;

    // Field 2: "name"
    int nullIndex2 = Array.IndexOf(buffer, (byte)0, offset);
    string name2 = Encoding.UTF8.GetString(buffer.AsSpan(offset, nullIndex2 - offset));
    Assert.Equal("name", name2);
    offset = nullIndex2 + 1;
    Assert.Equal((byte)PrimitiveDataType.Varchar, buffer[offset]); // Type
  }

  [Fact]
  public async Task WritePacketAsync_DataRowPacket_WritesCorrectBytes()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);

    // Row: [100 (Int), NULL, "Hello" (String)]
    // Note: For simplicity in Phase 1, we pass raw byte arrays for values.
    // The Engine/Executor layer is responsible for converting Record -> byte[].
    var val1 = new byte[4]; BinaryPrimitives.WriteInt32BigEndian(val1, 100);
    var val3 = Encoding.UTF8.GetBytes("Hello");

    var values = new List<byte[]?> { val1, null, val3 };
    var packet = new DataRowPacket(values);

    // Act
    await writer.WritePacketAsync(packet);

    // Assert
    stream.Position = 0;
    var buffer = stream.ToArray();

    // Format: [ValueCount (2 bytes)] + For Each: [Length (4 bytes)] [Data (N bytes)]
    Assert.Equal((byte)PacketType.DataRow, buffer[0]);

    short valCount = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(5, 2));
    Assert.Equal(3, valCount);

    int offset = 7;

    // Value 1: Int (4 bytes)
    int len1 = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(offset, 4));
    offset += 4;
    Assert.Equal(4, len1);
    int intVal = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(offset, 4));
    Assert.Equal(100, intVal);
    offset += 4;

    // Value 2: NULL (-1 length)
    int len2 = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(offset, 4));
    offset += 4;
    Assert.Equal(-1, len2);
    // No data bytes for NULL

    // Value 3: String "Hello" (5 bytes)
    int len3 = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(offset, 4));
    offset += 4;
    Assert.Equal(5, len3);
    string strVal = Encoding.UTF8.GetString(buffer.AsSpan(offset, 5));
    Assert.Equal("Hello", strVal);
  }

  [Fact]
  public async Task WritePacketAsync_UnknownPacketType_ThrowsArgumentException()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);

    // Act & Assert
    // We cast an integer to PacketType to simulate an unknown type
    var invalidPacket = new FakePacket((PacketType)255);
    await Assert.ThrowsAsync<NotSupportedException>(() => writer.WritePacketAsync(invalidPacket));
  }

  // Helper for invalid packet testing
  private record FakePacket(PacketType Type) : Packet
  {
    public override PacketType Type { get; } = Type;
  }
}