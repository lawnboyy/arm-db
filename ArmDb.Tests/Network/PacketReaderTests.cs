using System.Buffers.Binary;
using System.Net;
using System.Text;
using ArmDb.Network;
using ArmDb.SchemaDefinition;

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
  public async Task ReadPacketAsync_ErrorPacket_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    var originalPacket = new ErrorPacket(Severity: 2, Message: "Syntax Error");

    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0;

    // Act
    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    Assert.IsType<ErrorPacket>(readPacket);
    var errorPacket = (ErrorPacket)readPacket;
    Assert.Equal(originalPacket.Severity, errorPacket.Severity);
    Assert.Equal(originalPacket.Message, errorPacket.Message);
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

  [Fact]
  public async Task ReadPacketAsync_DataRowPacket_AllPrimitiveTypes_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);

    // 1. Int (4 bytes)
    var valInt = new byte[4];
    BinaryPrimitives.WriteInt32BigEndian(valInt, int.MaxValue);

    // 2. BigInt (8 bytes)
    var valBigInt = new byte[8];
    BinaryPrimitives.WriteInt64BigEndian(valBigInt, long.MaxValue);

    // 3. Varchar (UTF-8 String)
    var valVarchar = Encoding.UTF8.GetBytes("🔥 Test String 🔥");

    // 4. Boolean (1 byte)
    var valBool = new byte[] { 1 }; // True

    // 5. Decimal (16 bytes - usually 4 ints)
    // For testing "binary pass-through", we just need 16 bytes. 
    // Real serialization logic belongs in the Engine/Executor layer.
    var valDecimal = new byte[16];
    new Random(42).NextBytes(valDecimal);

    // 6. DateTime (8 bytes - Ticks)
    var valDateTime = new byte[8];
    BinaryPrimitives.WriteInt64BigEndian(valDateTime, DateTime.UtcNow.Ticks);

    // 7. Float (Double - 8 bytes)
    var valFloat = new byte[8];
    BinaryPrimitives.WriteDoubleBigEndian(valFloat, 123.456d);

    // 8. Blob (Arbitrary binary data)
    var valBlob = new byte[20];
    new Random(99).NextBytes(valBlob);

    var allValues = new List<byte[]?>
        {
            valInt, valBigInt, valVarchar, valBool,
            valDecimal, valDateTime, valFloat, valBlob
        };

    var originalPacket = new DataRowPacket(allValues);

    // Act
    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0;

    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    var dataRow = Assert.IsType<DataRowPacket>(readPacket);

    Assert.Equal(allValues.Count, dataRow.Values.Count);

    for (int i = 0; i < allValues.Count; i++)
    {
      Assert.NotNull(dataRow.Values[i]);
      Assert.Equal(allValues[i], dataRow.Values[i]);
    }
  }

  [Fact]
  public async Task ReadPacketAsync_QueryPacket_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);
    string expectedSql = "SELECT * FROM sys_tables WHERE table_id = 1";
    var originalPacket = new QueryPacket(Sql: expectedSql);

    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0; // Rewind for reading

    // Act
    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    Assert.IsType<QueryPacket>(readPacket);
    var queryPacket = (QueryPacket)readPacket;
    Assert.Equal(expectedSql, queryPacket.Sql);
  }

  [Fact]
  public async Task ReadPacketAsync_RowDescriptionPacket_ReadsCorrectly()
  {
    // Arrange
    var stream = new MemoryStream();
    var writer = new PacketWriter(stream);

    var fields = new List<RowDescriptionPacket.FieldDescription>
    {
      new("id", PrimitiveDataType.Int),
      new("username", PrimitiveDataType.Varchar),
      new("is_active", PrimitiveDataType.Boolean)
    };
    var originalPacket = new RowDescriptionPacket(fields);

    await writer.WritePacketAsync(originalPacket);
    stream.Position = 0;

    // Act
    var reader = new PacketReader(stream);
    var readPacket = await reader.ReadPacketAsync();

    // Assert
    Assert.NotNull(readPacket);
    Assert.IsType<RowDescriptionPacket>(readPacket);
    var rowDescPacket = (RowDescriptionPacket)readPacket;

    Assert.Equal(fields.Count, rowDescPacket.Fields.Count);

    Assert.Equal("id", rowDescPacket.Fields[0].Name);
    Assert.Equal(PrimitiveDataType.Int, rowDescPacket.Fields[0].DataType);

    Assert.Equal("username", rowDescPacket.Fields[1].Name);
    Assert.Equal(PrimitiveDataType.Varchar, rowDescPacket.Fields[1].DataType);

    Assert.Equal("is_active", rowDescPacket.Fields[2].Name);
    Assert.Equal(PrimitiveDataType.Boolean, rowDescPacket.Fields[2].DataType);
  }
}