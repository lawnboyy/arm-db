using System.Net.Sockets;
using System.Text;
using ArmDb.Common.Utils;
using ArmDb.Network;

namespace ArmDb.Tests.Integration;

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

  [Fact]
  public async Task Server_Query_ReturnsResults_ForSystemTable()
  {
    // Arrange
    var fileSystem = new FileSystem();
    await using var server = new TestServer(fileSystem);
    await server.StartAsync();

    using var client = new TcpClient();
    await client.ConnectAsync("127.0.0.1", server.Port);
    var stream = client.GetStream();
    var writer = new PacketWriter(stream);
    var reader = new PacketReader(stream);

    // Use a rigorous timeout for the whole interaction
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

    // Handshake
    await writer.WritePacketAsync(new ConnectPacket(1), cts.Token);
    await reader.ReadPacketAsync(cts.Token); // AuthOK
    await reader.ReadPacketAsync(cts.Token); // ReadyForQuery

    // Act: Send Query for sys_tables
    string sql = "SELECT * FROM sys_tables";
    await writer.WritePacketAsync(new QueryPacket(sql), cts.Token);

    // Assert: Expect RowDescription -> DataRows -> CommandComplete -> ReadyForQuery

    // 1. RowDescription
    // If server logic is missing, this line will throw OperationCanceledException after 5s
    var packet1 = await reader.ReadPacketAsync(cts.Token);
    var rowDesc = Assert.IsType<RowDescriptionPacket>(packet1);
    Assert.NotEmpty(rowDesc.Fields);

    // Verify 'table_name' column exists at index 2 (based on standard schema: table_id, database_id, table_name, creation_date)
    Assert.Equal("table_name", rowDesc.Fields[2].Name);

    // 2. DataRows
    // We capture the table names found in the results to verify against expected system tables.
    var foundTableNames = new HashSet<string>();

    while (true)
    {
      var p = await reader.ReadPacketAsync(cts.Token);
      if (p is DataRowPacket dataRow)
      {
        // Parse table_name from index 2
        // Values[2] is byte[]? (UTF-8 bytes)
        var nameBytes = dataRow.Values[2];
        Assert.NotNull(nameBytes);
        string tableName = Encoding.UTF8.GetString(nameBytes);
        foundTableNames.Add(tableName);
      }
      else if (p is CommandCompletePacket complete)
      {
        Assert.StartsWith("SELECT", complete.Tag);
        break;
      }
      else
      {
        Assert.Fail($"Unexpected packet type: {p.GetType().Name}");
      }
    }

    // Verify core system tables exist
    Assert.Contains("sys_databases", foundTableNames);
    Assert.Contains("sys_tables", foundTableNames);
    Assert.Contains("sys_columns", foundTableNames);
    Assert.Contains("sys_constraints", foundTableNames);

    // 3. ReadyForQuery
    var readyPacket = await reader.ReadPacketAsync(cts.Token);
    Assert.IsType<ReadyForQueryPacket>(readyPacket);
  }
}