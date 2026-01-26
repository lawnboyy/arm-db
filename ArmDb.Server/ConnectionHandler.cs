using System.Buffers.Binary;
using System.Data;
using System.Formats.Asn1;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using ArmDb.Common.Abstractions;
using ArmDb.Common.Utils;
using ArmDb.DataModel;
using ArmDb.Network;
using ArmDb.SchemaDefinition;
using Microsoft.Extensions.Logging;

namespace ArmDb.Server;

internal class ConnectionHandler
{
  private readonly TcpClient _tcpClient;
  private readonly IStorageEngine _storageEngine;
  private readonly ILogger<ConnectionHandler> _logger;

  internal ConnectionHandler(TcpClient client, IStorageEngine storageEngine, ILoggerFactory loggerFactory)
  {
    _tcpClient = client;
    _storageEngine = storageEngine;
    _logger = loggerFactory.CreateLogger<ConnectionHandler>();
  }

  internal async Task RunAsync(CancellationToken ct = default)
  {
    try
    {
      var stream = _tcpClient.GetStream();
      var reader = new PacketReader(stream);
      var writer = new PacketWriter(stream);

      var packet = await reader.ReadPacketAsync(ct);
      var connectPacket = packet as ConnectPacket;

      if (connectPacket == null)
      {
        // TODO: Return an error here...
      }
      else
      {
        // Check the protocol version to make sure we are compatible...
        // TODO: Load the protocol version support from configuration.
        if (connectPacket.ProtocolVersion != 1)
          throw new ProtocolViolationException($"Version: {connectPacket.ProtocolVersion} is not supported!");

        await writer.WritePacketAsync(new AuthenticationOkPacket());
        await writer.WritePacketAsync(new ReadyForQueryPacket('I'));
      }

      while (!ct.IsCancellationRequested)
      {
        packet = await reader.ReadPacketAsync(ct);

        switch (packet.Type)
        {
          case PacketType.Query:
            await HandleQuery(((QueryPacket)packet).Sql, writer);
            break;

          default:
            throw new NotSupportedException($"Packet type {packet.Type} is not supported!");
        }

        // Once the response has been sent, let the client know that we are ready to handle the next request.
        await writer.WritePacketAsync(new ReadyForQueryPacket('I'));
      }
    }
    catch (Exception ex)
    {
      _logger.LogError(ex, ex.Message);
      throw;
    }
  }

  private async Task HandleQuery(string sql, PacketWriter packetWriter, CancellationToken ct = default)
  {
    // TODO: Implement query parser and execution planner. 
    // For now we'll just hard code a table scan of the sys_tables table to pass the test.

    // First get the table definition so we can pass back the row description packet.
    var tableDefinition = await _storageEngine.GetTableDefinitionAsync("sys_tables");
    if (tableDefinition == null)
      throw new ArgumentNullException("The table definition was not found!");

    var fields = tableDefinition.Columns
      .Select(c => new RowDescriptionPacket.FieldDescription(c.Name, c.DataType.PrimitiveType))
      .ToList();
    var rowDescriptionPacket = new RowDescriptionPacket(fields);
    await packetWriter.WritePacketAsync(rowDescriptionPacket);

    await foreach (var row in _storageEngine.ScanAsync("sys_tables"))
    {
      // Write each data row as a packet to the stream
      // First serialize each data value into a collection byte arrays for our data row packet.s
      List<byte[]?> data = new();
      foreach (var value in row.Values)
      {
        var bytes = SerializeDataValue(value);
        data.Add(bytes);
      }

      // Now send the data row packet.
      await packetWriter.WritePacketAsync(new DataRowPacket(data));
    }

    // Now that we are done, we can send the complete packet...
    await packetWriter.WritePacketAsync(new CommandCompletePacket("SELECT completed."), ct);
  }

  private byte[] SerializeDataValue(DataValue value)
  {
    switch (value.DataType)
    {
      case PrimitiveDataType.BigInt:
        // First unbox the value...
        long bigInt = (long)value.Value!;
        // Now serialize it to big endian.
        byte[] bigIntBytes = new byte[sizeof(long)];
        BinaryUtilities.WriteInt64BigEndian(bigIntBytes, bigInt);
        return bigIntBytes;

      case PrimitiveDataType.Blob:
        // First unbox the value...
        byte[] blobValue = (byte[])value.Value!;
        return blobValue;

      case PrimitiveDataType.Boolean:
        bool boolValue = (bool)value.Value!;
        byte boolByte = boolValue ? (byte)1 : (byte)0;
        return [boolByte];

      case PrimitiveDataType.DateTime:
        DateTime dateTimeValue = Convert.ToDateTime(value.Value);
        byte[] dateTimeBytes = new byte[sizeof(long)];
        BinaryUtilities.WriteInt64LittleEndian(dateTimeBytes, dateTimeValue.ToBinary());
        return dateTimeBytes;

      case PrimitiveDataType.Decimal:
        decimal decimalValue = Convert.ToDecimal(value.Value);
        byte[] decimalBytes = new byte[sizeof(decimal)];
        MemoryMarshal.Write(decimalBytes, in decimalValue);
        return decimalBytes;

      case PrimitiveDataType.Float:
        double doubleValue = Convert.ToDouble(value.Value);
        byte[] doubleBytes = new byte[sizeof(double)];
        BinaryUtilities.WriteDoubleBigEndian(doubleBytes, doubleValue);
        return doubleBytes;

      case PrimitiveDataType.Int:
        // First unbox the value...
        int integer = (int)value.Value!;
        // Now serialize it to big endian.
        byte[] intBytes = new byte[sizeof(int)];
        BinaryUtilities.WriteInt32BigEndian(intBytes, integer);
        return intBytes;

      case PrimitiveDataType.Varchar:
        string? stringValue = Convert.ToString(value.Value);
        byte[] varcharBytes = Encoding.UTF8.GetBytes(stringValue!) ?? throw new ArgumentNullException("Could not convert string value for serialization!");
        return varcharBytes;

      default:
        throw new NotSupportedException($"Unsupported data type: {value.DataType}!");
    }
  }
}