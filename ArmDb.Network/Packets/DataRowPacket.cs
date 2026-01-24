namespace ArmDb.Network;

/// <summary>
/// Packet sent by the server containing a single row of data.
/// Format: [Type 'D'] [Length] [ValueCount (2 bytes)] [Value1] [Value2] ...
/// </summary>
public record DataRowPacket(List<byte[]?> Values) : Packet
{
  public override PacketType Type => PacketType.DataRow;
}