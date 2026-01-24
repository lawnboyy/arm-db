namespace ArmDb.Network;


/// <summary>
/// Packet sent by the client to execute a SQL query.
/// Format: [Type 'Q'] [Length] [Sql (UTF-8 String)]
/// </summary>
/// <param name="Sql"> The SQL query string to be executed. </param>
public record QueryPacket(string Sql) : Packet
{
  public override PacketType Type => PacketType.Query;
}