namespace ArmDb.Network;

/// <summary>
/// Packet sent by the server to signal the completion of a SQL command.
/// Format: [Type 'C'] [Length] [Tag (String)]
/// </summary>
public record CommandCompletePacket(string Tag) : Packet
{
  public override PacketType Type => PacketType.CommandComplete;
}