namespace ArmDb.Network;

/// <summary>
/// Packet sent by the client to signal termination of the connection.
/// Format: [Type 'X'] [Length]
/// </summary>
public record TerminatePacket : Packet
{
  public override PacketType Type => PacketType.Terminate;
}