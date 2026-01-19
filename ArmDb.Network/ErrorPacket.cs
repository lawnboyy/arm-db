namespace ArmDb.Network;

/// <summary>
/// Packet sent by the server when an error occurs.
/// Format: [Type 'E'] [Length] [Severity (1 byte)] [Message (String)]
/// </summary>
public record ErrorPacket(byte Severity, string Message) : Packet
{
  public override PacketType Type => PacketType.Error;
}