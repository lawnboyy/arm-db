namespace ArmDb.Network;

/// <summary>
/// Packet sent by the server to indicate successful authentication.
/// Format: [Type 'R'] [Length]
/// </summary>
public record AuthenticationOkPacket : Packet
{
  public override PacketType Type => PacketType.AuthenticationOk;
}