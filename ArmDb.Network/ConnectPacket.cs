namespace ArmDb.Network;

/// <summary>
/// Packet sent by the client to initiate a connection.
/// Format: [Type 'C'] [Length] [ProtocolVersion (4 bytes)]
/// </summary>
public record ConnectPacket : Packet
{
  public override PacketType Type => PacketType.Connect;

  /// <summary>
  /// The protocol version requested by the client.
  /// </summary>
  public int ProtocolVersion { get; }

  public ConnectPacket(int protocolVersion)
  {
    ProtocolVersion = protocolVersion;
  }
}