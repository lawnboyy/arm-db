namespace ArmDb.Network;

/// <summary>
/// Abstract base class for all network packets in the ArmDb protocol.
/// </summary>
public abstract class Packet
{
  /// <summary>
  /// Gets the type of the packet.
  /// </summary>
  public abstract PacketType Type { get; }
}