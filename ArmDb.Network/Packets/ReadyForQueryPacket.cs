namespace ArmDb.Network;

/// <summary>
/// Packet sent by the server to signal it is ready for a new query cycle.
/// Format: [Type 'Z'] [Length] [TransactionStatus (1 byte)]
/// </summary>
public record ReadyForQueryPacket(char TransactionStatus) : Packet
{
  public override PacketType Type => PacketType.ReadyForQuery;
}