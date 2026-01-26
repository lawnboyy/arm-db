using ArmDb.SchemaDefinition;

namespace ArmDb.Network;

/// <summary>
/// Packet sent by the server to describe the columns (fields) in a result set.
/// Format: [Type 'T'] [Length] [FieldCount (2 bytes)] [Field1] [Field2] ...
/// </summary>
public record RowDescriptionPacket(IReadOnlyList<RowDescriptionPacket.FieldDescription> Fields) : Packet
{
  public override PacketType Type => PacketType.RowDescription;

  public record FieldDescription(string Name, PrimitiveDataType DataType);
}