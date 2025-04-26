using ArmDb.SchemaDefinition;
using System.Text.Json.Serialization;

namespace ArmDb.Server.Bootstrap;

/// <summary>
/// Serializable surrogate class for DataTypeInfo. Internal to bootstrap process.
/// </summary>
internal sealed class DataTypeInfoSerializable
{
  [JsonConverter(typeof(JsonStringEnumConverter))]
  public PrimitiveDataType PrimitiveType { get; init; } = PrimitiveDataType.Unknown;

  public int? MaxLength { get; init; }
  public int? Precision { get; init; }
  public int? Scale { get; init; }

  public DataTypeInfoSerializable() { }

  public DataTypeInfo ToDataTypeInfo()
  {
    return new DataTypeInfo(PrimitiveType, MaxLength, Precision, Scale);
  }
}