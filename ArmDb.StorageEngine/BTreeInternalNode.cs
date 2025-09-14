using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

/// <summary>
/// Represents an internal node in a B*Tree clustered index. Each internal node a contain separator
/// key and a pointer to a child node that contains key values less than the separator key, but greater
/// than the previous separator key.
/// </summary>
internal sealed class BTreeInternalNode
{
  private static ColumnDefinition _tableIdColumnDefinition = new ColumnDefinition("_internal_tableId", new DataTypeInfo(PrimitiveDataType.Int), false);
  public static ColumnDefinition TableIdColumnDefinition => _tableIdColumnDefinition;

  private static ColumnDefinition _pageIndexColumnDefinition = new ColumnDefinition("_internal_pageIndex", new DataTypeInfo(PrimitiveDataType.Int), false);
  public static ColumnDefinition PageIndexColumnDefinition => _pageIndexColumnDefinition;
}