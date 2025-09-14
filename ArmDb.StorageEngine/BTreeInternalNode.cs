using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

/// <summary>
/// Represents an internal node in a B*Tree clustered index. Each internal node a contain separator
/// key and a pointer to a child node that contains key values less than the separator key, but greater
/// than the previous separator key.
/// </summary>
internal sealed class BTreeInternalNode
{
  private readonly Page _page;
  private readonly TableDefinition _tableDef;
  private static ColumnDefinition _tableIdColumnDefinition = new ColumnDefinition("_internal_tableId", new DataTypeInfo(PrimitiveDataType.Int), false);
  private static ColumnDefinition _pageIndexColumnDefinition = new ColumnDefinition("_internal_pageIndex", new DataTypeInfo(PrimitiveDataType.Int), false);

  public BTreeInternalNode(Page page, TableDefinition tableDef)
  {
    ArgumentNullException.ThrowIfNull(page);
    ArgumentNullException.ThrowIfNull(tableDef);

    var header = SlottedPage.GetHeader(page);
    if (header.PageType == PageType.Invalid)
    {
      throw new ArgumentException($"Received an invalid Page!", "page");
    }
    if (header.PageType != PageType.InternalNode)
    {
      throw new ArgumentException($"Expected an internal node page but recieved: {header.PageType}", "page");
    }

    _page = page;
    _tableDef = tableDef;
  }
}