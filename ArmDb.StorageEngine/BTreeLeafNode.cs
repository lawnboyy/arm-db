using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

/// <summary>
/// Represents a leaf node of a B+Tree structure. Leaf nodes are slotted pages that store
/// the actual table records.
/// </summary>
internal sealed class BTreeLeafNode
{
  private readonly Page _page;
  private readonly TableDefinition _tableDefinition;

  public BTreeLeafNode(Page page, TableDefinition tableDefinition)
  {
    ArgumentNullException.ThrowIfNull(page);
    ArgumentNullException.ThrowIfNull(tableDefinition);

    var header = SlottedPage.GetHeader(page);
    if (header.PageType == PageType.Invalid)
    {
      throw new ArgumentException($"Received an invalid Page!", "page");
    }
    if (header.PageType != PageType.LeafNode)
    {
      throw new ArgumentException($"Expected a leaf node page but recieved: {header.PageType}", "page");
    }

    _page = page;
    _tableDefinition = tableDefinition;
  }
}