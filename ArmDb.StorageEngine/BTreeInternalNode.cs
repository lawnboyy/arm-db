using ArmDb.DataModel;
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

  /// <summary>
  /// Use the given key to find the corresponding child pointer. We need to do a binary
  /// search across the interal node's records and find the smallest separator key that
  /// is larger than the given key. If such a key exists, return the corresponding child
  /// pointer. If no key exists, return the right-most (largest) separator key in this
  /// internal node.
  /// </summary>
  /// <param name="searchKey"></param>
  /// <returns></returns>
  /// <exception cref="NotImplementedException"></exception>
  internal PageId LookupChildPage(Key searchKey)
  {
    // TODO: Implement binary search over slots.
    // For each slot, deserialize the (Key, PageId) pair.
    // Use KeyComparer to find the correct child pointer.
    // Handle the right-most pointer case.
    throw new NotImplementedException();
  }

  // You will also need these helpers (or similar)
  internal static byte[] SerializeEntry(Key key, PageId childPageId, TableDefinition tableDef)
  {
    // First, construct a column list that represents the internal node record. This will be the primary key
    // columns, followed by the columns that make up the child pointer columns, the table ID and the page index.
    var nodeColumns = tableDef.GetPrimaryKeyColumnDefinitions();
    nodeColumns.Append(_tableIdColumnDefinition);
    nodeColumns.Append(_pageIndexColumnDefinition);

    // Construct our data row (should really be called record since it represents an abstraction below a data row)
    var tableId = DataValue.CreateInteger(childPageId.TableId);
    var pageIndex = DataValue.CreateInteger(childPageId.PageIndex);
    var values = key.Values.ToList();
    values.AddRange([tableId, pageIndex]);
    Record record = new Record(values);

    var bytes = RecordSerializer.Serialize(nodeColumns, record);

    return bytes;
  }

  internal static (Key key, PageId childPageId) DeserializeEntry(ReadOnlySpan<byte> recordData, TableDefinition tableDef)
  {
    // First, construct a column list that represents the internal node record. This will be the primary key
    // columns, followed by the columns that make up the child pointer columns, the table ID and the page index.
    var nodeColumns = tableDef.GetPrimaryKeyColumnDefinitions();
    nodeColumns.Append(_tableIdColumnDefinition);
    nodeColumns.Append(_pageIndexColumnDefinition);

    // Now we can deserialize an internal node record entry.
    var data = RecordSerializer.Deserialize(nodeColumns, recordData);

    if (data == null)
    {
      throw new Exception("Deserialized entry was null!");
    }

    var values = data.Values;
    var keyValues = values.Take(values.Count - 2).ToList();
    var key = new Key(keyValues);
    int tableId = (int)values[values.Count - 2].Value!;
    int pageIndex = (int)values[values.Count - 1].Value!;
    var pageId = new PageId(tableId, pageIndex);
    return (key, childPageId: pageId);
  }
}