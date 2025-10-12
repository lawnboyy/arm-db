using ArmDb.DataModel;
using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

/// <summary>
/// Represents an internal node in a B*Tree clustered index. Each internal node contains a separator
/// key and a pointer to a child node that contains key values less than the separator key, but greater
/// than the previous separator key.
/// </summary>
internal sealed class BTreeInternalNode : BTreeNode
{
  private static ColumnDefinition _tableIdColumnDefinition = new ColumnDefinition("_internal_tableId", new DataTypeInfo(PrimitiveDataType.Int), false);
  private static ColumnDefinition _pageIndexColumnDefinition = new ColumnDefinition("_internal_pageIndex", new DataTypeInfo(PrimitiveDataType.Int), false);

  public BTreeInternalNode(Page page, TableDefinition tableDef) : base(page, tableDef)
  {
    var header = SlottedPage.GetHeader(page);
    if (header.PageType != PageType.InternalNode)
    {
      throw new ArgumentException($"Expected an internal node page but recieved: {header.PageType}", "page");
    }
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
    // All the records for this page and it's children will have the same table ID since it is a clustered
    // index for a single table.
    var tableId = _page.Id.TableId;
    var header = SlottedPage.GetHeader(_page);

    // Use slot lookup binary search to determine which record to lookup and deserialize the child page pointer.
    var slotIndex = FindSlotIndex(searchKey, recordData => DeserializeEntryKey(_tableDefinition, recordData));

    // If we get a slot index that is less than zero, then that means we did not find an exact match and the index
    // is the insertion point. In the case of our internal node, this index will be used to look up the separator
    // key we need to find the child page pointer.
    if (slotIndex < 0)
    {
      var convertedIndex = ~slotIndex;

      // If the slot index of the insertion point is equal to the number of records in the page, then the key we're looking for
      // is greater than or equal to the largest key and we need to return the right-most child pointer.
      if (convertedIndex >= header.ItemCount)
      {
        // Construct the page ID of the right-most child pointer to return...        
        var rightmostChildPageId = new PageId(tableId, header.RightmostChildPageIndex);
        return rightmostChildPageId;
      }
      else // Return the separator key node's child pointer.
      {
        // Return the page ID associated with the record at the given slot.
        var recordData = SlottedPage.GetRecord(_page, convertedIndex);
        var (key, childPageId) = DeserializeEntry(_tableDefinition, recordData);
        return childPageId;
      }
    }
    else // We got a separate key match. The separator keys are the exclusive upper bound, so we need to return the pointer to the right of the separator key.
    {
      // Since the key is a match for a separator key which is an upper bounds, we want to return the slot to the right of the found key.
      var adjustedSlotIndex = slotIndex + 1;
      // First check if the slot index is equal to or greater than the number of records.
      // If so, then we need to return the right-most pointer.
      if (adjustedSlotIndex == header.ItemCount)
      {
        // Construct the page ID of the right-most child pointer to return...        
        var rightmostChildPageId = new PageId(tableId, header.RightmostChildPageIndex);
        return rightmostChildPageId;
      }
      else // Otherwise, we need to return the child page ID of the separator key node to the right of the slot index.
      {
        // Return the page ID associated with the record at the given slot.
        var recordData = SlottedPage.GetRecord(_page, adjustedSlotIndex);
        var (key, childPageId) = DeserializeEntry(_tableDefinition, recordData);
        return childPageId;
      }
    }
  }

  // You will also need these helpers (or similar)
  internal static byte[] SerializeEntry(Key key, PageId childPageId, TableDefinition tableDef)
  {
    // First, construct a column list that represents the internal node record. This will be the primary key
    // columns, followed by the columns that make up the child pointer columns, the table ID and the page index.
    var keyColumns = tableDef.GetPrimaryKeyColumnDefinitions();
    var recordColumns = new List<ColumnDefinition>();
    recordColumns.AddRange(keyColumns);
    recordColumns.Add(_tableIdColumnDefinition);
    recordColumns.Add(_pageIndexColumnDefinition);

    // Construct our data row (should really be called record since it represents an abstraction below a data row)
    var tableId = DataValue.CreateInteger(childPageId.TableId);
    var pageIndex = DataValue.CreateInteger(childPageId.PageIndex);
    var values = key.Values.ToList();
    values.AddRange([tableId, pageIndex]);
    Record record = new Record(values);

    var bytes = RecordSerializer.Serialize(recordColumns, record);

    return bytes;
  }

  internal static (Key key, PageId childPageId) DeserializeEntry(TableDefinition tableDef, ReadOnlySpan<byte> recordData)
  {
    // First, construct a column list that represents the internal node record. This will be the primary key
    // columns, followed by the columns that make up the child pointer columns, the table ID and the page index.
    var keyColumns = tableDef.GetPrimaryKeyColumnDefinitions();
    var recordColumns = new List<ColumnDefinition>();
    recordColumns.AddRange(keyColumns);
    recordColumns.Add(_tableIdColumnDefinition);
    recordColumns.Add(_pageIndexColumnDefinition);

    // Now we can deserialize an internal node record entry.
    var data = RecordSerializer.Deserialize(recordColumns, recordData);

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

  private static Key DeserializeEntryKey(TableDefinition tableDef, ReadOnlySpan<byte> recordData)
  {
    // First, construct a column list that represents the internal node record. This will be the primary key
    // columns, followed by the columns that make up the child pointer columns, the table ID and the page index.
    var keyColumns = tableDef.GetPrimaryKeyColumnDefinitions();
    var recordColumns = new List<ColumnDefinition>();
    recordColumns.AddRange(keyColumns);
    recordColumns.Add(_tableIdColumnDefinition);
    recordColumns.Add(_pageIndexColumnDefinition);

    // Now we can deserialize an internal node record entry's key.
    var data = RecordSerializer.DeserializeKey(recordColumns, keyColumns, recordData);

    if (data == null)
    {
      throw new Exception("Deserialized entry was null!");
    }

    var key = new Key(data.Values);
    return key;
  }
}