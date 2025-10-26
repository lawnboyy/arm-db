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

  internal void SetRightmostChildId(int pageIndex) => new PageHeader(_page).RightmostChildPageIndex = pageIndex;

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
    if (searchKey == null)
    {
      throw new ArgumentNullException(nameof(searchKey), "Search key cannot be null!");
    }

    // All the records for this page and it's children will have the same table ID since it is a clustered
    // index for a single table.
    var tableId = _page.Id.TableId;
    var header = SlottedPage.GetHeader(_page);

    // Use slot lookup binary search to determine which record to lookup and deserialize the child page pointer.
    var slotIndex = FindSlotIndex(searchKey, recordData => DeserializeRecordKey(_tableDefinition, recordData));

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
        var recordData = SlottedPage.GetRawRecord(_page, convertedIndex);
        var (key, childPageId) = DeserializeRecord(_tableDefinition, recordData);
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
        var recordData = SlottedPage.GetRawRecord(_page, adjustedSlotIndex);
        var (key, childPageId) = DeserializeRecord(_tableDefinition, recordData);
        return childPageId;
      }
    }
  }

  /// <summary>
  /// Attempts to insert a new record, a (key, child pointer) pair, into sorted order in the internal node. If 
  /// the insert is successful it returns true, false if the node is full and needs to be split.
  /// </summary>
  /// <param name="separatorKey">The separator key of the record to insert</param>
  /// <param name="childPageId">The child pointer of the record to insert</param>
  /// <returns>True, if the new internal node record is inserted successfully</returns>
  internal bool TryInsert(Key separatorKey, PageId childPageId)
  {
    // Construct the record we want to insert into the internal node. This is a (separator key, child pointer) pair.
    var recordToInsert = GetRecord(separatorKey, childPageId);
    return TryInsert(recordToInsert);
  }

  /// <summary>
  /// This method splits this internal node, adds the new separator key entry that points to the given child page ID,
  /// then promotes the midpoint separator key record to its parent node, removing it from this internal node.
  /// </summary>
  /// <param name="newSeparatorKey">The new separator key to insert. It will either be in this node, the new sibling, or
  /// promoted to this internal node's parent.</param>
  /// <param name="childPageId">The page ID of the child this new separator key points to.</param>
  /// <param name="newSiblingNode">A new right side sibling node to move half the records of this node to as part of the split.</param>
  /// <returns>The midpoint separator key, removed from the current node, to promote to the parent.</returns>
  internal Key SplitAndInsert(Key newSeparatorKey, PageId childPageId, BTreeInternalNode newSiblingNode)
  {
    if (newSiblingNode.ItemCount != 0)
    {
      throw new ArgumentException("The new sibling must be an empty, initialized page", nameof(newSiblingNode));
    }

    var thisNodeHeader = new PageHeader(_page);

    // Create a record for the new entry to insert.
    var newRecord = new Record([
      .. newSeparatorKey.Values,
      DataValue.CreateInteger(childPageId.TableId),
      DataValue.CreateInteger(childPageId.PageIndex)
    ]);

    // Create a sorted list of all the records for this node, including the internal node record we want to
    // add.
    var sortedRecords = new Record[thisNodeHeader.ItemCount + 1];
    var keyComparer = new KeyComparer();
    var keyColumns = _tableDefinition.GetPrimaryKeyColumnDefinitions();

    int sortedRecordIndex = 0;
    bool newRecordInserted = false;
    for (int slotIndex = 0; slotIndex < thisNodeHeader.ItemCount; slotIndex++)
    {
      // Fetch the record at the slot offset and deserialize it...
      var currentRawRecord = SlottedPage.GetRawRecord(_page, slotIndex);
      var internalNodeRecord = RecordSerializer.Deserialize(GetInternalNodeColumnDefinitions(keyColumns), currentRawRecord);
      // The slot array is ordered, but we need to determine where to insert the new record...
      var currentRecordKey = internalNodeRecord.GetPrimaryKey(_tableDefinition);
      // If the key to insert is less than the current data row, insert the new row first...
      if (!newRecordInserted && keyComparer.Compare(newSeparatorKey, currentRecordKey) < 0)
      {
        sortedRecords[sortedRecordIndex++] = newRecord;
        newRecordInserted = true;
      }
      sortedRecords[sortedRecordIndex++] = internalNodeRecord;
    }

    // If we never inserted the new record, then it's the largest separator key so add it at the end...
    if (sortedRecords[sortedRecords.Length - 1] == null)
    {
      sortedRecords[sortedRecords.Length - 1] = newRecord;
    }

    var totalRecords = sortedRecords.Length;

    // Determine the midpoint key to promote to the parent.
    var midpoint = totalRecords / 2;

    // Get the separator key to promote
    var midpointRecord = sortedRecords[midpoint];
    var (keyToPromote, midpointPageId) = ExtractKeyAndPageId(_tableDefinition, midpointRecord);
    var parentPageIndex = thisNodeHeader.ParentPageIndex;

    // Capture the right-most child pointer of this node so we can move it to the new right hand sibling.
    var newSiblingRightmostChildPageIndex = thisNodeHeader.RightmostChildPageIndex;

    // Wipe our original page to prep for rewriting half the records and reclaiming fragmented space...
    SlottedPage.Initialize(_page, PageType.InternalNode, parentPageIndex);

    // Write the first half of our recoreds to this internal node...
    for (int i = 0; i < midpoint; i++)
    {
      if (!TryInsert(sortedRecords[i]))
      {
        throw new Exception("Insert failed after a split! Something went terribly wrong since all inserts after a re-init of the leaf page are guaranteed to succeed.");
      }
    }

    thisNodeHeader.RightmostChildPageIndex = midpointPageId.PageIndex;

    // Write the second half of the data rows to the new node...
    for (int i = midpoint + 1; i < sortedRecords.Length; i++)
    {
      if (!newSiblingNode.TryInsert(sortedRecords[i]))
      {
        throw new Exception("Insert failed after a split! Something went terribly wrong since all inserts on a fresh new leaf page are guaranteed to succeed.");
      }
    }

    // Set the new right node's right most pointer...
    newSiblingNode.SetRightmostChildId(newSiblingRightmostChildPageIndex);

    return keyToPromote;
  }

  internal static byte[] SerializeRecord(Key key, PageId childPageId, TableDefinition tableDef)
  {
    // First, construct a column list that represents the internal node record. This will be the primary key
    // columns, followed by the columns that make up the child pointer columns, the table ID and the page index.
    var keyColumns = tableDef.GetPrimaryKeyColumnDefinitions();
    var recordColumns = new List<ColumnDefinition>();
    recordColumns.AddRange(keyColumns);
    recordColumns.Add(_tableIdColumnDefinition);
    recordColumns.Add(_pageIndexColumnDefinition);

    // Construct our record for the internal node.
    var record = GetRecord(key, childPageId);

    var bytes = RecordSerializer.Serialize(recordColumns, record);

    return bytes;
  }

  internal static (Key key, PageId childPageId) DeserializeRecord(TableDefinition tableDef, ReadOnlySpan<byte> recordData)
  {
    // First, construct a column list that represents the internal node record. This will be the primary key
    // columns, followed by the columns that make up the child pointer columns, the table ID and the page index.
    var keyColumns = tableDef.GetPrimaryKeyColumnDefinitions();
    var recordColumns = new List<ColumnDefinition>();
    recordColumns.AddRange(keyColumns);
    recordColumns.Add(_tableIdColumnDefinition);
    recordColumns.Add(_pageIndexColumnDefinition);

    // Now we can deserialize an internal node record entry.
    var record = RecordSerializer.Deserialize(recordColumns, recordData);

    return ExtractKeyAndPageId(tableDef, record);
  }

  private bool TryInsert(Record recordToInsert)
  {
    var keyColumns = _tableDefinition.GetPrimaryKeyColumnDefinitions();
    // Get the record column definition list for internal nodes.
    var recordColumns = GetInternalNodeColumnDefinitions(keyColumns);
    // Delegate the insert to the BTreeNode base class.
    return TryInsert(recordToInsert, recordColumns, recordData => DeserializeRecordKey(_tableDefinition, recordData));
  }

  private static Key DeserializeRecordKey(TableDefinition tableDef, ReadOnlySpan<byte> recordData)
  {
    // First, construct a column list that represents the internal node record. This will be the primary key
    // columns, followed by the columns that make up the child pointer columns, the table ID and the page index.
    var keyColumns = tableDef.GetPrimaryKeyColumnDefinitions();

    if (keyColumns == null)
    {
      throw new ArgumentNullException("Could not determine the primary key columns.");
    }

    var recordColumns = GetInternalNodeColumnDefinitions(keyColumns);

    // Now we can deserialize an internal node record entry's key.
    var data = RecordSerializer.DeserializeKey(recordColumns, keyColumns, recordData);

    if (data == null)
    {
      throw new Exception("Deserialized entry was null!");
    }

    var key = new Key(data.Values);
    return key;
  }

  private static Record GetRecord(Key separatorKey, PageId childPageId)
  {
    var values = separatorKey.Values.ToList();
    var tableId = DataValue.CreateInteger(childPageId.TableId);
    var pageIndex = DataValue.CreateInteger(childPageId.PageIndex);
    values.AddRange([tableId, pageIndex]);
    Record record = new Record(values);

    return record;
  }

  private static IReadOnlyList<ColumnDefinition> GetInternalNodeColumnDefinitions(ColumnDefinition[] keyColumns)
  {
    var recordColumns = new List<ColumnDefinition>();
    recordColumns.AddRange(keyColumns);
    recordColumns.Add(_tableIdColumnDefinition);
    recordColumns.Add(_pageIndexColumnDefinition);

    return recordColumns;
  }

  private static (Key key, PageId childPageId) ExtractKeyAndPageId(TableDefinition tableDef, Record record)
  {
    if (record == null)
    {
      throw new Exception("Deserialized entry was null!");
    }

    var values = record.Values;
    var keyValues = values.Take(values.Count - 2).ToList();
    var key = new Key(keyValues);
    int tableId = (int)values[values.Count - 2].Value!;
    int pageIndex = (int)values[values.Count - 1].Value!;
    var pageId = new PageId(tableId, pageIndex);
    return (key, childPageId: pageId);
  }

#if DEBUG
  // Helper for test setup
  internal void InsertEntryForTest(Key key, PageId childPageId)
  {
    // Simplified insert that assumes order and space, for test setup only
    var header = new PageHeader(_page);
    var entryBytes = SerializeRecord(key, childPageId, _tableDefinition);
    SlottedPage.TryAddRecord(_page, entryBytes, header.ItemCount);
  }

  // Helper for test verification
  internal (Key key, PageId childPageId) GetEntryForTest(int slotIndex)
  {
    var recordBytes = SlottedPage.GetRawRecord(_page, slotIndex);
    return DeserializeRecord(_tableDefinition, recordBytes);
  }
#endif
}