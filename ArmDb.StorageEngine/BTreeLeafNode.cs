using System.Text;
using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.Storage.Exceptions;

namespace ArmDb.Storage;

/// <summary>
/// Represents a leaf node of a B+Tree structure. Leaf nodes are slotted pages that store
/// the actual table rows.
/// </summary>
internal sealed class BTreeLeafNode : BTreeNode
{
  internal int PageIndex => _page.Id.PageIndex;

  internal int PrevPageIndex
  {
    get
    {
      return new PageHeader(_page).PrevPageIndex;
    }
    set
    {
      var pageHeader = new PageHeader(_page);
      pageHeader.PrevPageIndex = value;
    }
  }

  internal int NextPageIndex
  {
    get
    {
      return new PageHeader(_page).NextPageIndex;
    }
    set
    {
      var pageHeader = new PageHeader(_page);
      pageHeader.NextPageIndex = value;
    }
  }

  internal BTreeLeafNode(Page page, TableDefinition tableDefinition) : base(page, tableDefinition)
  {
    var header = SlottedPage.GetHeader(page);
    if (header.PageType != PageType.LeafNode)
    {
      throw new ArgumentException($"Expected a leaf node page but recieved: {header.PageType}", "page");
    }
  }

  /// <summary>
  /// Searches the slotted page for the given search key. If an exact match is found, the slot index
  /// is returned as a positive value. If an exact match is not found, the insertion point is returned
  /// as the bitwise complement (i.e. a negative value).
  /// </summary>
  /// <param name="searchKey"></param>
  /// <returns>Positive integer value if exact match is found. Negative integer value representing
  /// the insertion point index if no exact match is found.</returns>
  internal override int FindPrimaryKeySlotIndex(Key searchKey)
  {
    return FindSlotIndex(searchKey, recordBytes =>
        RecordSerializer.DeserializePrimaryKey(_tableDefinition, recordBytes)
    );
  }

  /// <summary>
  /// Merges this leaf node's data into the left sibling node and reformats itself. The B*Tree
  /// orchestrator will call this if it determines the data for 2 sibling leaf nodes can fit
  /// into a single node. This would occur after a delete operation.
  /// </summary>
  /// <param name="leftSibling">The sibling to merge the data into.</param>
  /// <param name="rightSibling">The right most sibling. Needed to update linked list pointer to left node.</param>
  /// <exception cref="ArgumentNullException">Left sibling cannot be null.</exception>
  /// <exception cref="BTreeNodeFullException">Critical error in which merging this node's data into the left
  /// sibling causes it to overflow.</exception>
  internal void MergeLeft(BTreeLeafNode leftSibling, BTreeLeafNode? rightSibling = null)
  {
    if (leftSibling == null)
    {
      throw new ArgumentNullException(nameof(leftSibling));
    }

    if (!HasSufficientSpace(GetAllRawRecords(), leftSibling.FreeSpace))
    {
      throw new InvalidOperationException("Cannot merge into left node due to insufficient space.");
    }

    // Write all the records in this node to the left sibling.
    // Loop through this leaf node's slots, pull the raw record and append it to the left sibling's records.
    for (var slotIndex = 0; slotIndex < ItemCount; slotIndex++)
    {
      // Fetch the record using the slot offset...
      var rawRecord = SlottedPage.GetRawRecord(_page, slotIndex);
      // Write this record to the left sibling...
      if (!leftSibling.Append(rawRecord))
      {
        // If we were unable to append the record, then there was not enough space. The caller (B*Tree orchestrator) is responsible
        // for determining when a merge is possible, so this is an error condition that we cannot handle.
        throw new BTreeNodeFullException("Could not merge due to a left sibling overflow!");
      }
    }
    // Update the pointer to the right sibling node.
    leftSibling.NextPageIndex = NextPageIndex;

    // If there is a right sibling, point it back to the left node.
    if (rightSibling != null)
    {
      rightSibling.PrevPageIndex = leftSibling.PageIndex;
    }

    // Wipe this page...
    SlottedPage.Initialize(_page, PageType.LeafNode);
  }

  /// <summary>
  /// Wipes and rewrites the given records to the node.
  /// </summary>
  /// <param name="rawRecords"></param>
  internal void Repopulate(List<byte[]> rawRecords)
  {
    Repopulate(rawRecords, PageType.LeafNode);
  }

  /// <summary>
  /// Searches the leaf node page for a record with the given key. The data row is returned, if
  /// found, otherwise, null is returned.
  /// </summary>
  /// <param name="key">Key of the record to search for.</param>
  /// <returns>DataRow record if found, otherwise, null.</returns>
  internal Record? Search(Key key)
  {
    // Attempt to locate the record with the given key...
    var slotIndex = FindPrimaryKeySlotIndex(key);
    if (slotIndex >= 0)
    {
      var recordData = SlottedPage.GetRawRecord(_page, slotIndex);
      return RecordSerializer.Deserialize(_tableDefinition.Columns, recordData);
    }

    return null;
  }

  /// <summary>
  /// Logically inserts the given data row in its ordered position in an in-memory list. Then this leaf node's 
  /// rows are split based on the midpoint key. Data rows from index 0 up to, but excluding the midpoint, will 
  /// be re-written to this leaf node in order to compact the records. All the records from the midpoint to the
  /// end of the leaf node will be written to the given new leaf node.
  /// </summary>
  /// <remarks>
  /// This method should only be called if there is insufficient space to insert the given row in the page.
  /// If there is sufficient space an exception will be thrown.
  /// </remarks>
  /// <param name="rowToInsert">The new data row to insert.</param>
  /// <param name="newRightLeaf">New node that will house half the current records.</param>
  /// <returns>The separator key to promote to the parent node.</returns>
  internal Key SplitAndInsert(Record rowToInsert, BTreeLeafNode newRightLeaf, BTreeLeafNode? rightLeafSibling = null)
  {
    if (newRightLeaf.ItemCount != 0)
    {
      throw new ArgumentException("The new sibling leaf must be an empty, initialized page", nameof(newRightLeaf));
    }

    var thisLeafHeader = new PageHeader(_page);

    var newRawRecord = RecordSerializer.Serialize(_tableDefinition.Columns, rowToInsert);
    var totalSize = newRawRecord.Length;

    // Create a sorted list of data rows by looping through the existing leaf's records and adding them to the list. Include
    // the new row in sorted order.
    var sortedDataRows = new Record[thisLeafHeader.ItemCount + 1];
    var sortedRawRecords = new byte[thisLeafHeader.ItemCount + 1][];
    var dataKeyToInsert = rowToInsert.GetPrimaryKey(_tableDefinition);
    var keyComparer = new KeyComparer();

    int sortedRowIndex = 0;
    bool newRowInserted = false;
    for (int slotIndex = 0; slotIndex < ItemCount; slotIndex++)
    {
      // Get the record...
      var rawRecord = SlottedPage.GetRawRecord(_page, slotIndex);
      totalSize += rawRecord.Length;
      var dataRow = RecordSerializer.Deserialize(_tableDefinition.Columns, rawRecord);
      // The slot array is ordered, but we need to determine where to insert the new record...
      var currentDataRowKey = dataRow.GetPrimaryKey(_tableDefinition);
      // If the key to insert is less than the current data row, insert the new row first...
      if (!newRowInserted && keyComparer.Compare(dataKeyToInsert, currentDataRowKey) < 0)
      {
        sortedRawRecords[sortedRowIndex] = newRawRecord;
        sortedDataRows[sortedRowIndex++] = rowToInsert;
        newRowInserted = true;
      }
      sortedRawRecords[sortedRowIndex] = rawRecord.ToArray();
      sortedDataRows[sortedRowIndex++] = dataRow;
    }

    // If we never inserted the new row, it's the largest key so add it as the last element.
    if (sortedDataRows[sortedDataRows.Length - 1] == null)
    {
      sortedRawRecords[sortedDataRows.Length - 1] = newRawRecord;
      sortedDataRows[sortedDataRows.Length - 1] = rowToInsert;
    }

    var totalRows = sortedDataRows.Length;

    // Determine the midpoint...
    var midpoint = _tableDefinition.Columns.Any(k => k.DataType.PrimitiveType == PrimitiveDataType.Varchar)
      ? FindOptimalSplitIndexByByteLength(sortedRawRecords, totalSize)
      : totalRows / 2;

    // Get our separator key at this index...
    var midpointRow = sortedDataRows[midpoint];
    var separatortKey = midpointRow.GetPrimaryKey(_tableDefinition);

    // Capture the sibling pointers before we re-initialize our page to rewrite and
    // compact it.
    var originalHeader = new PageHeader(_page);
    int parentPageIndex = originalHeader.ParentPageIndex;
    int nextLeafIndex = originalHeader.NextPageIndex;

    // Wipe our original page so we can rewrite it for compaction to reclaim fragmented space...
    SlottedPage.Initialize(_page, PageType.LeafNode, parentPageIndex);

    // Write half our updated content to this leaf node...
    for (int i = 0; i < midpoint; i++)
    {
      if (!TryInsert(sortedDataRows[i]))
      {
        throw new InvalidOperationException("Insert failed after a split. The record to insert may be too large to fit on a page. TODO: Implement overflow pages.");
      }
    }

    // Write the second half of the data rows to the new node...
    for (int i = midpoint; i < sortedDataRows.Length; i++)
    {
      if (!newRightLeaf.TryInsert(sortedDataRows[i]))
      {
        throw new InvalidOperationException("Insert failed after a split. The record to insert may be too large to fit on a page. TODO: Implement overflow pages.");
      }
    }

    // Update the sibling pointers...
    // Splitting this node into 2; This Leaf <-> New Leaf <-> Right Sibling Leaf
    thisLeafHeader.NextPageIndex = newRightLeaf.PageIndex;
    newRightLeaf.NextPageIndex = nextLeafIndex;
    newRightLeaf.PrevPageIndex = this.PageIndex;

    if (rightLeafSibling != null)
    {
      rightLeafSibling.PrevPageIndex = newRightLeaf.PageIndex;
    }

    return separatortKey;
  }

  /// <summary>
  /// Attempts to insert a new row in the leaf page. If the key is a duplicate, an exception is
  /// thrown. If the page is full, then the leaf node will be split. If the key is not a duplicate
  /// and there is sufficient space, the row is inserted into the leaf node. Delegates to the base
  /// class TryInsert method.
  /// </summary>
  /// <param name="row"></param>
  /// <returns></returns>
  internal bool TryInsert(Record row)
  {
    // Call the BTreeNode base class TryInsert method...
    return TryInsert(row, _tableDefinition.Columns, recordBytes =>
        RecordSerializer.DeserializePrimaryKey(_tableDefinition, recordBytes));
  }

  /// <summary>
  /// Attempts to update the given row. It looks up the current record by primary key. If the row is not
  /// found, then it throws an exception. Otherwise, it returns the result of updated the record in the
  /// slotted page.
  /// </summary>
  /// <param name="updatedRow">The updated row</param>
  /// <returns>True if the row is updated successfully, false otherwise.</returns>
  /// <exception cref="InvalidOperationException">Thrown if no record is found using the given primary key.</exception>
  internal bool TryUpdate(Record updatedRow)
  {
    Key primaryKey = updatedRow.GetPrimaryKey(_tableDefinition);
    var slotIndex = FindPrimaryKeySlotIndex(primaryKey);

    if (slotIndex >= 0)
    {
      return SlottedPage.TryUpdateRecord(_page, slotIndex, RecordSerializer.Serialize(_tableDefinition.Columns, updatedRow));
    }

    throw new RecordNotFoundException("Record could not be found using the given primary key.");
  }

  public override string ToString()
  {
    StringBuilder stringBuilder = new();
    var header = new PageHeader(_page);
    stringBuilder.Append("[");
    for (int slotIndex = 0; slotIndex < header.ItemCount; slotIndex++)
    {
      // Fetch the record at the slot offset and deserialize it...
      var currentRawRecord = SlottedPage.GetRawRecord(_page, slotIndex);
      var primaryKey = RecordSerializer.DeserializePrimaryKey(_tableDefinition, currentRawRecord);
      var keyStr = primaryKey.ToString();
      stringBuilder.Append($"{keyStr},");
    }

    stringBuilder.Remove(stringBuilder.Length - 1, 1);
    stringBuilder.Append("]");

    return stringBuilder.ToString();
  }
}