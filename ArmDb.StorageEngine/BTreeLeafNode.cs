using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine.Exceptions;

namespace ArmDb.StorageEngine;

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
  /// Searches the page for a record with a matching key value. If none is
  /// found, false is returned. If a match is found, the record is deleted
  /// by removing the corresponding slot index and compacting the slot
  /// array.
  /// </summary>
  /// <param name="keyToDelete">The key of the record to delete.</param>
  /// <returns>True if the delete is successful, otherwise, false.</returns>
  /// <exception cref="ArgumentNullException">Throws if the given key is null.</exception>
  internal bool Delete(Key keyToDelete)
  {
    if (keyToDelete == null)
    {
      throw new ArgumentNullException("keyToDelete", "Given key to delete is null!");
    }

    var slotIndex = FindPrimaryKeySlotIndex(keyToDelete);

    if (slotIndex >= 0)
    { // If the slot index is greater or equal to zero, then we found a match and can delete it.
      SlottedPage.DeleteRecord(_page, slotIndex);
      return true;
    }

    return false;
  }

  /// <summary>
  /// Searches the clustered index for the given search key. If an exact match is found, the slot index
  /// is returned as a positive value. If an exact match is not found, the insertion point is returned
  /// as the bitwise complement (i.e. a negative value).
  /// </summary>
  /// <param name="searchKey"></param>
  /// <returns>Positive integer value if exact match is found. Negative integer value representing
  /// the insertion point index if no exact match is found.</returns>
  internal int FindPrimaryKeySlotIndex(Key searchKey)
  {
    // It provides the specific key deserialization logic as a lambda
    // to the generic internal helper.
    return FindSlotIndex(searchKey, recordBytes =>
        RecordSerializer.DeserializePrimaryKey(_tableDefinition, recordBytes)
    );
  }

  internal void MergeLeft(BTreeLeafNode leftSibling, BTreeLeafNode? rightSibling = null)
  {
    if (leftSibling == null)
    {
      throw new ArgumentNullException(nameof(leftSibling));
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
  /// <param name="newLeaf">New node that will house half the current records.</param>
  /// <returns>The separator key to promote to the parent node.</returns>
  /// <exception cref="NotImplementedException"></exception>
  internal Key SplitAndInsert(Record rowToInsert, BTreeLeafNode newLeaf, BTreeLeafNode? rightLeafSibling = null)
  {
    if (newLeaf.ItemCount != 0)
    {
      throw new ArgumentException("The new sibling leaf must be an empty, initialized page", nameof(newLeaf));
    }

    var thisLeafHeader = new PageHeader(_page);

    // Create a sorted list of data rows by looping through the existing leaf's records and adding them to the list. Include
    // the new row in sorted order.
    var sortedDataRows = new Record[thisLeafHeader.ItemCount + 1];
    var dataKeyToInsert = rowToInsert.GetPrimaryKey(_tableDefinition);
    var keyComparer = new KeyComparer();

    int sortedRowIndex = 0;
    bool newRowInserted = false;
    for (int slotIndex = 0; slotIndex < ItemCount; slotIndex++)
    {
      // Get the record...
      var rawRecord = SlottedPage.GetRawRecord(_page, slotIndex);
      var dataRow = RecordSerializer.Deserialize(_tableDefinition.Columns, rawRecord);
      // The slot array is ordered, but we need to determine where to insert the new record...
      var currentDataRowKey = dataRow.GetPrimaryKey(_tableDefinition);
      // If the key to insert is less than the current data row, insert the new row first...
      if (!newRowInserted && keyComparer.Compare(dataKeyToInsert, currentDataRowKey) < 0)
      {
        sortedDataRows[sortedRowIndex++] = rowToInsert;
        newRowInserted = true;
      }
      sortedDataRows[sortedRowIndex++] = dataRow;
    }

    // If we never inserted the new row, it's the largest key so add it as the last element.
    if (sortedDataRows[sortedDataRows.Length - 1] == null)
    {
      sortedDataRows[sortedDataRows.Length - 1] = rowToInsert;
    }

    var totalRows = sortedDataRows.Length;

    // Determine the midpoint...
    var midpoint = totalRows / 2;

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
        throw new Exception("Insert failed after a split! Something went terribly wrong since all inserts after a re-init of the leaf page are guaranteed to succeed.");
      }
    }

    // Write the second half of the data rows to the new node...
    for (int i = midpoint; i < sortedDataRows.Length; i++)
    {
      if (!newLeaf.TryInsert(sortedDataRows[i]))
      {
        throw new Exception("Insert failed after a split! Something went terribly wrong since all inserts on a fresh new leaf page are guaranteed to succeed.");
      }
    }

    // Update the sibling pointers...
    // Splitting this node into 2; This Leaf <-> New Leaf <-> Right Sibling Leaf
    thisLeafHeader.NextPageIndex = newLeaf.PageIndex;
    newLeaf.NextPageIndex = nextLeafIndex;
    newLeaf.PrevPageIndex = this.PageIndex;

    if (rightLeafSibling != null)
    {
      rightLeafSibling.PrevPageIndex = newLeaf.PageIndex;
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
}