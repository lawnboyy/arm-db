using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
using ArmDb.SchemaDefinition;

namespace ArmDb.Storage;

internal abstract class BTreeNode
{
  protected readonly Page _page;
  protected readonly TableDefinition _tableDefinition;
  private readonly KeyComparer _keyComparer = new KeyComparer();

  internal int ItemCount => new PageHeader(_page).ItemCount;

  internal PageId PageId => _page.Id;

  internal int ParentPageIndex => new PageHeader(_page).ParentPageIndex;

  internal int RightmostChildIndex => new PageHeader(_page).RightmostChildPageIndex;

  internal int FreeSpace => SlottedPage.GetFreeSpace(_page);

  internal BTreeNode(Page page, TableDefinition tableDefinition)
  {
    ArgumentNullException.ThrowIfNull(page);
    ArgumentNullException.ThrowIfNull(tableDefinition);

    var header = SlottedPage.GetHeader(page);
    if (header.PageType == PageType.Invalid)
    {
      throw new ArgumentException($"Received an invalid Page!", "page");
    }

    _page = page;
    _tableDefinition = tableDefinition;
  }

  internal abstract int FindPrimaryKeySlotIndex(Key keyToDelete);

  /// <summary>
  /// Appends a raw record to the page. This method is meant to be used for performance when merging
  /// 2 nodes together. If we are merging the right node's records into the left node, then we know
  /// the data is already correctly ordered without needing to deserialize the key and check for the
  /// insertion point.
  /// </summary>
  /// <param name="rawRecord">The raw record to write to the next slot in this node.</param>
  /// <returns>True if the record is successfully appended.</returns>
  internal bool Append(ReadOnlySpan<byte> rawRecord)
  {
    return SlottedPage.TryAddRecord(_page, rawRecord, ItemCount);
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
  /// Returns all the raw records stored in this node. This is a performant way to pull all the records
  /// when deserialization is not needed. The B*Tree orchestrator will use this method to pull all the
  /// records when a redistribution is necessary to balance records between sibling nodes. It can pull
  /// the records for a left node and a right node, combine them, then write the first half of the records
  /// to the left node and the second half of the records to the right node. The records are guaranteed
  /// to be returned in sorted order so a simple concatenation will guaranteed that the redistribution
  /// list will be sorted without the need to deserialize primary keys.
  /// </summary>
  /// <returns>Read-only list of byte arrays representing the raw records. The list is guaranteed
  /// to be in sorted order.</returns>
  internal IReadOnlyList<byte[]> GetAllRawRecords()
  {
    var rawRecords = new List<byte[]>();
    for (var slotIndex = 0; slotIndex < ItemCount; slotIndex++)
    {
      var rawRecord = SlottedPage.GetRawRecord(_page, slotIndex).ToArray();

      rawRecords.Add(rawRecord);
    }

    return rawRecords;
  }

  /// <summary>
  /// Calculates the number of data in bytes currently stored on this page.
  /// </summary>
  /// <returns>The number of bytes stored on the page.</returns>
  internal int GetBytesUsed()
  {
    return Page.Size - SlottedPage.GetFreeSpace(_page);
  }

  /// <summary>
  /// Attempts to insert a new record in the page. If the key is a duplicate, an exception is
  /// thrown. If the page is full, the method returns false indicating the node must be split. 
  /// If the key is not a duplicate and there is sufficient space, the row is inserted into the
  /// node.
  /// </summary>
  /// <param name="record"></param>
  /// <returns></returns>
  internal bool TryInsert(Record record, IReadOnlyList<ColumnDefinition> columnDefinitions, Func<ReadOnlySpan<byte>, Key> deserializeKey)
  {
    // Find the primary key...
    var primaryKey = record.GetPrimaryKey(_tableDefinition);

    // Check if there is space available to insert the node...
    var freeSpace = SlottedPage.GetFreeSpace(_page);
    var serializedRecord = RecordSerializer.Serialize(columnDefinitions, record);
    // If there is not enough space, don't insert the row and return false;
    if (serializedRecord.Length + Slot.Size > freeSpace)
    {
      return false;
    }

    // Insert the node.
    var slotIndex = FindSlotIndex(primaryKey, deserializeKey);

    // If the slot index is postive, then the key was found and we cannot insert a duplicate...
    if (slotIndex >= 0)
    {
      throw new DuplicateKeyException($"The key '{primaryKey}' already exists");
    }

    var convertedIndex = ~slotIndex;

    SlottedPage.TryAddRecord(_page, serializedRecord, convertedIndex);

    return true;
  }

  /// <summary>
  /// Method that determines the midpoint index for a node split based on data size rather than a simple
  /// index midpoint. This is necessary for ensuring that data is optimally distributed between nodes 
  /// after a split operation in the case of variable length data.
  /// </summary>
  /// <param name="sortedRawRecords">Two dimensional array representing the total set of records in this node.</param>
  /// <param name="totalSize">The total data size of the records.</param>
  /// <returns></returns>
  protected int FindOptimalSplitIndexByByteLength(byte[][] sortedRawRecords, int totalSize)
  {
    var halfOfTotal = totalSize / 2;
    var currentDataSize = 0;
    var index = 0;
    foreach (var rawRecord in sortedRawRecords)
    {
      // First add our record size to our current total...
      currentDataSize += rawRecord.Length;

      // Now compare it half the total size...
      if (currentDataSize > halfOfTotal)
      {
        // As soon as we cross over half the total, then we have found our midpoint index...
        return index;
      }

      index++;
    }

    return index;
  }

  /// <summary>
  /// Performs a binary search on the node slotted page for the given search key.
  /// </summary>
  /// <param name="searchKey"></param>
  /// <param name="deserializeKey"></param>
  /// <returns>Positive integer value if exact match is found. Negative integer value representing
  /// the bitwise complement of the insertion point index if no exact match is found.</returns>
  protected int FindSlotIndex(Key searchKey, Func<ReadOnlySpan<byte>, Key> deserializeKey)
  {
    // Get the item count from the page header...
    var pageHeader = new PageHeader(_page);
    var itemCount = pageHeader.ItemCount;

    // Start our binary search through the records...
    var low = 0;
    var high = itemCount - 1;
    while (low <= high)
    {
      var mid = low + (high - low) / 2;
      var midPointKeyBytes = SlottedPage.GetRawRecord(_page, mid);
      var midPointKey = deserializeKey(midPointKeyBytes);
      var compareResult = _keyComparer.Compare(searchKey, midPointKey);

      if (compareResult == 0)
        return mid;
      else if (compareResult < 0)
        high = mid - 1;
      else if (compareResult > 0)
        low = mid + 1;
    }

    // If the loop completes, then we did not find an exact match, but the low
    // value contains the insertion point. We return the bitwise complement
    // of the insertion index.
    return ~low;
  }

  protected void Repopulate(List<byte[]> rawRecords, PageType pageType)
  {
    if (rawRecords == null)
    {
      throw new ArgumentNullException("sortedRawRecords");
    }

    // First check the available space to make sure it's enough for the incoming records...
    var freeSpaceInBytes = SlottedPage.EMPTY_PAGE_FREE_SPACE;

    if (!HasSufficientSpace(rawRecords, freeSpaceInBytes))
    {
      throw new InvalidOperationException("Data for repopulating is too large to fit on a single page.");
    }

    // Wipe the page, but maintain the parent page index.
    var parentPageIndex = new PageHeader(_page).ParentPageIndex;
    SlottedPage.Initialize(_page, pageType, parentPageIndex);

    // Write the given records to the page.
    for (var slotIndex = 0; slotIndex < rawRecords.Count; slotIndex++)
    {
      if (!SlottedPage.TryAddRecord(_page, rawRecords[slotIndex], slotIndex))
      {
        // If there is not enough space, then we need to throw...
        // This should not ever happen because we've already verified there
        // is enough space to write the given records.
        throw new InvalidOperationException("Detected an overflow after verifying the required space to re-populate. Something has gone terribly wrong!");
      }
    }
  }

  protected bool HasSufficientSpace(IReadOnlyList<byte[]> rawRecords, int spaceAvailable)
  {
    // We'll need enough space for the records themselves, as well as their corresponding
    // slots, one per record.
    var totalSizeOfRecords = rawRecords.Aggregate(0, (total, bytes) =>
    {
      total += bytes.Length;
      return total;
    });
    var spaceNeededInBytes = totalSizeOfRecords + rawRecords.Count * Slot.Size;

    return spaceNeededInBytes <= spaceAvailable;
  }

  internal void SetParentPageIndex(int pageIndex) => new PageHeader(_page).ParentPageIndex = pageIndex;
}