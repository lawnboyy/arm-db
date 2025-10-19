using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

internal abstract class BTreeNode
{
  protected readonly Page _page;
  protected readonly TableDefinition _tableDefinition;
  private readonly KeyComparer _keyComparer = new KeyComparer();

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
  /// Performs a binary search on the node page for the given search key.
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
      var midPointKeyBytes = SlottedPage.GetRecord(_page, mid);
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
}