using ArmDb.DataModel;
using ArmDb.DataModel.Exceptions;
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
  private readonly KeyComparer _keyComparer = new KeyComparer();

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

  /// <summary>
  /// Searches the leaf node page for a record with the given key. The data row is returned, if
  /// found, otherwise, null is returned.
  /// </summary>
  /// <param name="key">Key of the record to search for.</param>
  /// <returns>DataRow record if found, otherwise, null.</returns>
  internal DataRow? Search(Key key)
  {
    // Attempt to locate the record with the given key...
    var slotIndex = FindPrimaryKeySlotIndex(key);
    if (slotIndex >= 0)
    {
      var recordData = SlottedPage.GetRecord(_page, slotIndex);
      return RecordSerializer.Deserialize(_tableDefinition, recordData);
    }

    return null;
  }

  /// <summary>
  /// Attempts to insert a new row in the leaf page. If the key is a duplicate, an exception is
  /// thrown. If the page is full, then the leaf node will be split. If the key is not a duplicate
  /// and there is sufficient space, the row is inserted into the leaf node.
  /// </summary>
  /// <param name="row"></param>
  /// <returns></returns>
  internal bool TryInsert(DataRow row)
  {
    // Find the primary key...
    var primaryKey = row.GetPrimaryKey(_tableDefinition);

    // Check if there is space available to insert the node...
    // Insert the node.
    var slotIndex = FindPrimaryKeySlotIndex(primaryKey);

    // If the slot index is postive, then the key was found and we cannot insert a duplicate...
    if (slotIndex >= 0)
    {
      throw new DuplicateKeyException($"The key '{primaryKey}' already exists");
    }

    var convertedIndex = ~slotIndex;

    SlottedPage.TryAddItem(_page, RecordSerializer.Serialize(_tableDefinition, row), convertedIndex);

    return true;
  }

  /// <summary>
  /// Performs a binary search on the leaf node page for the given search key.
  /// </summary>
  /// <param name="searchKey"></param>
  /// <param name="deserializeKey"></param>
  /// <returns>Positive integer value if exact match is found. Negative integer value representing
  /// the insertion point index if no exact match is found.</returns>
  private int FindSlotIndex(Key searchKey, Func<ReadOnlySpan<byte>, Key> deserializeKey)
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