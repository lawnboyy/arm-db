using ArmDb.DataModel;
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