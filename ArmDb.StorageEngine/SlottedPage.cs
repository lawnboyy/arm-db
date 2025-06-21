namespace ArmDb.StorageEngine;

/// <summary>
/// A static helper class providing methods to manage the structure of a Slotted Page.
/// It operates directly on Page objects, interpreting their memory according to the layout:
/// [ Header | Slots -> | Free Space | <- Data Cells ]
/// </summary>
internal static class SlottedPage
{
  /// <summary>
  /// Initializes a new, empty page with a clean Slotted Page header for a specific PageType.
  /// This method should be called on a newly allocated page before any other operations.
  /// </summary>
  /// <param name="page">The Page object to format. Its existing content will be overwritten.</param>
  /// <param name="pageType">The type of page to initialize (e.g., LeafNode, InternalNode).</param>
  /// <param name="parentPageIndex">Optional: The page index of the parent page in the B+Tree. Defaults to -1 (no parent).</param>
  /// <exception cref="ArgumentNullException">Thrown if the page is null.</exception>
  /// <exception cref="ArgumentException">Thrown if an invalid or unsupported PageType is provided.</exception>
  public static void Initialize(Page page, PageType pageType, int parentPageIndex = PageHeader.INVALID_PAGE_INDEX)
  {
    ArgumentNullException.ThrowIfNull(page);

    // Create a header view over the page's memory span.
    // This allows us to use strongly-typed properties to manipulate the header bytes.
    var header = new PageHeader(page);

    // --- Set Common Header Fields ---
    header.PageLsn = 0;                       // Initial Log Sequence Number is 0
    header.ItemCount = 0;                     // Starts with 0 records/slots
    header.DataStartOffset = Page.Size;       // Data heap starts at the very end and grows backward
    header.ParentPageIndex = parentPageIndex;
    header.PageType = pageType;

    // --- Set Type-Specific Header Fields to their "null" state ---
    switch (pageType)
    {
      case PageType.LeafNode:
        header.PrevPageIndex = PageHeader.INVALID_PAGE_INDEX;
        header.NextPageIndex = PageHeader.INVALID_PAGE_INDEX;
        // Note: The memory for RightmostChildPageIndex (which overlaps with PrevPageIndex)
        // is set here, but will be ignored when the page type is LeafNode.
        break;

      case PageType.InternalNode:
        header.RightmostChildPageIndex = PageHeader.INVALID_PAGE_INDEX;
        // Note: The memory for NextPageIndex is unused for InternalNode pages.
        break;

      case PageType.Invalid:
      default:
        throw new ArgumentException($"Cannot initialize a page with an invalid or unsupported page type: {pageType}", nameof(pageType));
    }
  }

  /// <summary>
  /// Calculates the size of the contiguous block of free space between the slot array
  /// and the data cells in the slotted page.
  /// </summary>
  /// <param name="page">The page to check for free space.</param>
  /// <returns>The number of bytes of free space available.</returns>
  /// <exception cref="ArgumentNullException">Thrown if the page is null.</exception>
  public static int GetFreeSpace(Page page)
  {
    ArgumentNullException.ThrowIfNull(page);

    // 1. Create a header view to read the page's metadata
    var header = new PageHeader(page);

    // 2. Calculate the offset immediately after the last slot entry
    int endOfSlotsOffset = PageHeader.HEADER_SIZE + (header.ItemCount * Slot.Size);

    // 3. Get the offset where the data heap begins
    int startOfDataOffset = header.DataStartOffset;

    // 4. The free space is the difference between these two pointers
    int freeSpace = startOfDataOffset - endOfSlotsOffset;

    // 5. In a corrupted page, this calculation could be negative. Return 0 in that case.
    // TODO: Consider throwing an exception if the page is corrupted.
    return Math.Max(0, freeSpace);
  }

  /// <summary>
  /// Attempts to add a new data item to the page at a specific logical index, shifting existing
  /// slots to the right to maintain order.
  /// </summary>
  /// <param name="page">The page to modify.</param>
  /// <param name="itemData">The raw byte data of the item to insert.</param>
  /// <param name="indexToInsertAt">The logical, 0-based index in the slot array where the new item's pointer should be inserted.</param>
  /// <returns>True if the item was successfully added; false if there was not enough free space.</returns>
  /// <exception cref="ArgumentNullException">Thrown if page is null.</exception>
  /// <exception cref="ArgumentException">Thrown if itemData is empty.</exception>
  /// <exception cref="ArgumentOutOfRangeException">Thrown if indexToInsertAt is out of the valid range [0 to current ItemCount].</exception>
  internal static bool TryAddItem(Page page, ReadOnlySpan<byte> itemData, int indexToInsertAt)
  {
    ArgumentNullException.ThrowIfNull(page);
    if (itemData.IsEmpty)
    {
      throw new ArgumentException("Item data to be added cannot be empty.", nameof(itemData));
    }

    var header = new PageHeader(page);
    int currentItemCount = header.ItemCount;

    // The insertion index must be from 0 (start) to currentItemCount (end).
    if ((uint)indexToInsertAt > (uint)currentItemCount)
    {
      throw new ArgumentOutOfRangeException(nameof(indexToInsertAt), $"Insertion index ({indexToInsertAt}) must be within the range [0..{currentItemCount}].");
    }

    // 1. Calculate space needed and check for availability.
    int spaceNeeded = itemData.Length + Slot.Size;
    if (spaceNeeded > GetFreeSpace(page))
    {
      // Not enough contiguous free space for the new data and its slot pointer.
      return false;
    }

    // 2. Write the new item's data into the data heap (growing backward).
    int recordOffset = header.DataStartOffset - itemData.Length;
    page.WriteBytes(recordOffset, itemData);

    // 3. Shift existing slots to the right to make room for the new slot.
    int slotInsertionOffset = PageHeader.HEADER_SIZE + (indexToInsertAt * Slot.Size);
    int slotsToShiftCount = currentItemCount - indexToInsertAt;

    if (slotsToShiftCount > 0)
    {
      int sourceSlotBlockOffset = slotInsertionOffset;
      int destSlotBlockOffset = sourceSlotBlockOffset + Slot.Size;
      int slotBlockLength = slotsToShiftCount * Slot.Size;

      // Get a view of the block of slots that needs to be moved.
      // Using GetReadOnlySpan is safe as it provides a zero-copy view for the source.
      ReadOnlySpan<byte> slotsToMove = page.GetReadOnlySpan(sourceSlotBlockOffset, slotBlockLength);
      // Copy it one slot to the right.
      page.WriteBytes(destSlotBlockOffset, slotsToMove);
    }

    // 4. Write the new slot data into the now-vacant position.
    // Write the pointer to the new data cell...
    page.WriteInt32(slotInsertionOffset, recordOffset);
    // ...and the length of the data.
    page.WriteInt32(slotInsertionOffset + sizeof(int), itemData.Length);

    // 5. Update the page header with the new state.
    header.ItemCount++; // Increment the item count
    header.DataStartOffset = recordOffset;   // Update the data heap pointer

    return true;
  }

  /// <summary>
  /// Retrieves the raw byte data of a record at a specific slot index in the slotted page.
  /// </summary>
  /// <param name="page">The page to read from.</param>
  /// <param name="slotIndex">The slot index to jump to.</param>
  /// <returns></returns>
  internal static ReadOnlySpan<byte> GetRecord(Page page, int slotIndex)
  {
    ArgumentNullException.ThrowIfNull(page, nameof(page));

    var pageHeader = new PageHeader(page);
    int currentItemCount = pageHeader.ItemCount;

    // Casting to uint allows a single comparison to check for both negative and out-of-bounds indices.
    // If slotIndex is negative then the uint cast will yield a large value greater than currentItemCount.
    // Not sure if this performance optimization is really worth it.
    if ((uint)slotIndex >= (uint)currentItemCount)
    {
      // Provide a clear message for an empty page or out-of-bounds index.
      string errorMessage = currentItemCount == 0
          ? "The page is empty."
          : $"Slot index {slotIndex} is out of range. Valid range is [0..{currentItemCount - 1}].";
      throw new ArgumentOutOfRangeException(nameof(slotIndex), errorMessage);
    }

    // Read the slot to get the record offset and length.
    var slot = ReadSlot(page, slotIndex);

    // If the data length is zero, the record has been deleted.
    if (slot.RecordLength == 0)
    {
      return ReadOnlySpan<byte>.Empty;
    }

    // This returns a zero-copy view of the record data. The GetReadOnlySpan method
    // on the Page itself provides another layer of safety against corrupted slot pointers.
    return page.GetReadOnlySpan(slot.RecordOffset, slot.RecordLength);
  }

  /// <summary>
  /// Deletes a record by marking the slot offset invalid by marking the slot and the length to 0.
  /// This makes deleting the record very performant but does not reclaim space. The compaction/vacuum
  /// process will handle that.
  /// </summary>
  /// <param name="page"></param>
  /// <param name="slotIndex"></param>
  internal static void DeleteRecord(Page page, int slotIndex)
  {
    ArgumentNullException.ThrowIfNull(page, nameof(page));

    var pageHeader = new PageHeader(page);
    int currentItemCount = pageHeader.ItemCount;

    // Perform validation on the given slot index to determine if it's in bounds.
    // Casting to uint allows a single comparison to check for both negative and out-of-bounds indices.
    // If slotIndex is negative then the uint cast will yield a large value greater than currentItemCount.
    // Not sure if this performance optimization is really worth it.
    if ((uint)slotIndex >= (uint)currentItemCount)
    {
      // Provide a clear message for an out-of-bounds index.
      throw new ArgumentOutOfRangeException(nameof(slotIndex), $"Slot index {slotIndex} is out of range. Valid range is [0..{currentItemCount - 1}].");
    }

    // If we have a valid slot, zero out the slot offset and length.
    DeleteSlot(page, slotIndex);
  }

  private static void DeleteSlot(Page page, int slotIndex)
  {
    int slotOffset = PageHeader.HEADER_SIZE + (slotIndex * Slot.Size);

    // Zero out the slot's record offset
    page.WriteInt32(slotOffset, 0);

    // Zero out the slot's record length
    page.WriteInt32(slotOffset + sizeof(int), 0);
  }

  private static Slot ReadSlot(Page page, int slotIndex)
  {
    int slotOffset = PageHeader.HEADER_SIZE + (slotIndex * Slot.Size);

    // Get the record offset by reading reading the first 32 bit integer. 
    int recordOffset = page.ReadInt32(slotOffset);

    // Read the next 32 bit integer for the record length.
    int recordLength = page.ReadInt32(slotOffset + sizeof(int));
    return new Slot { RecordOffset = recordOffset, RecordLength = recordLength };
  }
}