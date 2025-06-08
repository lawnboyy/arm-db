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
}