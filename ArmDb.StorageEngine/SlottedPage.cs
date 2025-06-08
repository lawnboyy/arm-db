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

  // --- Other methods (GetFreeSpace, TryAddRecord, GetRecord, etc.) will be added here next ---
}