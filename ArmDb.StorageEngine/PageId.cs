namespace ArmDb.StorageEngine;

/// <summary>
/// Represents a unique identifier for a page within the database.
/// Consists of the identifier for the table the page belongs to
/// and the zero-based index of the page within that table's file.
/// This assumes a single file per table.
/// </summary>
public readonly record struct PageId
{
  /// <summary>
  /// Gets the unique identifier of the table this page belongs to.
  /// </summary>
  public int TableId { get; init; }

  /// <summary>
  /// Gets the zero-based index of this page within its table's file.
  /// </summary>
  public int PageIndex { get; init; }

  /// <summary>
  /// Initializes a new instance of the <see cref="PageId"/> struct.
  /// </summary>
  /// <param name="tableId">The table identifier.</param>
  /// <param name="pageIndex">The zero-based page index (must be non-negative).</param>
  /// <exception cref="ArgumentOutOfRangeException">Thrown if pageIndex is negative.</exception>
  public PageId(int tableId, int pageIndex)
  {
    // Validate that page index is not negative
    ArgumentOutOfRangeException.ThrowIfNegative(pageIndex);

    TableId = tableId;
    PageIndex = pageIndex;
  }

  // Implicitly provides Equals, GetHashCode, and ToString based on members
  // Example ToString(): "PageId { TableId = 1, PageIndex = 10 }"
}