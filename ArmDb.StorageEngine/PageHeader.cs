using System.Buffers.Binary;

namespace ArmDb.StorageEngine;

/// <summary>
/// A readonly ref struct that provides a strongly-typed view and structured access
/// over the header portion of a Page's memory buffer.
/// </summary>
public readonly ref struct PageHeader
{
  /// <summary>
  /// Total size of the page header in bytes.
  /// </summary>
  public const int HEADER_SIZE = 32;

  public const int INVALID_PAGE_INDEX = -1;

  internal const int PAGE_LSN_OFFSET = 0;
  internal const int ITEM_COUNT_OFFSET = 8;
  internal const int DATA_START_OFFSET = 12;
  internal const int PARENT_PAGE_INDEX_OFFSET = 16;
  internal const int TYPE_SPECIFIC_POINTER_1_OFFSET = 20;
  internal const int TYPE_SPECIFIC_POINTER_2_OFFSET = 24;
  internal const int PAGE_TYPE_OFFSET = 28;

  private readonly Span<byte> _headerSpan;

  /// <summary>
  /// Initializes a new PageHeader view over the header section of a given page.
  /// </summary>
  /// <param name="page">The page whose header is to be accessed.</param>
  public PageHeader(Page page)
  {
    _headerSpan = page.Span.Slice(0, HEADER_SIZE);
  }

  /// <summary>
  /// Gets or sets the Log Sequence Number (LSN) for this page.
  /// This is used for tracking changes and ensuring durability.
  /// </summary>
  public long PageLsn
  {
    get => BinaryPrimitives.ReadInt64LittleEndian(_headerSpan.Slice(PAGE_LSN_OFFSET));
    set => BinaryPrimitives.WriteInt64LittleEndian(_headerSpan.Slice(PAGE_LSN_OFFSET), value);
  }

  /// <summary>
  /// Gets or sets the number of items on the page.
  /// For a leaf page, this is the SlotCount. For an internal page, this is the KeyCount.
  /// </summary>
  public int ItemCount
  {
    get => BinaryPrimitives.ReadInt32LittleEndian(_headerSpan.Slice(ITEM_COUNT_OFFSET));
    set => BinaryPrimitives.WriteInt32LittleEndian(_headerSpan.Slice(ITEM_COUNT_OFFSET), value);
  }

  /// <summary>
  /// Gets or sets the byte offset where the data/item heap begins (growing backwards from the end of the page).
  /// </summary>
  public int DataStartOffset
  {
    get => BinaryPrimitives.ReadInt32LittleEndian(_headerSpan.Slice(DATA_START_OFFSET));
    set => BinaryPrimitives.WriteInt32LittleEndian(_headerSpan.Slice(DATA_START_OFFSET), value);
  }

  /// <summary>
  /// Gets or sets the PageIndex of this page's parent in the B+Tree.
  /// Will be INVALID_PAGE_INDEX (-1) for the root page.
  /// </summary>
  public int ParentPageIndex
  {
    get => BinaryPrimitives.ReadInt32LittleEndian(_headerSpan.Slice(PARENT_PAGE_INDEX_OFFSET));
    set => BinaryPrimitives.WriteInt32LittleEndian(_headerSpan.Slice(PARENT_PAGE_INDEX_OFFSET), value);
  }

  /// <summary>
  /// Gets or sets the type of this page.
  /// </summary>
  public PageType PageType
  {
    get => (PageType)_headerSpan[PAGE_TYPE_OFFSET];
    set => _headerSpan[PAGE_TYPE_OFFSET] = (byte)value;
  }

  // --- B+Tree Leaf Page Specific Properties ---

  /// <summary>
  /// Gets or sets the PageIndex of the previous (left) sibling leaf page.
  /// Throws InvalidOperationException if this is not a LeafNode page.
  /// </summary>
  public int PrevPageIndex
  {
    get
    {
      if (PageType != PageType.LeafNode) // Updated enum name
        throw new InvalidOperationException("PrevPageIndex is only valid for LeafNode pages.");
      return BinaryPrimitives.ReadInt32LittleEndian(_headerSpan.Slice(TYPE_SPECIFIC_POINTER_1_OFFSET));
    }
    set
    {
      if (PageType != PageType.LeafNode) // Updated enum name
        throw new InvalidOperationException("PrevPageIndex is only valid for LeafNode pages.");
      BinaryPrimitives.WriteInt32LittleEndian(_headerSpan.Slice(TYPE_SPECIFIC_POINTER_1_OFFSET), value);
    }
  }

  /// <summary>
  /// Gets or sets the PageIndex of the next (right) sibling leaf page.
  /// Throws InvalidOperationException if this is not a LeafNode page.
  /// </summary>
  public int NextPageIndex
  {
    get
    {
      if (PageType != PageType.LeafNode) // Updated enum name
        throw new InvalidOperationException("NextPageIndex is only valid for LeafNode pages.");
      return BinaryPrimitives.ReadInt32LittleEndian(_headerSpan.Slice(TYPE_SPECIFIC_POINTER_2_OFFSET));
    }
    set
    {
      if (PageType != PageType.LeafNode) // Updated enum name
        throw new InvalidOperationException("NextPageIndex is only valid for LeafNode pages.");
      BinaryPrimitives.WriteInt32LittleEndian(_headerSpan.Slice(TYPE_SPECIFIC_POINTER_2_OFFSET), value);
    }
  }

  /// <summary>
  /// Gets or sets the PageIndex of the right-most child pointer in a B+Tree internal node.
  /// Throws InvalidOperationException if this is not an InternalNode page.
  /// </summary>
  public int RightmostChildPageIndex
  {
    get
    {
      if (PageType != PageType.InternalNode) // Updated enum name
        throw new InvalidOperationException("RightmostChildPageIndex is only valid for InternalNode pages.");
      return BinaryPrimitives.ReadInt32LittleEndian(_headerSpan.Slice(TYPE_SPECIFIC_POINTER_1_OFFSET));
    }
    set
    {
      if (PageType != PageType.InternalNode) // Updated enum name
        throw new InvalidOperationException("RightmostChildPageIndex is only valid for InternalNode pages.");
      BinaryPrimitives.WriteInt32LittleEndian(_headerSpan.Slice(TYPE_SPECIFIC_POINTER_1_OFFSET), value);
    }
  }

  /// <summary>
  /// Gets or sets the Root Page's index in the table metadata. Specific to table
  /// header pages.
  /// Throws InvalidOperationException if this is not a TableHeader page.
  /// </summary>
  public int RootPageIndex
  {
    get
    {
      if (PageType != PageType.TableHeader)
        throw new InvalidOperationException("RootPageIndex is only valid for TableHeader pages.");
      return BinaryPrimitives.ReadInt32LittleEndian(_headerSpan.Slice(TYPE_SPECIFIC_POINTER_1_OFFSET));
    }
    set
    {
      if (PageType != PageType.TableHeader)
        throw new InvalidOperationException("RootPageIndex is only valid for TableHeader pages.");
      BinaryPrimitives.WriteInt32LittleEndian(_headerSpan.Slice(TYPE_SPECIFIC_POINTER_1_OFFSET), value);
    }
  }
}