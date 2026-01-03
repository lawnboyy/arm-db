namespace ArmDb.Storage;

/// <summary>
/// Defines the different types of pages used by the storage engine,
/// allowing the engine to correctly interpret a page's content and structure.
/// </summary>
public enum PageType : byte // Stored as a single byte
{
  /// <summary>
  /// An uninitialized or invalid page type.
  /// </summary>
  Invalid = 0,

  /// <summary>
  /// A B+Tree leaf page, containing actual table row data.
  /// </summary>
  LeafNode = 1,

  /// <summary>
  /// A B+Tree internal (or branch) page, containing keys and pointers to child pages.
  /// </summary>
  InternalNode = 2,

  /// <summary>
  /// The page at index 0 will hold any metadata about the table, such as the page index
  /// of the root of the clustered index / B-Tree.
  /// </summary>
  TableHeader = 3
}