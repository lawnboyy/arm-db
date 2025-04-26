namespace ArmDb.StorageEngine;

/// <summary>
/// Represents an in-memory copy of a fixed-size page from disk.
/// Provides structured access to the underlying byte data.
/// (Implementation details will be added incrementally)
/// </summary>
public sealed class Page
{
  /// <summary>
  /// The underlying memory buffer holding the page's data. Typically an 8KB slice.
  /// </summary>
  private readonly Memory<byte> _memory;

  /// <summary>
  /// The unique identifier of this page within the database instance.
  /// </summary>
  private readonly long _pageId;
}