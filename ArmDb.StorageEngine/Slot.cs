namespace ArmDb.Storage;

/// <summary>
/// Represents a slot entry in the page's slot array.
/// Points to a specific record's location and size within the page.
/// </summary>
internal readonly record struct Slot
{
  /// <summary>
  /// The size of a slot entry in bytes (4-byte int for offset + 4-byte int for length).
  /// </summary>
  public const int Size = sizeof(int) + sizeof(int); // 8 bytes

  /// <summary>
  /// The starting byte offset of the record within the page.
  /// </summary>
  public int RecordOffset { get; init; }

  /// <summary>
  /// The total length of the record in bytes.
  /// </summary>
  public int RecordLength { get; init; }
}