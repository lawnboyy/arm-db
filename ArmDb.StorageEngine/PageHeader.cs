namespace ArmDb.StorageEngine;

internal static class PageHeader
{
  // All offsets are from the beginning of the page (offset 0)
  public const int LsnOffset = 0;              // 8 bytes for long
  public const int SlotCountOffset = 8;        // 4 bytes for int
  public const int DataStartOffset = 12;       // 4 bytes for int

  // The header size is the offset of the first byte AFTER the header
  public const int HeaderSize = 16;
}