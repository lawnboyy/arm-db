namespace ArmDb.StorageEngine;

/// <summary>
/// Configuration options for the BufferPoolManager.
/// </summary>
public class BufferPoolManagerOptions
{
  /// <summary>
  /// Gets or sets the total number of page frames in the buffer pool.
  /// Each frame can hold one page (e.g., 8KB).
  /// </summary>
  public int PoolSizeInPages { get; set; } = 1024; // Default to 1024 pages (e.g., 8MB if pages are 8KB)

  // Add other BPM-specific configurations here later if needed, e.g.:
  // public int MaxDirtyPagesBeforeFlush { get; set; } = 256;
  // public string PageReplacementAlgorithm { get; set; } = "LRU";
}