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

  /// <summary>
  /// Gets or sets the desired concurrency level for the internal page table
  /// (ConcurrentDictionary). This is a hint to the dictionary about the
  /// estimated number of threads that will be updating it concurrently.
  /// If null or less than or equal to 0, a default based on processor count will be used.
  /// </summary>
  public int? PageTableConcurrencyLevel { get; set; } = null;

  // Add other BPM-specific configurations here later if needed, e.g.:
  // public int MaxDirtyPagesBeforeFlush { get; set; } = 256;
  // public string PageReplacementAlgorithm { get; set; } = "LRU";
}