using System;

namespace ArmDb.StorageEngine;

/// <summary>
/// Represents a single frame within the Buffer Pool. Each frame holds one page of data
/// and its associated metadata, such as its PageId, dirty status, and pin count.
/// This class is intended for internal use by the BufferPoolManager.
/// </summary>
internal class Frame
{
  /// <summary>
  /// The actual 8KB memory buffer for this frame's page data.
  /// This Memory<byte> slice is typically rented from a pool by the BufferPoolManager
  /// and assigned once when the Frame object is constructed.
  /// </summary>
  public Memory<byte> PageData { get; }

  /// <summary>
  /// The PageId of the disk page currently loaded into this frame.
  /// Value is default(PageId) if the frame is free or uninitialized.
  /// </summary>
  public PageId CurrentPageId { get; set; }

  /// <summary>
  /// Gets or sets a value indicating whether the page in this frame has been modified
  /// since being read from disk. If true, it must be written back before eviction.
  /// </summary>
  public bool IsDirty { get; set; }

  /// <summary>
  /// Gets or sets the count of how many active operations are using (have "pinned") this page.
  /// A page cannot be evicted by the replacement policy if PinCount > 0.
  /// Concurrency for PinCount updates must be handled by the BufferPoolManager
  /// (e.g., using Interlocked operations or locks).
  /// </summary>
  public int PinCount;

  // --- Fields/Properties for Page Replacement Algorithm State ---
  // Example for LRU (actual LinkedListNode might be managed by the replacer component itself):
  // public object? ReplacerStateHandle { get; set; }
  // Example for Clock:
  // public bool ReferencedBit { get; set; }

  /// <summary>
  /// Initializes a new Frame with its dedicated memory buffer.
  /// The frame is initially considered free/empty (after Reset is called).
  /// </summary>
  /// <param name="dataBuffer">The 8KB memory slice allocated for this frame.</param>
  /// <exception cref="ArgumentException">Thrown if dataBuffer length does not match Page.Size.</exception>
  public Frame(Memory<byte> dataBuffer)
  {
    if (dataBuffer.Length != Page.Size) // Assuming Page.Size is accessible (e.g., static const on Page class)
    {
      throw new ArgumentException($"Frame data buffer must be Page.Size ({Page.Size} bytes), but was {dataBuffer.Length} bytes.", nameof(dataBuffer));
    }
    PageData = dataBuffer; // This reference is fixed for the lifetime of the Frame in the array.
    Reset(); // Initialize metadata to a clean/free state.
  }

  /// <summary>
  /// Resets the frame's metadata to represent a clean, unpinned, and free state.
  /// Typically called when the frame is initialized or when a page is evicted from it.
  /// </summary>
  public void Reset()
  {
    // default(PageId) will have TableId = 0 and PageIndex = 0.
    // We might need a globally recognized PageId.Invalid static field/property if (0,0) is a valid PageId
    // for a system table. For now, default behavior is usually sufficient if 0 TableId is unused or special.
    CurrentPageId = default;
    IsDirty = false;
    PinCount = 0;
    PageData.Span.Clear(); // CRUCIAL: Zero out buffer from ArrayPool to prevent stale data

    // Reset any replacer-specific state here, e.g.:
    // ReplacerStateHandle = null;
    // ReferencedBit = false; // For Clock algorithm
  }

  // Note on PinCount: While it has a public setter, the BufferPoolManager
  // will be responsible for ensuring that increments and decrements to PinCount
  // are performed in a thread-safe manner (e.g., using Interlocked methods
  // or by acquiring a lock before modifying the Frame's state).
}