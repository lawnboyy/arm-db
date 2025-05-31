using System.Buffers;
using System.Collections.Concurrent;
using ArmDb.StorageEngine.Exceptions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArmDb.StorageEngine;

/// <summary>
/// Manages a pool of in-memory page buffers (frames) to cache disk pages,
/// reducing I/O operations and improving performance.
/// It's responsible for fetching pages from disk (via DiskManager),
/// evicting pages when the pool is full, and writing dirty pages back to disk.
/// </summary>
internal sealed class BufferPoolManager : IAsyncDisposable
{
  private readonly DiskManager _diskManager;
  private readonly ILogger<BufferPoolManager> _logger;
  private readonly int _poolSizeInPages;
  private readonly int _pageTableConcurrencyLevel;

  /// <summary>
  /// The array of frames that make up the buffer pool. Each frame holds page data and metadata.
  /// The index in this array acts as the Frame ID.
  /// </summary>
  private readonly Frame[] _frames;

  /// <summary>
  /// Stores the actual byte arrays rented from the ArrayPool for each frame's data.
  /// Needed to return the memory to the pool on dispose.
  /// </summary>
  private readonly byte[][] _rentedBuffers;

  /// <summary>
  /// Maps a PageId (identifying a unique disk page) to the index of the frame
  /// in the _frames array where that page is currently loaded.
  /// This allows for quick O(1) average time lookup to check if a page is in the buffer pool.
  /// </summary>
  private readonly ConcurrentDictionary<PageId, int> _pageTable;

  /// <summary>
  /// A thread-safe queue holding the indices of frames that are currently free and
  /// available for use (e.g., to load a new page from disk).
  /// Initially, all frames are free.
  /// </summary>
  private readonly ConcurrentQueue<int> _freeFrameIndices;

  // --- Page Replacement Policy State ---
  // We'll need data structures to implement our chosen page replacement policy (e.g., LRU, Clock).
  // For an LRU (Least Recently Used) policy, we might have:
  private readonly LinkedList<int> _lruList; // Stores frame indices. LRU at the head, MRU at the tail.
  private readonly Dictionary<int, LinkedListNode<int>> _lruNodeLookup; // Maps frame index to its node in _lruList for O(1) move/remove.
  private readonly object _replacerLock = new object(); // Lock to protect _lruList and _lruNodeLookup as they are not thread-safe.

  /// <summary>
  /// Initializes a new instance of the <see cref="BufferPoolManager"/> class.
  /// </summary>
  /// <param name="optionsAccessor">Accessor for buffer pool configuration options.</param>
  /// <param name="diskManager">The disk manager for disk I/O operations.</param>
  /// <param name="logger">The logger instance.</param>
  /// <exception cref="ArgumentNullException">If optionsAccessor, optionsAccessor.Value, diskManager, or logger is null.</exception>
  /// <exception cref="ArgumentOutOfRangeException">If PoolSizeInPages from options is not positive.</exception>
  internal BufferPoolManager(
      IOptions<BufferPoolManagerOptions> optionsAccessor,
      DiskManager diskManager,
      ILogger<BufferPoolManager> logger)
  {
    ArgumentNullException.ThrowIfNull(optionsAccessor, nameof(optionsAccessor));
    BufferPoolManagerOptions options = optionsAccessor.Value ?? throw new ArgumentNullException(nameof(optionsAccessor.Value), "BufferPoolManagerOptions cannot be null.");
    ArgumentNullException.ThrowIfNull(diskManager, nameof(diskManager));
    ArgumentNullException.ThrowIfNull(logger, nameof(logger));

    if (options.PoolSizeInPages <= 0)
    {
      throw new ArgumentOutOfRangeException(nameof(options.PoolSizeInPages), "Buffer pool size must be positive.");
    }

    if (options.PageTableConcurrencyLevel.HasValue && options.PageTableConcurrencyLevel.Value > 0)
    {
      _pageTableConcurrencyLevel = options.PageTableConcurrencyLevel.Value;
    }
    else
    {
      // Default heuristic: 2x processor count, minimum of 1
      _pageTableConcurrencyLevel = Environment.ProcessorCount * 2;
      if (_pageTableConcurrencyLevel <= 0) _pageTableConcurrencyLevel = 1;
    }

    _poolSizeInPages = options.PoolSizeInPages;
    _diskManager = diskManager;
    _logger = logger;

    _logger.LogInformation(
        "Initializing Buffer Pool Manager with {PoolSize} pages ({MemorySizeMb:F2} MB)...",
        _poolSizeInPages,
        _poolSizeInPages * (long)Page.Size / (1024.0 * 1024.0) // Cast Page.Size to long for multiplication
    );

    // Initialize core data structures
    _frames = new Frame[_poolSizeInPages];
    _rentedBuffers = new byte[_poolSizeInPages][]; // To hold the actual rented arrays
    _pageTable = new ConcurrentDictionary<PageId, int>(_pageTableConcurrencyLevel, _poolSizeInPages); // Concurrency level, initial capacity
    _freeFrameIndices = new ConcurrentQueue<int>();

    // Initialize LRU replacer structures
    _lruList = new LinkedList<int>();
    _lruNodeLookup = new Dictionary<int, LinkedListNode<int>>(_poolSizeInPages);

    // Allocate memory for frames and initialize Frame objects
    for (int i = 0; i < _poolSizeInPages; i++)
    {
      // Rent a buffer from the shared pool. It might be larger than Page.Size.
      _rentedBuffers[i] = ArrayPool<byte>.Shared.Rent(Page.Size);
      // Create a Memory<byte> slice that is exactly Page.Size.
      Memory<byte> frameMemory = new Memory<byte>(_rentedBuffers[i], 0, Page.Size);

      _frames[i] = new Frame(frameMemory); // Frame constructor calls Reset()
      _freeFrameIndices.Enqueue(i);        // Add the frame index to the free list
    }

    _logger.LogInformation("Buffer Pool Manager initialized with {FrameCount} frames.", _frames.Length);
  }

  /// <summary>
  /// Fetches a page. First checks the buffer pool to see if it's cached, and, if so, the page
  /// is pinned and returned. If the page is not found in the buffer pool, it checks for a
  /// free frame. If a free frame is available, the page is loaded into the free frame. Otherwise,
  /// a page must be evicted using the page replacement policy (e.g., LRU). Once a page is evicted,
  /// and a free frame is available, the page is loaded into the free frame. The frame is then pinned
  /// and returned.
  /// </summary>
  /// <param name="pagedId">The Id of the page to return.</param>
  /// <returns>The Page if it's found; null if the page was not found or could not be loaded.</returns>
  // Method within BufferPoolManager class

  internal async Task<Page> FetchPageAsync(PageId pageId)
  {
    // 1. Check Page Table (Cache Hit)
    if (_pageTable.TryGetValue(pageId, out int frameIndex))
    {
      return GetCachedPage(pageId, frameIndex);
    }

    // 2. Cache Miss - Page not in Buffer Pool
    _logger.LogTrace("Page {PageId} not in buffer pool (cache miss). Attempting to find frame.", pageId);

    // 2a. Find an Available Frame (this whole block needs to be atomic regarding frame state changes)
    int? availableFrameIndex;

    // Try to get from free list first
    if (_freeFrameIndices.TryDequeue(out int freeFrameIdx))
    {
      availableFrameIndex = freeFrameIdx;
      _logger.LogTrace("Found free frame {FrameIndex} for page {PageId}.", availableFrameIndex, pageId);
    }
    else
    {
      // No free frames, try to find a victim using replacement policy
      _logger.LogTrace("No free frames. Attempting to find victim frame for page {PageId}.", pageId);
      availableFrameIndex = FindVictimPage(pageId);
    }

    if (availableFrameIndex == null)
    {
      _logger.LogWarning("Buffer pool out of unpinned frames. Cannot fetch page {PageId}.", pageId);
      throw new BufferPoolFullException($"Buffer pool is full. Cannot fetch page {pageId}.");
    }

    frameIndex = availableFrameIndex.Value;
    var targetFrame = _frames[frameIndex];

    // 2b. Evict Old Page from the chosen frame (if it was holding one)
    if (targetFrame.CurrentPageId != default) // default(PageId) means it was free or already reset
    {
      PageId oldPageId = targetFrame.CurrentPageId;
      _logger.LogDebug("Evicting page {OldPageId} from frame {FrameIndex} to make space for {NewPageId}.", oldPageId, frameIndex, pageId);

      if (!_pageTable.TryRemove(oldPageId, out _))
      {
        // This would indicate an inconsistency - page was in frame but not page table. Critical error.
        _logger.LogCritical("CRITICAL: Page {OldPageId} was in frame {FrameIndex} but not found in page table during eviction!", oldPageId, frameIndex);
        // Handle this severe inconsistency, maybe reset the frame and try to continue, or throw.
        // For now, log and proceed to overwrite.
      }

      if (targetFrame.IsDirty)
      {
        _logger.LogInformation("Flushing dirty page {OldPageId} from frame {FrameIndex} before eviction.", oldPageId, frameIndex);
        try
        {
          await _diskManager.WriteDiskPageAsync(oldPageId, targetFrame.PageData);
          targetFrame.IsDirty = false; // Mark as no longer dirty after successful write
        }
        catch (Exception ex)
        {
          _logger.LogError(ex, "Failed to flush dirty page {OldPageId} from frame {FrameIndex} during eviction. Data loss may occur.", oldPageId, frameIndex);
          // This is a serious issue. Options:
          // 1. Re-queue the frame to free list and return null for current fetch (data for old page not saved).
          // 2. Keep the frame "stuck" and try another victim (complex).
          // 3. Throw, indicating failure to fetch new page due to flush failure.
          targetFrame.Reset(); // Reset the frame metadata
          lock (_replacerLock) { _freeFrameIndices.Enqueue(frameIndex); } // Return frame to free list (if safe)
          throw new CouldNotFlushToDiskException("Failed to flush dirty page during eviction. Data loss may occur.", ex);
        }
      }
      targetFrame.Reset(); // Reset metadata for reuse (PinCount should be 0, IsDirty false)
    }

    // 2c. Load New Page into Frame
    targetFrame.CurrentPageId = pageId;
    targetFrame.IsDirty = false;      // Freshly loaded, not dirty
    targetFrame.PinCount = 0;         // Will be incremented after successful load
    // Clear the memory buffer if ArrayPool doesn't guarantee it (it doesn't by default on Return)
    // This ensures no stale data from a previous occupant of the frame.
    Array.Clear(_rentedBuffers[frameIndex], 0, Page.Size);


    _logger.LogDebug("Reading page {PageId} from disk into frame {FrameIndex}.", pageId, frameIndex);
    try
    {
      int bytesRead = await _diskManager.ReadDiskPageAsync(pageId, targetFrame.PageData);
      // ReadPageDiskAsync already throws IOException if bytesRead < Page.Size
      // If it completes without error, we assume a full page was read.
    }
    catch (Exception ex)
    {
      _logger.LogError(ex, "Failed to read page {PageId} from disk into frame {FrameIndex}.", pageId, frameIndex);
      targetFrame.Reset(); // Reset the frame as it's in an invalid state
      lock (_replacerLock) { _freeFrameIndices.Enqueue(frameIndex); } // Return frame to free list
      throw;
    }

    // Set PinCount *after* successful load
    targetFrame.PinCount = 1;

    // 2d. Update Page Table & Replacer for New Page
    if (!_pageTable.TryAdd(pageId, frameIndex))
    {
      // Should not happen if eviction logic and frame finding is correct and serialized.
      // This means another thread somehow loaded this pageId into another frame between our check and add.
      _logger.LogCritical("CRITICAL: Failed to add page {PageId} to page table for frame {FrameIndex}. Race condition or bug in eviction/frame finding.", pageId, frameIndex);
      targetFrame.Reset();
      targetFrame.PinCount = 0; // Unpin as it's not properly registered
      lock (_replacerLock) { _freeFrameIndices.Enqueue(frameIndex); } // Return frame to free list
      throw new InvalidOperationException($"Failed to add page {pageId} to page table for frame {frameIndex}. Indicates a race condition or bug in eviction/frame finding logic.");
    }

    lock (_replacerLock)
    {
      var newNode = _lruList.AddLast(frameIndex);
      _lruNodeLookup[frameIndex] = newNode;
    }

    // 2e. Return the New Page Object
    _logger.LogDebug("Page {PageId} fetched into frame {FrameIndex} and pinned. Pin count: 1", pageId, frameIndex);
    return new Page(targetFrame.CurrentPageId, targetFrame.PageData);
  }

  /// <summary>
  /// Decrements the pin count of a page. If the page was marked as dirty by the caller,
  /// its dirty flag in the frame is set. If the pin count reaches zero, the page becomes
  /// a candidate for eviction.
  /// </summary>
  /// <param name="pageId">The ID of the page to unpin.</param>
  /// <param name="isDirty">True if the page content was modified while pinned; false otherwise.</param>
  /// <returns>A task representing the asynchronous unpin operation.</returns>
  /// <exception cref="InvalidOperationException">
  /// Thrown if the pageId is not found in the buffer pool,
  /// or (later) if an attempt is made to unpin a page whose pin count is already zero.
  /// </exception>
  internal async Task UnpinPageAsync(PageId pageId, bool isDirty)
  {
    _logger.LogTrace("Attempting to unpin page {PageId}. IsDirty: {IsDirtyFlag}", pageId, isDirty);

    // Step 1: Try to find the page in the page table.
    // _pageTable is a ConcurrentDictionary<PageId, int>
    if (!_pageTable.TryGetValue(pageId, out int frameIndex))
    {
      // Page not found in the page table. This is an invalid state for unpinning,
      // as a page must be fetched (and thus in the page table) to be pinned.
      _logger.LogError("Cannot unpin page {PageId} because it was not found in the buffer pool's page table. This likely indicates a bug in the caller or BPM.", pageId);
      throw new InvalidOperationException($"Page {pageId} cannot be unpinned because it could not be found in the buffer pool's page table.");
    }

    // If we reach here, the pageId was found in the _pageTable, and frameIndex is valid.
    _logger.LogTrace("Page {PageId} found in frame {FrameIndex} for unpinning process.", pageId, frameIndex);

    // Step 2: Get the Frame object
    Frame frame = _frames[frameIndex];

    // Step 3: Check current PinCount
    // It is an error to unpin a page that is not currently pinned (i.e., PinCount <= 0).
    // We check frame.PinCount directly here. The atomicity of the decrement will be handled by Interlocked.
    if (frame.PinCount <= 0)
    {
      _logger.LogError("Cannot unpin page {PageId} in frame {FrameIndex}: Pin count is already {PinCountValue} (expected > 0). This may indicate a double unpin or a bug.",
          pageId, frameIndex, frame.PinCount);
      throw new InvalidOperationException($"Page {pageId} in frame {frameIndex} cannot be unpinned. Its pin count is {frame.PinCount} (must be > 0).");
    }

    // Step 4: Atomically decrement the PinCount atomically.
    // Using Interlocked.Decrement ensures that the operation is atomic and thread-safe.
    int newPinCount = Interlocked.Decrement(ref frame.PinCount);

    // Defensive check: Pin count should ideally not go below zero with correct usage.
    // If it does, it's a bug (more unpins than pins).
    if (newPinCount < 0)
    {
      // This state indicates a severe logic error.
      _logger.LogCritical("Pin count for page {PageId} in frame {FrameIndex} dropped below zero to {NewPinCount} after decrement. Attempting to restore to 0. This indicates a critical bug in pin/unpin logic.",
          pageId, frameIndex, newPinCount);
      // Attempt to correct the PinCount to a sane state (0), though the system is likely inconsistent.
      Interlocked.Exchange(ref frame.PinCount, 0);
      // Still throw, because this is a symptom of a larger issue.
      throw new InvalidOperationException($"Pin count for page {pageId} became negative ({newPinCount}), indicating a critical error in pin/unpin logic.");
    }

    // No interlock is necessary for isDirty because boolean writes are atomic, and we only set it to true if the caller
    // is marking the page as dirty.
    if (isDirty)
    {
      // If this unpin operation dirtied the page, mark the frame dirty.
      // If it was already dirty, it remains dirty.
      frame.IsDirty = true;
    }

    _logger.LogDebug("Page {PageId} in frame {FrameIndex} unpinned. New PinCount: {NewPinCount}, Frame IsDirty: {FrameIsDirtyStatus}",
        pageId, frameIndex, newPinCount, frame.IsDirty);

    if (newPinCount == 0)
    {
      _logger.LogInformation("Page {PageId} in frame {FrameIndex} now has PinCount 0 and is a candidate for eviction by replacer.",
          pageId, frameIndex);
      // No explicit action on LRU list here.
      // MoveToMostRecentlyUsed was called when the page was fetched/pinned.
      // Now that PinCount is 0, the LRU replacer's victim selection logic will be able to consider it.
    }

    // Placeholder to make the method async if no other await is present in subsequent steps.
    // This will be removed as more logic (especially any async disk operations if needed for some reason) is added.
    await Task.CompletedTask;
  }

  private Page GetCachedPage(PageId pageId, int frameIndex)
  {
    _logger.LogTrace("Page {PageId} found in frame {FrameIndex} (cache hit).", pageId, frameIndex);
    var frame = _frames[frameIndex];

    // Atomically increment pin count. Using Interlocked is best for simple increments.
    Interlocked.Increment(ref frame.PinCount);

    // Update page replacement policy state to mark this page as most recently used.
    // This needs to be thread-safe.
    lock (_replacerLock)
    {
      // If frame was in LRU list (meaning it was unpinned), remove it.
      // Then add it as most recently used.
      if (_lruNodeLookup.TryGetValue(frameIndex, out var node))
      {
        _lruList.Remove(node);
        _lruNodeLookup.Remove(frameIndex); // Remove old mapping
      }
      // If pin count was 0 and now > 0, it's being actively used, so it's "newly" MRU from replacer's POV
      // For LRU, we always move accessed pages to the MRU end if they become pinned or are already pinned.
      // Or, some LRU variants only update on unpin. For fetch, always mark as MRU.
      var newNode = _lruList.AddLast(frameIndex); // Add to MRU end
      _lruNodeLookup[frameIndex] = newNode;      // Store new node
    }

    _logger.LogDebug("Page {PageId} pinned in frame {FrameIndex}. Pin count: {PinCount}", pageId, frameIndex, frame.PinCount);
    return new Page(frame.CurrentPageId, frame.PageData);
  }

  /// <summary>
  /// Attempts to find a victim page to evict searching for the first least recently used
  /// page that is unpinned. If no unpinned pages are found, it returns null.
  /// </summary>
  /// <param name="pageId"></param>
  /// <returns></returns>
  private int? FindVictimPage(PageId pageId)
  {
    lock (_replacerLock) // Protect LRU list and frame selection
    {
      int? availableFrameIndex = null;
      // Iterate from the Least Recently Used end of the list
      var lruNode = _lruList.First;
      while (lruNode != null)
      {
        int victimFrameIndexCandidate = lruNode.Value;
        if (_frames[victimFrameIndexCandidate].PinCount == 0) // Found an unpinned page
        {
          availableFrameIndex = victimFrameIndexCandidate;
          _lruList.Remove(lruNode); // Remove from LRU list
          _lruNodeLookup.Remove(victimFrameIndexCandidate);
          _logger.LogTrace("Found victim frame {FrameIndex} for page {PageId} using LRU.", availableFrameIndex, pageId);
          break;
        }
        lruNode = lruNode.Next;
      }
      return availableFrameIndex;
    }
  }
  /// <summary>
  /// Asynchronously disposes of resources managed by the BufferPoolManager.
  /// </summary>
  public async ValueTask DisposeAsync()
  {
    _logger.LogInformation("BufferPoolManager disposing...");

    // TODO: STEP 1 - Flush all dirty pages to disk
    // This requires a FlushAllDirtyPagesAsync() method or similar logic.
    // For now, we'll assume this will be implemented.
    // await FlushAllDirtyPagesAsync();

    // STEP 2 - Return rented buffers to the ArrayPool
    if (_rentedBuffers != null)
    {
      for (int i = 0; i < _rentedBuffers.Length; i++)
      {
        if (_rentedBuffers[i] != null)
        {
          ArrayPool<byte>.Shared.Return(_rentedBuffers[i]);
          // Optionally clear the reference: _rentedBuffers[i] = null;
        }
      }
    }

    _logger.LogInformation("BufferPoolManager disposed. Rented memory returned to pool.");
    GC.SuppressFinalize(this);
  }
}