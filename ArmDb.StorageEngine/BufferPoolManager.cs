using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
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
    _logger.LogTrace("Fetching page {PageId}.", pageId);

    // Step 1: Attempt to get the page from cache.
    if (TryGetCachedPageAndPin(pageId, out Page? cachedPage))
    {
      // Cache Hit! All necessary actions for hit path are done.
      // Null-forgiving operator used because bool return true guarantees page is not null.
      return cachedPage!;
    }

    // Cache miss: Page not found in the buffer pool.
    _logger.LogTrace("Page {PageId} not in cache. Page must be read and loaded into a free frame...", pageId);

    return await HandleCacheMissAsync(pageId);
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

  /// <summary>
  /// Attempts to retrieve the page from the cache. If found, the page's frame is pinned, 
  /// its position in the LRU replacer is set to most recently used, and the method returns 
  /// true with the page in the out parameter.
  /// </summary>
  /// <param name="pageId">The ID of the page to retrieve.</param>
  /// <param name="page">
  /// When this method returns, contains the cached Page object if found and processed;
  /// otherwise, null.
  /// </param>
  /// <returns>True if the page was found in the cache and processed; false otherwise.</returns>
  private bool TryGetCachedPageAndPin(PageId pageId, [MaybeNullWhen(false)] out Page page)
  {
    _logger.LogTrace("Checking cache for page {PageId}.", pageId);
    // Check if the page is in the page table
    if (_pageTable.TryGetValue(pageId, out int frameIndex))
    {
      _logger.LogTrace("Page {PageId} found in frame {FrameIndex} (cache hit).", pageId, frameIndex);
      Frame frame = _frames[frameIndex];

      // Page is cached:
      // 1. Increment pin count atomically.
      Interlocked.Increment(ref frame.PinCount);

      // 2. Update its position in the page replacement policy (e.g., LRU).
      MoveToMostRecentlyUsed(frameIndex); // This method handles its own locking for LRU structures

      _logger.LogDebug("Page {PageId} (cached) pinned in frame {FrameIndex}. Pin count now: {PinCount}",
                       frame.CurrentPageId, // Should match input pageId
                       frameIndex,
                       frame.PinCount);

      // 3. Create the Page object for the caller.
      page = new Page(frame.CurrentPageId, frame.PageData);
      return true; // Indicate success
    }

    _logger.LogTrace("Page {PageId} not found in cache by TryGetCachedPageAndPin.", pageId);
    page = null; // Explicitly set out parameter to null on failure
    return false; // Indicate failure (cache miss)
  }

  private async Task<Page> HandleCacheMissAsync(PageId pageIdToLoad)
  {
    _logger.LogTrace("HandleCacheMissAsync started for page {PageIdToLoad}.", pageIdToLoad);
    int frameIndex = -1;
    Frame? targetFrame = null;

    // Check if there is a free frame available. If so, set the free frame as the target frame.
    // The _freeFrameIndices queue itself is thread-safe for Enqueue/Dequeue.
    // However, interaction with LRU (_lruNodeLookup, _lruList) requires synchronization.
    lock (_replacerLock)
    {
      if (_freeFrameIndices.TryDequeue(out int dequeuedFrameIndex))
      {
        frameIndex = dequeuedFrameIndex;
        targetFrame = _frames[frameIndex]; // targetFrame is now assigned
        _logger.LogTrace("Found free frame {FrameIndex} for page {PageIdToLoad}.", frameIndex, pageIdToLoad);

        // Safeguard: Ensure this frame is not lingering in LRU tracking.
        // A frame from the free list should ideally already be out of LRU.
        if (_lruNodeLookup.TryGetValue(frameIndex, out LinkedListNode<int>? node))
        {
          _lruList.Remove(node);
          _lruNodeLookup.Remove(frameIndex);
          _logger.LogTrace("Removed frame {FrameIndex} from LRU tracking as it was taken from free list.", frameIndex);
        }
      }
    } // Release _replacerLock

    if (targetFrame != null) // A free frame was successfully dequeued
    {
      // The frame from the free list is considered "empty".
      // It should have been Reset() before being added to the free list.
      // We'll Reset() again for safety and clear the buffer.
      _logger.LogTrace("Preparing free frame {FrameIndex} for page {PageIdToLoad}.", frameIndex, pageIdToLoad);

      targetFrame.Reset(); // Ensures PinCount=0, IsDirty=false, CurrentPageId=default
      targetFrame.PageData.Span.Clear(); // CRUCIAL: Zero out buffer from ArrayPool to prevent stale data
    }
    else // No free frame was found so we must select a victim frame and evict it.
    {
      _logger.LogTrace("No free frame found for page {PageIdToLoad}. Victim selection logic is next.", pageIdToLoad);

      int victimFrameIndex;
      lock (_replacerLock) // Acquire lock to select a victim atomically from LRU
      {
        victimFrameIndex = FindVictimFrame(); // This will throw BufferPoolFullException if no unpinned victim is found
      } // _replacerLock released. victimFrameIndex is now "reserved" for us.

      _logger.LogTrace("HandleCacheMissAsync: Victim frame {VictimFrameIndex} selected for page {PageIdToLoad}.", victimFrameIndex, pageIdToLoad);
      frameIndex = victimFrameIndex;    // Use this frame index
      targetFrame = _frames[frameIndex]; // Get the frame

      // Now, evict the content of this victim frame (flushes if dirty, resets frame, clears buffer)
      // This is an async operation and happens *after* _replacerLock for victim selection is released.
      try
      {
        await EvictPageFromFrameAsync(frameIndex); // Throws PageFlushException on failure
      }
      catch (CouldNotFlushToDiskException pex)
      {
        _logger.LogCritical(pex, "HandleCacheMissAsync: Failed to flush dirty page from victim frame {FrameIndex} for page {PageIdToLoad}. Aborting page fetch.", frameIndex, pageIdToLoad);
        // If flush failed, this frame is problematic and has been reset by EvictPageFromFrameAsync.
        // The original caller (FetchPageAsync) will catch this PageFlushException.
        throw;
      }
      // --- End of New Eviction Logic ---
    }

    // --- Common logic for a successfully prepared frame (either free or victim) ---
    // At this point, 'targetFrame' and 'frameIndex' point to a frame that is:
    // 1. Reserved for our use (removed from free list OR removed from LRU by victim selection).
    // 2. Its previous content (if any) has been handled (flushed if dirty, page table updated).
    // 3. The frame itself has been Reset() and its PageData.Span.Clear()'d by EvictPageFromFrameAsync.

    _logger.LogTrace("HandleCacheMissAsync: Frame {FrameIndex} is prepared. Loading page {PageIdToLoad} from disk.", frameIndex, pageIdToLoad);

    targetFrame.CurrentPageId = pageIdToLoad;
    // targetFrame.IsDirty = false; // Done by Reset() in EvictPageFromFrameAsync
    // targetFrame.PinCount = 0;    // Done by Reset() in EvictPageFromFrameAsync

    try
    {
      await _diskManager.ReadDiskPageAsync(pageIdToLoad, targetFrame.PageData);
    }
    catch (Exception ex)
    {
      _logger.LogError(ex, "HandleCacheMissAsync: Failed to read page {PageIdToLoad} from disk into frame {FrameIndex}.", pageIdToLoad, frameIndex);
      CleanupFailedFrameLoadAndFree(frameIndex, targetFrame); // Helper to reset & free frame
      throw new CouldNotLoadPageFromDiskException($"Failed to read page {pageIdToLoad} from disk.", ex);
    }

    if (!_pageTable.TryAdd(pageIdToLoad, frameIndex))
    {
      _logger.LogCritical("HandleCacheMissAsync: CRITICAL RACE: Page {PageIdToLoad} added to page table by another thread for frame {FrameIndex} while this thread was loading it.", pageIdToLoad, frameIndex);
      CleanupFailedFrameLoadAndFree(frameIndex, targetFrame);
      throw new InvalidOperationException($"Race condition: Failed to add page {pageIdToLoad} to page table for frame {frameIndex}.");
    }
    _logger.LogTrace("HandleCacheMissAsync: Page {PageIdToLoad} added to page table for frame {FrameIndex}.", pageIdToLoad, frameIndex);

    Interlocked.Increment(ref targetFrame.PinCount); // PinCount becomes 1
    MoveToMostRecentlyUsed(frameIndex); // This locks _replacerLock internally

    _logger.LogDebug("HandleCacheMissAsync: Page {PageIdToLoad} loaded into frame {FrameIndex} and pinned. Pin count: {PinCount}",
                     targetFrame.CurrentPageId, frameIndex, targetFrame.PinCount);
    return new Page(targetFrame.CurrentPageId, targetFrame.PageData);
  }

  /// <summary>
  /// Finds an unpinned frame to evict based on the LRU policy and removes it
  /// from LRU tracking.
  /// This method MUST be called while holding the _replacerLock.
  /// </summary>
  /// <returns>The index of the victim frame.</returns>
  /// <exception cref="BufferPoolFullException">Thrown if no unpinned victim frame can be found (e.g., all pages are pinned or LRU list is empty).</exception>
  private int FindVictimFrame()
  {
    // This method assumes _replacerLock is already held by the caller.
    _logger.LogTrace("FindVictimFrame: Searching for LRU victim (replacer lock held).");

    LinkedListNode<int>? currentNode = _lruList.First; // LRU victim is at the head

    while (currentNode != null)
    {
      int candidateFrameIndex = currentNode.Value;
      Frame candidateFrame = _frames[candidateFrameIndex];

      // Check PinCount. We must not evict a pinned page.
      if (candidateFrame.PinCount == 0)
      {
        // Found an unpinned victim.
        _lruList.Remove(currentNode);           // Remove from the LRU list
        _lruNodeLookup.Remove(candidateFrameIndex); // Remove from the lookup map

        _logger.LogDebug("FindVictimFrame: LRU victim selected: Frame {FrameIndex}, previously holding PageId {PageId}.",
                         candidateFrameIndex,
                         candidateFrame.CurrentPageId == default ? "None/Empty" : candidateFrame.CurrentPageId.ToString());
        return candidateFrameIndex; // Return the frame index directly
      }
      else
      {
        _logger.LogTrace("FindVictimFrame: LRU candidate Frame {FrameIndex} (PageId {PageId}) is pinned (PinCount: {PinCount}). Skipping.",
                         candidateFrameIndex,
                         candidateFrame.CurrentPageId == default ? "None/Empty" : candidateFrame.CurrentPageId.ToString(),
                         candidateFrame.PinCount);
      }
      currentNode = currentNode.Next; // Move to the next candidate
    }

    // If the loop completes, no unpinned page was found in the LRU list.
    _logger.LogWarning("FindVictimFrame: No unpinned victim found (all pages in LRU list are pinned, or list is empty).");
    throw new BufferPoolFullException("No unpinned victim frame could be found in the buffer pool. All pages may be pinned or the LRU list is empty.");
  }

  private async Task EvictPageFromFrameAsync(int frameIndexToEvict)
  {
    Frame frame = _frames[frameIndexToEvict];
    _logger.LogTrace("Processing frame {FrameIndexToEvict} for eviction. Current PageId: {PageId}, IsDirty: {IsDirty}",
                     frameIndexToEvict,
                     frame.CurrentPageId == default ? "None" : frame.CurrentPageId.ToString(),
                     frame.IsDirty);

    if (frame.CurrentPageId != default) // Frame holds a valid page
    {
      PageId oldPageId = frame.CurrentPageId;

      if (!_pageTable.TryRemove(oldPageId, out _))
      {
        _logger.LogWarning("During eviction of frame {FrameIndexToEvict}, its PageId {OldPageId} was not found in the page table. Possible inconsistency.", frameIndexToEvict, oldPageId);
      }

      if (frame.IsDirty)
      {
        _logger.LogInformation("Flushing dirty page {OldPageId} from frame {FrameIndexToEvict}.", oldPageId, frameIndexToEvict);
        try
        {
          await _diskManager.WriteDiskPageAsync(oldPageId, frame.PageData);
        }
        catch (Exception ex)
        {
          _logger.LogCritical(ex, "CRITICAL: Failed to flush dirty page {OldPageId} from frame {FrameIndexToEvict}. Data for may be lost.", oldPageId, frameIndexToEvict);
          // Propagate a specific exception so HandleCacheMissAsync knows the flush failed.
          throw new CouldNotFlushToDiskException($"Failed to flush dirty page {oldPageId} from frame {frameIndexToEvict}. Data may be lost.", ex);
        }
      }
    }

    // Reset frame metadata and clear buffer regardless of whether it held a page
    frame.Reset(); // Sets CurrentPageId = default, IsDirty = false, PinCount = 0
    frame.PageData.Span.Clear(); // Zero out the memory buffer

    _logger.LogDebug("Frame {FrameIndexToEvict} successfully processed for eviction and is now clean and ready for reuse.", frameIndexToEvict);
  }

  private void CleanupFailedFrameLoadAndFree(int frameIndex, Frame frame)
  {
    _logger.LogWarning("Cleaning up frame {FrameIndex} after failed page load for intended PageId {PageId}.", frameIndex, frame.CurrentPageId); // frame.CurrentPageId was set to the new pageId
    if (frame.CurrentPageId != default(PageId))
    {
      _pageTable.TryRemove(frame.CurrentPageId, out _); // Remove if it was added by mistake or for the new page
    }
    frame.Reset(); // Reset all metadata including CurrentPageId, PinCount, IsDirty
    // Return frame to free list (needs lock for _freeFrameIndices AND LRU data)
    lock (_replacerLock)
    {
      if (_lruNodeLookup.TryGetValue(frameIndex, out var node)) // Ensure not in LRU if it was added
      {
        _lruList.Remove(node);
        _lruNodeLookup.Remove(frameIndex);
      }
      _freeFrameIndices.Enqueue(frameIndex);
    }
  }

  private void MoveToMostRecentlyUsed(int frameIndex)
  {
    lock (_replacerLock)
    {
      if (_lruNodeLookup.TryGetValue(frameIndex, out LinkedListNode<int>? node))
      {
        _lruList.Remove(node);
      }
      // Add the frame index to the end of the LRU list (MRU)
      var lastNode = _lruList.AddLast(frameIndex);
      _lruNodeLookup[frameIndex] = lastNode;
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