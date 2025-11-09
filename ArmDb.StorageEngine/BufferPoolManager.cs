using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using ArmDb.StorageEngine.Exceptions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualBasic;

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
  /// A thread-safe dictionary to manage locks on a per page basis. These locks handle multiple
  /// concurrent attempts to load the same page from disk.
  /// </summary>
  private readonly ConcurrentDictionary<PageId, SemaphoreSlim> _pageLoadingLocks = new();

  /// <summary>
  /// A thread-safe queue holding the indices of frames that are currently free and
  /// available for use (e.g., to load a new page from disk).
  /// Initially, all frames are free.
  /// </summary>
  private readonly ConcurrentQueue<int> _freeFrameIndices;

  /// <summary>
  /// Data structures to implement the page replacement policy, which for now is Least Recently Used (LRU). When
  /// the buffer is full and we need to evict an old page to pull in a new page, we'll remove the least recently
  /// used frame. There are probably better algorithms that we can explore later, but this will get us started.
  /// </summary>
  private readonly LinkedList<int> _lruList; // Stores frame indices. LRU at the head, MRU at the tail.
  private readonly Dictionary<int, LinkedListNode<int>> _lruNodeLookup; // Maps frame index to its node in _lruList for O(1) move/remove.

  /// <summary>
  /// Lock to protect frame access, including any related state such as pin count and the position in the page replacement
  /// policy (LRU).
  /// </summary>
  private readonly object _replacerLock = new object();

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
      // The processor count is the logical processor count on the machine.
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

  internal async Task<Page> CreatePageAsync(int tableId)
  {
    // Allocate a new page on disk. This will create a new table file if it doesn't exist
    // already.
    // TODO: Do we need to protect access to the table in this case?
    var pageId = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Frame to load the page into...
    Frame frame;
    int frameIndex;
    PageId? evictedPageId = null;
    byte[] evictedDirtyPageData = [];

    // Case 1: There are free frames available.
    // Find a frame to load the new page into.
    if (_freeFrameIndices.TryDequeue(out frameIndex))
    {
      // Protect access to the free frame's state...
      lock (_replacerLock)
      {
        // We found a free frame, so      
        // Pull the frame...
        frame = _frames[frameIndex];
        ReserveFrame(frame, pageId, frameIndex, true);
      }

      // Return the page.
      return new Page(pageId, frame.PageData);
    }
    else // There are no free frames available, so we must evict a cached page.
    {
      // Case 2: There are no free frames available, so we must evict a page, and there is an unpinned page to evict.
      // Protect access to this section as we select a frame to evict and evict it.
      lock (_replacerLock)
      {
        // TODO: Should we catch the buffer pool full exception here?
        // Evict a frame
        frame = EvictFrame(pageId, out frameIndex, ref evictedPageId, ref evictedDirtyPageData);
        ReserveFrame(frame, pageId, frameIndex, true);
      }

      // If we evicted a dirty page, now we need to flush its captured content to disk outside the lock.
      if (evictedPageId != null && evictedDirtyPageData.Length > 0)
      {
        _logger.LogInformation("Flushing dirty page {OldPageId} from frame {FrameIndexToEvict}.", evictedPageId, frameIndex);
        try
        {
          await _diskManager.WriteDiskPageAsync(evictedPageId.Value, evictedDirtyPageData);
        }
        catch (Exception ex)
        {
          _logger.LogCritical(ex, "CRITICAL: Failed to flush dirty page {OldPageId} from frame {FrameIndexToEvict}. Data for may be lost.", evictedPageId, frameIndex);
          // Propagate a specific exception so HandleCacheMissAsync knows the flush failed.
          throw new CouldNotFlushToDiskException($"Failed to flush dirty page {evictedPageId} from frame {frameIndex}. Data may be lost.", ex);
        }
      }

      // Return the page.
      return new Page(pageId, frame.PageData);
    }

    throw new InvalidOperationException("Unable to create new page!");
  }

  /// <summary>
  /// Fetches a page. First checks the buffer pool to see if it's cached, and, if so, the page
  /// is pinned and returned. If the page is not found in the buffer pool, it checks for a
  /// free frame. If a free frame is available, the page is loaded into the free frame. Otherwise,
  /// a page must be evicted using the page replacement policy (e.g., LRU). Once a page is evicted,
  /// and a free frame is available, the page is loaded into the free frame. The frame is then pinned
  /// and returned. If an evicted frame is dirty, it must be flushed to disk.
  /// </summary>
  /// <remarks>
  /// The Buffer Pool Manager must be thread safe and protect access to pages. We use a semaphore to
  /// control access to any one page ID. This ensures we don't allow multiple concurrent reads or
  /// writes. If a thread pulls the page into the buffer pool, any other threads accessing that
  /// page should be guaranteed to find it in the cache.
  /// 
  /// We must also protect access to a frame's state which includes pin count, it's position in the
  /// page replacement policy data structures (LRU list and lookup), dirty flag, and page information.
  /// This logic uses a lock object to protect access to critical sections where the frame is accessed.
  /// </remarks
  /// <param name="pagedId">The Id of the page to return.</param>
  /// <returns>The Page if it's found; null if the page was not found or could not be loaded.</returns>
  internal async Task<Page> FetchPageAsync(PageId pageId)
  {
    // Capture the thread ID for debugging.
    int threadId = Thread.CurrentThread.ManagedThreadId;
    _logger.LogTrace($"Thread: {threadId} FetchPageAsync: Fetching page {pageId}.");

    // First check to see if the page is already in the cache; if so, we can simply return it.
    if (_pageTable.TryGetValue(pageId, out int frameIndex))
    {
      // Case 1: The page is cached, so pull it and return it.
      // The page is now in the cache, so we can just increment the pin, update the position in the LRU, and return it.
      lock (_replacerLock)
      {
        // Pull the frame...
        Frame frame = _frames[frameIndex];
        // Increment the cached page's pin count...
        frame.PinCount++;
        // Update the LRU list...
        MoveToMostRecentlyUsed(frameIndex);
        // Return the page.
        return new Page(frame.CurrentPageId, frame.PageData);
      }
    }
    else
    {
      // Case 2: Cache miss; load the page from disk.
      _logger.LogTrace($"Thread {threadId} FetchPageAsync: Page {pageId} not in cache. Page must be read and loaded into a free frame...");

      // Lock access to the page to avoid multiple threads attempting to load the same page from disk.
      SemaphoreSlim? pageLoadLock = _pageLoadingLocks.GetOrAdd(pageId, _ => new SemaphoreSlim(1, 1));

      // Asynchronously wait to acquire the lock to enter the critical section for this page
      _logger.LogTrace("[Thread:{ThreadId}] FetchPageAsync: Cache miss for PageId {PageId}. Attempting to acquire loading lock.", threadId, pageId);
      await pageLoadLock.WaitAsync();
      _logger.LogTrace("[Thread:{ThreadId}] FetchPageAsync: Loading lock ACQUIRED for PageId {PageId}.", threadId, pageId);

      try
      {
        // Variables to track if we have an evicted dirty page.
        // If we evict a frame that contains a dirty page, we'll need to flush it to disk outside a lock because it's asynchronous.
        PageId? evictedPageId = null;
        byte[] evictedDirtyPageData = [];

        // If the lock was previously held, then the page may have been loaded from disk, in which case we must check the cache again...
        if (_pageTable.TryGetValue(pageId, out frameIndex))
        {
          // The page is now in the cache, so we can just increment the pin, update the position in the LRU, and return it.
          lock (_replacerLock)
          {
            Frame frame = _frames[frameIndex];
            // Increment the cached page's pin count...
            frame.PinCount++;
            MoveToMostRecentlyUsed(frameIndex);

            return new Page(frame.CurrentPageId, frame.PageData);
          }
        }
        else
        {
          Frame? targetFrame = null;
          // The page is still not found, so we must load it from disk.
          // Lock access to critical section where we find a frame to load the page into and prep that frame for the new page.
          lock (_replacerLock)
          {
            // First find an available frame either from the free frame queue or if there are no free frames, we evict
            // the least recently used...
            // TODO: We don't really need to use a concurrent queue for free frame indices since it has to be locked with other operations...
            if (_freeFrameIndices.TryDequeue(out frameIndex))
            {
              // Once the frame is found, update the pin count...
              targetFrame = _frames[frameIndex];
              // ...and the LRU list.
              MoveToMostRecentlyUsed(frameIndex);
              // Now we can release the lock because we have acquired a frame and pinned it.
            }
            else // No free frames available, so we must evict...
            {
              // Find a frame to evict...
              targetFrame = EvictFrame(pageId, out frameIndex, ref evictedPageId, ref evictedDirtyPageData);
            }

            targetFrame.Reset(); // Ensures PinCount=0, IsDirty=false, CurrentPageId=default
            targetFrame.CurrentPageId = pageId; // Load the page from disk into the frame.
            targetFrame.PinCount++;
          } // Release the page lock.

          // If we evicted a dirty page, now we need to flush its captured content to disk outside the lock.
          if (evictedPageId != null && evictedDirtyPageData.Length > 0)
          {
            _logger.LogInformation("Flushing dirty page {OldPageId} from frame {FrameIndexToEvict}.", evictedPageId, frameIndex);
            try
            {
              await _diskManager.WriteDiskPageAsync(evictedPageId.Value, evictedDirtyPageData);
            }
            catch (Exception ex)
            {
              _logger.LogCritical(ex, "CRITICAL: Failed to flush dirty page {OldPageId} from frame {FrameIndexToEvict}. Data for may be lost.", evictedPageId, frameIndex);
              // Propagate a specific exception so HandleCacheMissAsync knows the flush failed.
              throw new CouldNotFlushToDiskException($"Failed to flush dirty page {evictedPageId} from frame {frameIndex}. Data may be lost.", ex);
            }
          }

          // Finally, read the new page from disk and return it.
          try
          {
            await _diskManager.ReadDiskPageAsync(pageId, targetFrame.PageData);
          }
          catch (Exception ex)
          {
            _logger.LogError(ex, "HandleCacheMissAsync: Failed to read page {PageIdToLoad} from disk into frame {FrameIndex}.", pageId, frameIndex);
            CleanupFailedFrameLoadAndFree(frameIndex, targetFrame); // Helper to reset & free frame
            throw new CouldNotLoadPageFromDiskException($"Failed to read page {pageId} from disk.", ex);
          }
          // Update the page table lookup.
          _pageTable.TryAdd(pageId, frameIndex);
          // Return the new page.
          return new Page(targetFrame.CurrentPageId, targetFrame.PageData);
        }
      }
      finally
      {
        pageLoadLock.Release();
      }
    }
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

    lock (_replacerLock)
    {
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
        frame.PinCount = 0;         // Still throw, because this is a symptom of a larger issue.
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
    }

    // Placeholder to make the method async if no other await is present in subsequent steps.
    // This will be removed as more logic (especially any async disk operations if needed for some reason) is added.
    await Task.CompletedTask;
  }

  /// <summary>
  /// Finds and evicts a page from the LRU frame. This method must be called inside a lock or protected with
  /// a semaphore as it accesses and updates the frame's state.
  /// </summary>
  /// <param name="pageId"></param>
  /// <param name="frameIndex"></param>
  /// <param name="evictedPageId"></param>
  /// <param name="evictedDirtyPageData"></param>
  /// <returns></returns>
  /// <exception cref="BufferPoolFullException"></exception>
  private Frame EvictFrame(PageId pageId, out int frameIndex, ref PageId? evictedPageId, ref byte[] evictedDirtyPageData)
  {
    // Find a frame to evict...
    frameIndex = FindVictimFrame();

    if (frameIndex < 0)
    {
      // If we didn't get a valid index, then no frame was unpinned, so we could not evict.
      _logger.LogWarning("FindVictimFrame: No unpinned victim found (all pages in LRU list are pinned, or list is empty).");
      throw new BufferPoolFullException("No unpinned victim frame could be found in the buffer pool. All pages may be pinned or the LRU list is empty.");
    }

    // Add right back to the LRU list?
    MoveToMostRecentlyUsed(frameIndex);

    _logger.LogTrace("Victim frame {VictimFrameIndex} selected for page {PageIdToLoad}.", frameIndex, pageId);
    var targetFrame = _frames[frameIndex];

    _logger.LogTrace("Processing frame {FrameIndexToEvict} for eviction. Current PageId: {PageId}, IsDirty: {IsDirty}",
                     frameIndex,
                     targetFrame.CurrentPageId == default ? "None" : targetFrame.CurrentPageId.ToString(),
                     targetFrame.IsDirty);

    // Remove the page from the cache look up...
    if (!_pageTable.TryRemove(targetFrame.CurrentPageId, out _))
    {
      _logger.LogWarning("During eviction of frame {FrameIndexToEvict}, its PageId {OldPageId} was not found in the page table. Possible inconsistency.", frameIndex, evictedPageId);
    }

    // Capture the information about the evicted frame if the frame is dirty and needs to be flushed to disk.
    if (targetFrame.IsDirty)
    {
      evictedPageId = targetFrame.CurrentPageId;
      // Copy the data to flush so that we can perform this asynchronous operation outside of the lock.
      evictedDirtyPageData = targetFrame.PageData.ToArray();
    }

    _logger.LogDebug("Frame {FrameIndexToEvict} successfully processed for eviction and is now clean and ready for reuse.", frameIndex);

    return targetFrame;
  }

  /// <summary>
  /// Reserves a frame by resetting it and setting it's state to the new page ID. This
  /// method must be called in a lock or with a semaphore to protect access to to the
  /// frame's state.
  /// </summary>
  /// <param name="frame"></param>
  /// <param name="pageId"></param>
  /// <param name="frameIndex"></param>
  /// <param name="isDirty"></param>
  private void ReserveFrame(Frame frame, PageId pageId, int frameIndex, bool? isDirty = null)
  {
    frame.Reset(); // Ensures PinCount=0, IsDirty=false, CurrentPageId=default, data is cleared...
    frame.CurrentPageId = pageId; // Set the new page ID
    frame.PinCount++;
    frame.IsDirty = isDirty.HasValue ? isDirty.Value : false;
    _pageTable.TryAdd(pageId, frameIndex);
    // Update the LRU list...
    MoveToMostRecentlyUsed(frameIndex);
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
        // We don't remove from the LRU because we are evicting the page frame so that frame can
        // be used by another page. So we expect the caller to always update this evicted frame
        // to be the MRU.
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
    return -1;
  }

  private void CleanupFailedFrameLoadAndFree(int frameIndex, Frame frame)
  {
    lock (_replacerLock)
    {
      _logger.LogWarning("Cleaning up frame {FrameIndex} after failed page load for intended PageId {PageId}.", frameIndex, frame.CurrentPageId); // frame.CurrentPageId was set to the new pageId
      if (frame.CurrentPageId != default(PageId))
      {
        _pageTable.TryRemove(frame.CurrentPageId, out _); // Remove if it was added by mistake or for the new page
      }
      frame.Reset(); // Reset all metadata including CurrentPageId, PinCount, IsDirty
                     // Return frame to free list (needs lock for _freeFrameIndices AND LRU data)

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
    if (_lruNodeLookup.TryGetValue(frameIndex, out LinkedListNode<int>? node))
    {
      _lruList.Remove(node);
    }
    // Add the frame index to the end of the LRU list (MRU)
    var lastNode = _lruList.AddLast(frameIndex);
    _lruNodeLookup[frameIndex] = lastNode;
  }

  /// <summary>
  /// Flushes a single dirty page to disk if it exists in the buffer pool.
  /// </summary>
  /// <param name="pageId">The ID of the page to flush.</param>
  /// <returns>True if the page was found, was dirty, and successfully flushed; false otherwise.</returns>
  internal async Task<bool> FlushPageAsync(PageId pageId)
  {
    // Check if the page is resident in the page table
    if (_pageTable.TryGetValue(pageId, out int frameIndex))
    {
      var frame = _frames[frameIndex];
      // No lock is taken here for IsDirty check, assuming that concurrent writes to IsDirty are acceptable
      // and that a flush call is a coordination point. A more complex system might latch the page here.
      if (frame.IsDirty)
      {
        _logger.LogTrace("Flushing dirty page {PageId} from frame {FrameIndex} on demand.", pageId, frameIndex);
        try
        {
          await _diskManager.WriteDiskPageAsync(pageId, frame.PageData);
          frame.IsDirty = false; // Mark clean after successful write
          return true;
        }
        catch (Exception ex)
        {
          _logger.LogError(ex, "Failed to flush dirty page {PageId} from frame {FrameIndex}.", pageId, frameIndex);
          // Do not mark clean if write failed
          return false;
        }
      }
    }
    return false; // Page not found or not dirty
  }

  /// <summary>
  /// Flushes all dirty pages currently in the buffer pool to disk.
  /// </summary>
  /// <returns>A task representing the asynchronous flush operation.</returns>
  internal async Task FlushAllDirtyPagesAsync()
  {
    _logger.LogInformation("Flushing all dirty pages to disk...");
    int flushedCount = 0;
    // Take a snapshot of the keys to avoid issues with collection modification during iteration.
    // This is safer in a highly concurrent environment.
    // This is not a perfect solution if pages are added/removed while flushing; for a more
    // robust solution, we need to implement page latching.
    // TODO: Implement page latching to handle concurrent writes.
    var pageIdsInPool = _pageTable.Keys.ToList();

    foreach (var pageId in pageIdsInPool)
    {
      if (await FlushPageAsync(pageId))
      {
        flushedCount++;
      }
    }
    _logger.LogInformation("Flush all pages complete. {Count} pages flushed to disk.", flushedCount);
  }

  /// <summary>
  /// Asynchronously disposes of resources managed by the BufferPoolManager,
  /// ensuring all dirty pages are flushed to disk and rented memory is returned.
  /// </summary>
  public async ValueTask DisposeAsync()
  {
    _logger.LogInformation("Disposing BufferPoolManager...");

    // Step 1: Ensure all modified data in the buffer pool is persisted.
    await FlushAllDirtyPagesAsync();

    // Step 2: Return all rented buffers back to the shared ArrayPool.
    if (_rentedBuffers != null)
    {
      for (int i = 0; i < _rentedBuffers.Length; i++)
      {
        if (_rentedBuffers[i] != null)
        {
          ArrayPool<byte>.Shared.Return(_rentedBuffers[i]);
        }
      }
    }

    _logger.LogInformation("BufferPoolManager disposed successfully.");
    // No need to call GC.SuppressFinalize here because ValueTask-returning DisposeAsync
    // handles this implicitly when there is no finalizer.
  }

#if DEBUG
  /// <summary>
  /// [TESTING ONLY] Gets the Frame object associated with a PageId if it's currently
  /// resident in the buffer pool.
  /// This method is intended for use in unit/integration tests to inspect internal BPM state
  /// like PinCount or IsDirty status of a specific frame.
  /// It should NOT be used in production code.
  /// </summary>
  /// <param name="pageId">The PageId to look for.</param>
  /// <returns>The Frame object if the page is found in the buffer pool; otherwise, null.</returns>
  internal Frame? GetFrameByPageId_TestOnly(PageId pageId)
  {
    // _pageTable is ConcurrentDictionary<PageId, int>
    // _frames is Frame[]
    if (_pageTable.TryGetValue(pageId, out int frameIndex))
    {
      // Basic sanity check, though TryGetValue should give a valid index if true
      if (frameIndex >= 0 && frameIndex < _frames.Length)
      {
        return _frames[frameIndex];
      }
      else
      {
        // This indicates a severe inconsistency between _pageTable and _frames
        _logger?.LogCritical("GetFrameByPageId_TestOnly: PageTable returned an invalid frameIndex {FrameIndex} for PageId {PageId}. Frames array size: {FramesLength}.",
                            frameIndex, pageId, _frames.Length);
        return null;
      }
    }
    return null; // PageId not found in the page table
  }
#endif
}