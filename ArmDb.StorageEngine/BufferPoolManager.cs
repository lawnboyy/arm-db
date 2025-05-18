using System.Buffers;
using System.Collections.Concurrent;
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
    _pageTable = new ConcurrentDictionary<PageId, int>(Environment.ProcessorCount * 2, _poolSizeInPages); // Concurrency level, initial capacity
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
  internal async Task<Page?> FetchPageAsync(PageId pageId)
  {
    _logger.LogTrace("Fetching page {PageId}.", pageId);
    Frame? frame = null;
    int frameIndex = -1; // Initialize to an invalid/default state

    // First check if the page is already in the buffer pool (cache hit).
    if (_pageTable.TryGetValue(pageId, out int cachedFrameIndex))
    {
      _logger.LogTrace("Page {PageId} found in frame {FrameIndex} (cache hit).", pageId, cachedFrameIndex);
      frame = _frames[cachedFrameIndex];
      frameIndex = cachedFrameIndex;
    }
    else // Cache miss
    {
      _logger.LogTrace("Page {PageId} not in cache. Attempting to find an available frame.", pageId);

      // Try to get a frame from the free list first
      if (_freeFrameIndices.TryDequeue(out int dequeuedFrameIndex))
      {
        frameIndex = dequeuedFrameIndex;
        frame = _frames[frameIndex];
        _logger.LogDebug("Found free frame {FrameIndex} for page {PageId}.", frameIndex, pageId);

        // A frame from the free list should ideally not be in active LRU tracking.
        // This step ensures it's cleanly removed from LRU if it somehow was (e.g., after a reset/failed load).
        lock (_replacerLock)
        {
          if (_lruNodeLookup.TryGetValue(frameIndex, out LinkedListNode<int>? node))
          {
            _lruList.Remove(node);
            _lruNodeLookup.Remove(frameIndex); // Remove its old LRU state
            _logger.LogTrace("Removed frame {FrameIndex} from LRU structures as it was obtained from the free list.", frameIndex);
          }
        }
      }
      else // No free frames, try to find a victim to evict
      {
        if (TryFindVictimFrame(out int availableFrameIndex))
        {
          if (!await EvictPageFromFrameAsync(availableFrameIndex))
          {
            _logger.LogCritical("Failed to prepare frame {FrameIndex} by evicting its content. Aborting fetch for {PageId}.", availableFrameIndex, pageId);
            // If eviction failed (e.g., couldn't flush a dirty page), we should attempt to return the frame
            // to the free list so it's not lost from management, though it might be in a problematic state.
            // This recovery needs careful thought. For now, failing the fetch is key.
            lock (_replacerLock)
            {
              // Ensure it's not in LRU (should have been handled by TryFindVictimFrame)
              _lruNodeLookup.Remove(availableFrameIndex);
              // Attempt to add back to free list
              _freeFrameIndices.Enqueue(availableFrameIndex);
            }
            return null;
          }
        }
        else
        {
          _logger.LogWarning("No unpinned victim found in LRU list. Unable to evict a page for {PageId}.", pageId);
          return null; // No available frame to load the page
        }
      }

      // If a frame was secured (either free, or will be from future eviction logic):
      if (frame != null) // This means a free frame was found. Later, eviction will also set 'frame'.
      {
        // TODO: Load the page data into the frame:
        //       This involves:
        //       1. If frame held an old page (after eviction), evict it (flush if dirty, remove from page table).
        //       2. frame.Reset(); frame.CurrentPageId = pageId; frame.IsDirty = false;
        //       3. frame.PageData.Span.Clear(); // Zero out buffer
        //       4. await _diskManager.ReadDiskPageAsync(pageId, frame.PageData);
        //       5. If read fails, reset frame, return to free list, remove from page table if added, return null.
        //       6. _pageTable.TryAdd(pageId, frameIndex); if failed, cleanup and return null.

        // For this incremental step, we've found a free frame.
        // The actual loading into 'frame' and updating 'frame.CurrentPageId'
        // is part of the subsequent TODOs.
        // 'frame' currently points to an empty (or reset by eviction) frame,
        // and 'frameIndex' is its index.
      }
    }

    // Common operations IF a frame was successfully obtained AND prepared for 'pageId'
    // The "prepared for pageId" part is crucial and handled by the TODOs above for a cache miss.
    if (frame != null && (frame.CurrentPageId == pageId || _pageTable.ContainsKey(pageId))) // Refined check: frame is assigned, and it now correctly holds our pageId or is about to.
    {                                                                                        // For now, just 'frame != null' indicates we have a frame to use. The TODOs must ensure it's the *correct* one.
      Interlocked.Increment(ref frame.PinCount);
      MoveToMostRecentlyUsed(frameIndex);
      _logger.LogDebug("Page {PageId} (Frame's CurrentPageId: {FramePageId}) pinned in frame {FrameIndex}. Pin count: {PinCount}",
                       pageId, frame.CurrentPageId, frameIndex, frame.PinCount);
      // For this step, frame.CurrentPageId might not yet be 'pageId' if it's a cache miss.
      // The 'Page' object should be constructed with the 'pageId' we intended to fetch.
      // The frame.CurrentPageId will be updated in the "Update the metadata" TODO.
      return new Page(pageId, frame.PageData); // Constructing Page with the *requested* pageId.
    }

    _logger.LogWarning("Failed to fetch page {PageId}. No suitable frame found or loading process incomplete.", pageId);
    return null; // Page not found or could not be loaded
  }

  /// <summary>
  /// Handles the eviction of a page from a given frame.
  /// This includes removing the page from the page table and writing it to disk if it's dirty.
  /// After successful eviction (or if the frame was already free), the frame's metadata is reset
  /// and its data buffer is cleared.
  /// This method assumes the frame has already been selected as a victim and removed
  /// from the page replacement policy's active tracking (e.g., LRU list).
  /// </summary>
  /// <param name="frameIndexToEvict">The index of the frame to process for eviction.</param>
  /// <returns>
  /// True if the frame was successfully prepared (dirty page flushed if necessary, frame reset and cleared).
  /// False if a critical error occurred, such as failing to flush a dirty page.
  /// </returns>
  private async Task<bool> EvictPageFromFrameAsync(int frameIndexToEvict)
  {
    Frame frame = _frames[frameIndexToEvict];
    _logger.LogTrace("Processing frame {FrameIndex} for eviction. Current PageId: {PageId}, IsDirty: {IsDirty}",
                     frameIndexToEvict,
                     frame.CurrentPageId == default ? "None" : frame.CurrentPageId.ToString(),
                     frame.IsDirty);

    // Step 1: If the frame currently holds a valid page, process it.
    if (frame.CurrentPageId != default(PageId)) // Check against default PageId
    {
      PageId oldPageId = frame.CurrentPageId;

      // 1a. Remove the old page from the page table.
      // This should be done before potential disk I/O to reflect that the page is no longer
      // considered "in this frame" by the page table.
      if (!_pageTable.TryRemove(oldPageId, out _))
      {
        // This indicates an inconsistency: the frame thought it held oldPageId,
        // but the page table didn't map oldPageId to this frame (or oldPageId wasn't in table).
        _logger.LogWarning("During eviction of frame {FrameIndex}, its PageId {OldPageId} was not found in the page table or did not map to this frame. Possible inconsistency.",
                           frameIndexToEvict, oldPageId);
      }

      // 1b. If the page in the frame is dirty, write it to disk.
      if (frame.IsDirty)
      {
        _logger.LogInformation("Flushing dirty page {OldPageId} from frame {FrameIndex} before reuse.", oldPageId, frameIndexToEvict);
        try
        {
          await _diskManager.WriteDiskPageAsync(oldPageId, frame.PageData);
          // If WritePageDiskAsync succeeds, the data on disk for oldPageId is now up-to-date.
          // The frame.IsDirty flag will be reset by frame.Reset() later.
        }
        catch (Exception ex)
        {
          _logger.LogCritical(ex, "CRITICAL: Failed to flush dirty page {OldPageId} from frame {FrameIndex}. Data for {OldPageId} may be lost. Frame cannot be safely reused for new page load at this time.", oldPageId, frameIndexToEvict, oldPageId);
          // If we can't flush a dirty page, it's a significant error.
          // We should not reuse this frame for a new page without resolving the dirty data.
          // Returning false signals that this frame preparation failed.
          return false;
        }
      }
    }

    // Step 2: Reset the frame's metadata and clear its data buffer.
    // This happens whether the frame held a page or was already considered "empty" by CurrentPageId.
    _logger.LogTrace("Resetting frame {FrameIndex} and clearing its buffer.", frameIndexToEvict);
    frame.Reset(); // Sets CurrentPageId = default, IsDirty = false, PinCount = 0
    frame.PageData.Span.Clear(); // Zero out the memory buffer to prevent stale data exposure

    _logger.LogDebug("Frame {FrameIndex} successfully processed for eviction and is now clean and ready for reuse.", frameIndexToEvict);
    return true; // Frame is successfully prepared
  }

  /// <summary>
  /// Tries to find an unpinned frame to evict based on the LRU policy.
  /// If a victim is found, it's removed from the LRU tracking structures.
  /// This method MUST be called while holding the _replacerLock.
  /// </summary>
  /// <param name="frameIndex">
  /// When this method returns true, contains the index of the victim frame.
  /// When this method returns false, the value of frameIndex is undefined (typically -1).
  /// </param>
  /// <returns>True if an unpinned victim frame was found and prepared (removed from LRU); false otherwise.</returns>
  private bool TryFindVictimFrame(out int frameIndex)
  {
    // This method assumes _replacerLock is already held by the caller.
    _logger.LogTrace("Searching for LRU victim (replacer lock held).");

    LinkedListNode<int>? currentNode = _lruList.First; // LRU victim is at the head of the list

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

        frameIndex = candidateFrameIndex;       // Set the out parameter
        _logger.LogDebug("LRU victim selected: Frame {FrameIndex}, previously holding PageId {PageId}.",
                         frameIndex,
                         candidateFrame.CurrentPageId == default ? "None/Empty" : candidateFrame.CurrentPageId.ToString());
        return true; // Indicate success and that frameIndex is valid
      }
      else
      {
        _logger.LogTrace("LRU candidate Frame {FrameIndex} (PageId {PageId}) is pinned (PinCount: {PinCount}). Skipping.",
                         candidateFrameIndex,
                         candidateFrame.CurrentPageId == default ? "None/Empty" : candidateFrame.CurrentPageId.ToString(),
                         candidateFrame.PinCount);
      }
      currentNode = currentNode.Next; // Move to the next candidate
    }

    // If the loop completes, no unpinned page was found in the LRU list.
    _logger.LogWarning("No unpinned victim found in LRU list (all pages currently in LRU are pinned, or LRU list is empty).");
    frameIndex = -1; // Assign a default value for the out parameter on failure
    return false;    // Indicate failure to find a victim
  }


  private void MoveToMostRecentlyUsed(int frameIndex)
  {
    lock (_replacerLock)
    {
      // Remove the node from its current position in the LRU list
      if (_lruNodeLookup.TryGetValue(frameIndex, out LinkedListNode<int>? node))
      {
        _lruList.Remove(node);
      }

      // Add the frame index to the end of the LRU list because is it the Most Recently Used (MRU)
      var last = _lruList.AddLast(frameIndex);
      _lruNodeLookup[frameIndex] = last;
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