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
  private readonly LinkedList<int> _lruList; // Stores frame indices. MRU at one end, LRU at the other.
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

  // A finalizer (~BufferPoolManager()) is generally not needed if IAsyncDisposable
  // is implemented correctly and all managed/unmanaged resources are handled
  // in DisposeAsync. If you were directly managing unmanaged handles that
  // weren't wrapped in SafeHandle types, a finalizer might be considered.
}