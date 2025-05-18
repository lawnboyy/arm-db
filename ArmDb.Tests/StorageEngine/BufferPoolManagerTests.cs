using ArmDb.Common.Abstractions;
using ArmDb.Common.Utils;
using ArmDb.StorageEngine;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ArmDb.UnitTests.StorageEngine;

public class BufferPoolManagerTests : IDisposable
{
  private readonly IFileSystem _fileSystem;
  private readonly DiskManager _diskManager;
  private readonly BufferPoolManager _bpm;
  private readonly string _baseTestDir;
  private readonly BufferPoolManagerOptions _bpmOptions;
  private const int PageSize = Page.Size;

  public BufferPoolManagerTests()
  {
    _fileSystem = new FileSystem();
    _baseTestDir = Path.Combine(Path.GetTempPath(), $"ArmDb_BPM_Tests_{Guid.NewGuid()}");

    // Ensure DiskManager uses a fresh directory for each test class instance
    var diskManagerLogger = NullLogger<DiskManager>.Instance;
    _diskManager = new DiskManager(_fileSystem, diskManagerLogger, _baseTestDir); // Constructor initializes directory

    // Configure BPM
    _bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 10 }; // Small pool for testing
    var bpmLogger = NullLogger<BufferPoolManager>.Instance;
    _bpm = new BufferPoolManager(Options.Create(_bpmOptions), _diskManager, bpmLogger);
  }

  [Fact]
  public async Task FetchPageAsync_NewPage_LoadsFromDiskAndPins()
  {
    // Arrange
    int tableId = 1;
    int pageIndex = 0;
    var pageId = new PageId(tableId, pageIndex);

    // Create sample page data to write to disk
    byte[] diskPageData = new byte[PageSize];
    for (int i = 0; i < PageSize; i++) diskPageData[i] = (byte)i; // Fill with 0-255 pattern
    await CreateTestPageFileWithDataAsync(tableId, pageIndex, diskPageData);

    // Act
    Page? fetchedPage = await _bpm.FetchPageAsync(pageId);

    // Assert
    Assert.NotNull(fetchedPage); // Page should be fetched
    Assert.Equal(pageId, fetchedPage.Id); // Verify PageId
    Assert.True(fetchedPage.Data.Span.SequenceEqual(diskPageData), "Page content does not match disk content."); // Verify content
  }

  // [Fact]
  // public async Task FetchPageAsync_PageAlreadyCached_ReturnsFromCacheAndIncrementsPin()
  // {
  //   // Arrange
  //   int tableId = 10;
  //   int pageIndex = 0;
  //   var pageId = new PageId(tableId, pageIndex);

  //   // Create sample page data and write it to a mock disk file
  //   byte[] diskPageData = new byte[PageSize];
  //   diskPageData[0] = (byte)'A'; // Initial data
  //   diskPageData[1] = (byte)'B';
  //   await CreateTestPageFileWithDataAsync(tableId, pageIndex, diskPageData);

  //   // Act: First fetch (cache miss, loads from disk)
  //   Page? pageInstance1 = await _bpm.FetchPageAsync(pageId);
  //   Assert.NotNull(pageInstance1);
  //   Assert.Equal(pageId, pageInstance1.Id);
  //   Assert.Equal((byte)'A', pageInstance1.Data.Span[0]);

  //   // Modify the data through the first Page object's memory.
  //   // This change happens in the buffer pool's frame.
  //   pageInstance1.Data.Span[0] = (byte)'X';

  //   // Act: Second fetch for the same page (should be a cache hit)
  //   Page? pageInstance2 = await _bpm.FetchPageAsync(pageId);

  //   // Assert
  //   Assert.NotNull(pageInstance2);
  //   Assert.Equal(pageId, pageInstance2.Id);

  //   // 1. Verify that pageInstance2 sees the modification made through pageInstance1.
  //   // This confirms they share the same underlying Memory<byte> from the frame,
  //   // proving it was a cache hit and not re-read from the original disk content.
  //   Assert.Equal((byte)'X', pageInstance2.Data.Span[0]);
  //   Assert.Equal((byte)'B', pageInstance2.Data.Span[1]); // Unchanged original data

  //   // 2. Verify the original disk data is unchanged (BPM hasn't flushed yet).
  //   // This indirectly supports that the second fetch was a cache hit.
  //   // We'll read the file directly to check.
  //   var actualDiskData = new byte[PageSize];
  //   await using (var handle = File.OpenHandle(GetExpectedTablePath(tableId), FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous))
  //   {
  //     await RandomAccess.ReadAsync(handle, actualDiskData.AsMemory(), 0);
  //   }
  //   Assert.Equal((byte)'A', actualDiskData[0]); // Original disk data

  //   // Note: Directly testing PinCount and LRU state changes requires either:
  //   // - Exposing internal state/methods of BufferPoolManager for testing (e.g., via InternalsVisibleTo).
  //   // - More complex scenario tests involving unpinning and eviction to observe behavior.
  //   // This test focuses on the core cache hit behavior: data consistency and avoiding a disk read.
  // }

  // Helper to get the expected table file path
  private string GetExpectedTablePath(int tableId) => Path.Combine(_baseTestDir, $"{tableId}{DiskManager.TableFileExtension}");

  // Helper to create a file with specific page content for tests
  private async Task CreateTestPageFileWithDataAsync(int tableId, int pageIndex, byte[] pageData)
  {
    if (pageData.Length != PageSize)
      throw new ArgumentException("Page data must be PageSize.", nameof(pageData));

    var filePath = GetExpectedTablePath(tableId);
    long offset = (long)pageIndex * PageSize;

    // Ensure directory for the file path exists (DiskManager's constructor does it for _baseTestDir)
    // For specific table files, PhysicalFileSystem's WriteFileAsync will create if OpenOrCreate is used.
    // We can use our IFileSystem implementation here.
    await _fileSystem.WriteFileAsync(filePath, offset, pageData.AsMemory());
  }

  public void Dispose()
  {
    // Cleanup task: dispose BPM to flush pages and release resources
    // _bpm.DisposeAsync().AsTask().GetAwaiter().GetResult(); // Synchronous wait for dispose in sync Dispose

    // Cleanup: Delete the temporary directory and its contents
    try
    {
      if (Directory.Exists(_baseTestDir))
      {
        Directory.Delete(_baseTestDir, recursive: true);
      }
    }
    catch (Exception ex)
    {
      Console.WriteLine($"Error cleaning up test directory '{_baseTestDir}': {ex.Message}");
    }
    GC.SuppressFinalize(this);
  }
}