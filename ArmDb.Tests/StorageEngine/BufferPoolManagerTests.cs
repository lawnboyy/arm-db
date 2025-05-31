using ArmDb.Common.Abstractions;
using ArmDb.Common.Utils;
using ArmDb.StorageEngine;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using static ArmDb.UnitTests.StorageEngine.StorageEngineTestHelper;

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

  [Fact]
  public async Task FetchPageAsync_PageAlreadyCached_ReturnsFromCacheAndIncrementsPin()
  {
    // Arrange
    int tableId = 10;
    int pageIndex = 0;
    var pageId = new PageId(tableId, pageIndex);

    // Create sample page data and write it to a mock disk file
    byte[] diskPageData = new byte[PageSize];
    diskPageData[0] = (byte)'A'; // Initial data
    diskPageData[1] = (byte)'B';
    await CreateTestPageFileWithDataAsync(tableId, pageIndex, diskPageData);

    // Act: First fetch (cache miss, loads from disk)
    Page? pageInstance1 = await _bpm.FetchPageAsync(pageId);
    Assert.NotNull(pageInstance1);
    Assert.Equal(pageId, pageInstance1.Id);
    Assert.Equal((byte)'A', pageInstance1.Data.Span[0]);

    // Modify the data through the first Page object's memory.
    // This change happens in the buffer pool's frame.
    pageInstance1.Data.Span[0] = (byte)'X';

    // Act: Second fetch for the same page (should be a cache hit)
    Page? pageInstance2 = await _bpm.FetchPageAsync(pageId);

    // Assert
    Assert.NotNull(pageInstance2);
    Assert.Equal(pageId, pageInstance2.Id);

    // 1. Verify that pageInstance2 sees the modification made through pageInstance1.
    // This confirms they share the same underlying Memory<byte> from the frame,
    // proving it was a cache hit and not re-read from the original disk content.
    Assert.Equal((byte)'X', pageInstance2.Data.Span[0]);
    Assert.Equal((byte)'B', pageInstance2.Data.Span[1]); // Unchanged original data

    // 2. Verify the original disk data is unchanged (BPM hasn't flushed yet).
    // This indirectly supports that the second fetch was a cache hit.
    // We'll read the file directly to check.
    var actualDiskData = new byte[PageSize];
    using (var handle = File.OpenHandle(GetExpectedTablePath(tableId), FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous))
    {
      await RandomAccess.ReadAsync(handle, actualDiskData.AsMemory(), 0);
    }
    Assert.Equal((byte)'A', actualDiskData[0]); // Original disk data

    // Note: Directly testing PinCount and LRU state changes requires either:
    // - Exposing internal state/methods of BufferPoolManager for testing (e.g., via InternalsVisibleTo).
    // - More complex scenario tests involving unpinning and eviction to observe behavior.
    // This test focuses on the core cache hit behavior: data consistency and avoiding a disk read.
  }

  [Fact]
  public async Task FetchPageAsync_PoolFullOfPinnedPages_ReturnsNull()
  {
    // Arrange
    // Re-initialize BPM with a small pool size for this specific test
    var options = new BufferPoolManagerOptions { PoolSizeInPages = 3 }; // Small pool
    var localBpm = new BufferPoolManager(Options.Create(options), _diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 20;
    var pageId0 = new PageId(tableId, 0);
    var pageId1 = new PageId(tableId, 1);
    var pageId2 = new PageId(tableId, 2);
    var pageId3 = new PageId(tableId, 3); // The page that shouldn't fit

    // Create corresponding data on "disk"
    await CreateTestPageFileWithDataAsync(tableId, 0, StorageEngineTestHelper.CreateTestBuffer((byte)'A'));
    await CreateTestPageFileWithDataAsync(tableId, 1, StorageEngineTestHelper.CreateTestBuffer((byte)'B'));
    await CreateTestPageFileWithDataAsync(tableId, 2, StorageEngineTestHelper.CreateTestBuffer((byte)'C'));
    await CreateTestPageFileWithDataAsync(tableId, 3, StorageEngineTestHelper.CreateTestBuffer((byte)'D'));

    // Act: Fill the buffer pool. Each fetched page will be pinned (PinCount = 1).
    Page? p0 = await localBpm.FetchPageAsync(pageId0);
    Page? p1 = await localBpm.FetchPageAsync(pageId1);
    Page? p2 = await localBpm.FetchPageAsync(pageId2);

    Assert.NotNull(p0); // Ensure setup is correct
    Assert.NotNull(p1);
    Assert.NotNull(p2);

    // Act: Attempt to fetch a 4th page, which should fail as pool is full of pinned pages
    Page? p3 = await localBpm.FetchPageAsync(pageId3);

    // Assert
    Assert.Null(p3); // Should fail to fetch and return null

    // Optional: Verify the original pages are still accessible if we kept references
    // (This just confirms our p0,p1,p2 are still valid, not a direct BPM state check)
    Assert.Equal((byte)'A', p0.Data.Span[0]);
    Assert.Equal((byte)'B', p1.Data.Span[0]);
    Assert.Equal((byte)'C', p2.Data.Span[0]);

    // Cleanup for this local BPM instance
    await localBpm.DisposeAsync();
  }

  [Fact]
  public async Task UnpinPageAsync_PageNotInPageTable_ThrowsInvalidOperationException()
  {
    // Arrange
    // _bpm is initialized in the constructor with an empty page table.
    // We'll use a PageId that is guaranteed not to have been fetched.
    var nonExistentPageId = new PageId(tableId: 999, pageIndex: 0);
    bool isDirty = false;

    // Act & Assert
    var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
        _bpm.UnpinPageAsync(nonExistentPageId, isDirty)
    );

    // Optional: Verify the exception message for more specificity
    Assert.NotNull(exception.Message);
  }

  [Fact]
  public async Task UnpinPageAsync_PinCountAlreadyZero_ThrowsInvalidOperationException()
  {
    // Arrange
    int tableId = 501; // Use a unique tableId for test isolation
    int pageIndex = 0;
    var pageId = new PageId(tableId, pageIndex);
    byte[] pageData = CreateTestBuffer(0xCF); // Use helper to create page data

    // Create the file on disk so FetchPageAsync can load it
    await CreateTestPageFileWithDataAsync(tableId, pageIndex, pageData);

    // 1. Fetch the page: this pins it, and its PinCount should be 1 internally.
    Page? fetchedPage = await _bpm.FetchPageAsync(pageId);
    Assert.NotNull(fetchedPage); // Ensure the page was actually fetched for setup

    // 2. Unpin it the first time: PinCount should go from 1 to 0.
    //    We assume this call works as intended based on future tests or implementation.
    //    No specific 'isDirty' concern for this test's purpose.
    await _bpm.UnpinPageAsync(pageId, isDirty: false);

    // At this point, the Frame for pageId in the BPM should have PinCount = 0.

    // Act: Attempt to unpin the *same page* again.
    var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
        _bpm.UnpinPageAsync(pageId, isDirty: false)
    );

    // Assert
    Assert.NotNull(exception.Message);
    Assert.Contains($"Page {pageId}", exception.Message);
    Assert.Contains("pin count is 0 (must be > 0)", exception.Message);
  }

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