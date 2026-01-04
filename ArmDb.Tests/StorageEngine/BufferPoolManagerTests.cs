using ArmDb.Common.Abstractions;
using ArmDb.Common.Utils;
using ArmDb.Storage;
using ArmDb.Storage.Exceptions;
using ArmDb.UnitTests.Server;
using ArmDb.UnitTests.TestUtils;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit.Abstractions;
using static ArmDb.UnitTests.Storage.StorageEngineTestHelper;

namespace ArmDb.UnitTests.Storage;

public partial class BufferPoolManagerTests : IDisposable
{
  private readonly IFileSystem _fileSystem;
  private readonly DiskManager _diskManager;
  private readonly BufferPoolManager _bpm;
  private readonly string _baseTestDir;
  private readonly BufferPoolManagerOptions _bpmOptions;
  private const int PageSize = Page.Size;

  public BufferPoolManagerTests(ITestOutputHelper output)
  {
    _fileSystem = new FileSystem();
    _baseTestDir = Path.Combine(Path.GetTempPath(), $"ArmDb_BPM_Tests_{Guid.NewGuid()}");

    // Ensure DiskManager uses a fresh directory for each test class instance
    var diskManagerLogger = NullLogger<DiskManager>.Instance;
    _diskManager = new DiskManager(_fileSystem, diskManagerLogger, _baseTestDir); // Constructor initializes directory

    // Configure BPM
    _bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 10 }; // Small pool for testing

    var loggerFactory = LoggerFactory.Create(builder =>
            {
              builder
              .AddFilter("ArmDb.Storage.BufferPoolManager", LogLevel.Trace) // Show Trace and above for BPM
              .AddFilter("Default", LogLevel.Information) // Other categories at Information
                                                          // .AddConsole(); // Standard console logger
                                                          // For xUnit, a provider that writes to ITestOutputHelper is best.
                                                          // If you don't have a specific xUnit logging provider,
                                                          // AddDebug() or AddConsole() might show up in "Test Detail Summary" or general output.
              .AddXUnit(output); // Requires a NuGet package like Xunit.Microsoft.Extensions.Logging
                                 // Or implement a simple one yourself if needed for just console-like output
            });
    var bpmLogger = loggerFactory.CreateLogger<BufferPoolManager>();

    //var bpmLogger = NullLogger<BufferPoolManager>.Instance;
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
  public async Task FetchPageAsync_PoolFullOfPinnedPages_Throws()
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
    await CreateTestPageFileWithDataAsync(tableId, 0, CreateTestBuffer((byte)'A'));
    await CreateTestPageFileWithDataAsync(tableId, 1, CreateTestBuffer((byte)'B'));
    await CreateTestPageFileWithDataAsync(tableId, 2, CreateTestBuffer((byte)'C'));
    await CreateTestPageFileWithDataAsync(tableId, 3, CreateTestBuffer((byte)'D'));

    // Act: Fill the buffer pool. Each fetched page will be pinned (PinCount = 1).
    Page? p0 = await localBpm.FetchPageAsync(pageId0);
    Page? p1 = await localBpm.FetchPageAsync(pageId1);
    Page? p2 = await localBpm.FetchPageAsync(pageId2);

    Assert.NotNull(p0); // Ensure setup is correct
    Assert.NotNull(p1);
    Assert.NotNull(p2);

    // Act: Attempt to fetch a 4th page, which should fail as pool is full of pinned pages
    Page? p3 = null;
    await Assert.ThrowsAsync<BufferPoolFullException>(async () => { p3 = await localBpm.FetchPageAsync(pageId3); });

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
    var exception = Assert.Throws<InvalidOperationException>(() =>
        _bpm.UnpinPage(nonExistentPageId, isDirty)
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
    _bpm.UnpinPage(pageId, isDirty: false);

    // At this point, the Frame for pageId in the BPM should have PinCount = 0.

    // Act: Attempt to unpin the *same page* again.
    var exception = Assert.Throws<InvalidOperationException>(() =>
        _bpm.UnpinPage(pageId, isDirty: false)
    );

    // Assert
    Assert.NotNull(exception.Message);
    Assert.Contains($"Page {pageId}", exception.Message);
    Assert.Contains("pin count is 0 (must be > 0)", exception.Message);
  }

  [Fact]
  public async Task FetchPageAsync_DiskReadFailsForNewPage_ThrowsPageLoadFromDiskException()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    // DiskManager uses the base test directory, ensure it exists for ControllableFileSystem's perspective
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManagerWithError = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    var options = new BufferPoolManagerOptions { PoolSizeInPages = 1 }; // Small pool
    var bpmWithErrorHandling = new BufferPoolManager(Options.Create(options), diskManagerWithError, NullLogger<BufferPoolManager>.Instance);

    int tableId = 701;
    var pageIdToLoad = new PageId(tableId, 0);
    var filePath = GetExpectedTablePath(tableId); // This path is relative to _baseTestDir

    // No actual file on disk is created for ControllableFileSystem unless explicitly added to its dictionary.
    // We want ReadFileAsync to throw.
    mockFileSystem.ReadFailurePaths.Add(filePath); // Configure mock to fail on read for this path

    // Act & Assert
    var exception = await Assert.ThrowsAsync<CouldNotLoadPageFromDiskException>(() =>
        bpmWithErrorHandling.FetchPageAsync(pageIdToLoad)
    );

    Assert.NotNull(exception.InnerException);
    Assert.IsType<IOException>(exception.InnerException); // Check that the underlying IO error is preserved
    Assert.Contains($"Failed to read page {pageIdToLoad} from disk", exception.Message);

    // Verify the frame that was attempted is now free again (BPM internal state)
    // This requires a test hook or verifying that fetching another page uses that frame.
    // For now, primarily test the exception.
    // Example of an advanced check if a test hook was available:
    // Assert.Null(bpmWithErrorHandling.GetFrameByPageId_TestOnly(pageIdToLoad));

    await bpmWithErrorHandling.DisposeAsync();
  }

  [Fact]
  public async Task FetchPageAsync_DirtyVictimFlushFails_ThrowsPageFlushException()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManagerWithError = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    var options = new BufferPoolManagerOptions { PoolSizeInPages = 1 }; // Pool size of 1 to force eviction
    var bpmWithErrorHandling = new BufferPoolManager(Options.Create(options), diskManagerWithError, NullLogger<BufferPoolManager>.Instance);

    int tableId = 702;
    var pageIdToEvict = new PageId(tableId, 0); // This will be our dirty victim
    var pageIdToLoad = new PageId(tableId, 1);  // This is the new page we want to load

    // Setup initial data for pageIdToEvict in our controllable FS
    byte[] initialPage0Data = CreateTestBuffer(0xAA);
    var page0FilePath = GetExpectedTablePath(pageIdToEvict.TableId); // Use pageIdToEvict.TableId
    mockFileSystem.AddFile(page0FilePath, initialPage0Data);

    // Fetch, modify, and unpin pageIdToEvict as dirty
    Page? p0 = await bpmWithErrorHandling.FetchPageAsync(pageIdToEvict);
    Assert.NotNull(p0);
    p0.Data.Span[0] = 0xFF; // Modify it
    bpmWithErrorHandling.UnpinPage(pageIdToEvict, true); // Unpin as dirty

    // Setup ControllableFileSystem to fail when WriteFileAsync is called for pageIdToEvict's file
    mockFileSystem.WriteFailurePaths.Add(page0FilePath);

    // Act & Assert: Attempting to fetch pageIdToLoad should trigger eviction of dirty pageIdToEvict,
    // which will fail during flush.
    var exception = await Assert.ThrowsAsync<CouldNotFlushToDiskException>(() =>
        bpmWithErrorHandling.FetchPageAsync(pageIdToLoad)
    );

    Assert.NotNull(exception.InnerException);
    Assert.IsType<IOException>(exception.InnerException);
    Assert.Contains($"Failed to flush dirty page {pageIdToEvict}", exception.Message);

    // Verify pageIdToLoad was not loaded
    // This would require an internal check or fetching it and expecting a miss / specific error
    // Assert.Null(bpmWithErrorHandling.GetFrameByPageId_TestOnly(pageIdToLoad));

    await bpmWithErrorHandling.DisposeAsync();
  }

  // In BufferPoolManagerTests.cs

  [Fact]
  public async Task FlushAllDirtyPagesAsync_OnlyWritesDirtyPages_AndResetsFlags()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManager = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    // Pool size must be large enough to hold all pages without eviction
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 5 };
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 201;
    var cleanPageId0 = new PageId(tableId, 0);
    var dirtyPageId1 = new PageId(tableId, 1);
    var cleanPageId2 = new PageId(tableId, 2);
    var dirtyPageId3 = new PageId(tableId, 3);

    // Prepare initial data for all pages
    var pageDataMap = new Dictionary<PageId, byte[]>
    {
        { cleanPageId0, CreateTestBuffer(0xAA) },
        { dirtyPageId1, CreateTestBuffer(0xBB) },
        { cleanPageId2, CreateTestBuffer(0xCC) },
        { dirtyPageId3, CreateTestBuffer(0xDD) }
    };
    CreateMultiPageTestFileInMock(mockFileSystem, tableId, pageDataMap);
    string filePath = GetExpectedTablePath(tableId);

    // Prepare modified data for the dirty pages
    var p1ModifiedData = CreateTestBuffer(0xB1);
    var p3ModifiedData = CreateTestBuffer(0xD1);

    // 1. Fetch all pages to load them into the buffer pool
    var p0 = await bpm.FetchPageAsync(cleanPageId0);
    var p1 = await bpm.FetchPageAsync(dirtyPageId1);
    var p2 = await bpm.FetchPageAsync(cleanPageId2);
    var p3 = await bpm.FetchPageAsync(dirtyPageId3);
    Assert.NotNull(p0); Assert.NotNull(p1); Assert.NotNull(p2); Assert.NotNull(p3);

    // 2. Modify the pages that should be dirty
    p1ModifiedData.CopyTo(p1.Data);
    p3ModifiedData.CopyTo(p3.Data);

    // 3. Unpin pages, setting the dirty flag appropriately
    bpm.UnpinPage(cleanPageId0, isDirty: false);
    bpm.UnpinPage(dirtyPageId1, isDirty: true);
    bpm.UnpinPage(cleanPageId2, isDirty: false);
    bpm.UnpinPage(dirtyPageId3, isDirty: true);

    // Reset I/O counters after the setup phase
    mockFileSystem.ResetReadFileCallCount(filePath);

    // Act
    await bpm.FlushAllDirtyPagesAsync();

    // Assert
    // 1. Verify disk write counts
    Assert.Equal(2, mockFileSystem.GetWriteFileCallCount(filePath)); // Only dirty pages should be written
    Assert.Equal(0, mockFileSystem.GetReadFileCallCount(filePath));  // No reads should have occurred

    // 2. Verify the flushed data on "disk" is the MODIFIED data
    Assert.True(mockFileSystem.Files.TryGetValue(filePath, out byte[]? finalFileContent));
    Assert.NotNull(finalFileContent);

    // Check the dirty pages' content
    var p1_diskContentAfter = finalFileContent.AsSpan((int)Page.Size * 1, Page.Size);
    var p3_diskContentAfter = finalFileContent.AsSpan((int)Page.Size * 3, Page.Size);
    Assert.True(p1ModifiedData.AsSpan().SequenceEqual(p1_diskContentAfter), "Dirty page P1 was not flushed correctly.");
    Assert.True(p3ModifiedData.AsSpan().SequenceEqual(p3_diskContentAfter), "Dirty page P3 was not flushed correctly.");

    // Check that the clean pages' content was NOT modified
    var p0_diskContentAfter = finalFileContent.AsSpan((int)Page.Size * 0, Page.Size);
    var p2_diskContentAfter = finalFileContent.AsSpan((int)Page.Size * 2, Page.Size);
    Assert.True(pageDataMap[cleanPageId0].AsSpan().SequenceEqual(p0_diskContentAfter), "Clean page P0 content was unexpectedly changed.");
    Assert.True(pageDataMap[cleanPageId2].AsSpan().SequenceEqual(p2_diskContentAfter), "Clean page P2 content was unexpectedly changed.");


    // 3. Verify IsDirty flags were reset to false for the flushed pages
#if DEBUG
    var frame1 = bpm.GetFrameByPageId_TestOnly(dirtyPageId1);
    var frame3 = bpm.GetFrameByPageId_TestOnly(dirtyPageId3);
    Assert.NotNull(frame1);
    Assert.NotNull(frame3);
    Assert.False(frame1.IsDirty, "IsDirty flag for flushed page P1 should be false.");
    Assert.False(frame3.IsDirty, "IsDirty flag for flushed page P3 should be false.");

    // Also verify clean pages remain clean
    var frame0 = bpm.GetFrameByPageId_TestOnly(cleanPageId0);
    var frame2 = bpm.GetFrameByPageId_TestOnly(cleanPageId2);
    Assert.NotNull(frame0);
    Assert.NotNull(frame2);
    Assert.False(frame0.IsDirty);
    Assert.False(frame2.IsDirty);
#endif

    // Cleanup
    await bpm.DisposeAsync();
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

  private async Task<byte[]> ReadPageDirectlyAsync(string path, int pageIndex)
  {
    var buffer = new byte[PageSize];
    // Check existence using the abstraction
    if (!_fileSystem.FileExists(path))
    {
      Array.Clear(buffer);
      return buffer;
    }

    try
    {
      // Use the abstraction to read for verification
      int bytesRead = await _fileSystem.ReadFileAsync(path, (long)pageIndex * PageSize, buffer.AsMemory());
      if (bytesRead < PageSize && bytesRead >= 0)
      {
        Array.Clear(buffer, bytesRead, PageSize - bytesRead);
      }
    }
    catch (Exception ex) when (ex is FileNotFoundException || ex is ArgumentOutOfRangeException || ex is IOException)
    {
      Console.WriteLine($"Note: Exception during direct read for verification in test: {ex.GetType().Name} - {ex.Message}");
      Array.Clear(buffer);
    }
    return buffer;
  }
}