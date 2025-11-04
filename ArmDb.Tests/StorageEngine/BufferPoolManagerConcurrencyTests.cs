using ArmDb.StorageEngine;
using ArmDb.UnitTests.TestUtils;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using static ArmDb.UnitTests.StorageEngine.StorageEngineTestHelper;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BufferPoolManagerTests
{
  [Fact]
  public async Task FetchPageAsync_ConcurrentFetchesForSameNewPage_ReadsDiskOnceAndCorrectlyPins()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    // DiskManager needs its base directory to exist even in the mock FS if it checks/creates it.
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManagerWithControllableFs = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 5 }; // Sufficiently large pool
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManagerWithControllableFs, NullLogger<BufferPoolManager>.Instance);

    int tableId = 501;
    var targetPageId = new PageId(tableId, 0);
    string filePath = GetExpectedTablePath(tableId); // Helper to get consistent path based on _baseTestDir
    byte[] pageDataOnDisk = CreateTestBuffer(0xAA); // Creates a PageSize buffer filled with 0xAA

    // Add the file content to our controllable file system
    mockFileSystem.AddFile(filePath, pageDataOnDisk);
    // controllableFs.ResetReadFileCallCount(filePath); // Call this if your controllable FS accumulates counts globally

    int concurrentFetches = 3;
    var fetchTasks = new List<Task<Page>>();

    // Act
    // Launch all tasks. Using Task.Run ensures they are scheduled on thread pool
    // and have a better chance of executing concurrently, stressing the BPM's sync mechanisms.
    for (int i = 0; i < concurrentFetches; i++)
    {
      fetchTasks.Add(Task.Run(() => bpm.FetchPageAsync(targetPageId)));
    }

    // Wait for all fetch operations to complete
    Page[] results = await Task.WhenAll(fetchTasks);

    // Assert
    // 1. All fetches succeeded and returned non-null Page objects
    Assert.Equal(concurrentFetches, results.Length);
    Assert.All(results, Assert.NotNull); // Ensure no null pages returned

    // 2. All Page objects have the correct PageId and initial content
    foreach (var page in results)
    {
      Assert.Equal(targetPageId, page!.Id); // page! because Assert.NotNull was called
      Assert.True(page.Data.Span.SequenceEqual(pageDataOnDisk), $"Page {page.Id} content mismatch after initial concurrent fetch.");
    }

    // 3. DiskManager's ReadFileAsync (via IFileSystem.ReadFileAsync) was called only once
    // This assertion relies on you implementing call counting in your ControllableFileSystem
    // For example: Assert.Equal(1, controllableFs.GetReadFileCallCount(filePath));
    // Let's assume your ControllableFileSystem has a way to get this count.
    // If not, the shared buffer test (4) is an indirect proof.
    var readStats = mockFileSystem as BpmMockFileSystem; // Cast if needed, or ensure your _fileSystem field in the test class is already the controllable one
    if (readStats != null)
    {
      Assert.Equal(1, readStats.GetReadFileCallCount(filePath)); // Replace GetReadFileCallCount with your actual method
    }


    // 4. All returned Page objects share the same underlying memory buffer
    if (results.Length > 1 && results[0] != null && results[1] != null)
    {
      // Modify data through the first fetched page instance
      results[0].Data.Span[0] = 0xFF; // Change first byte
      results[0].Data.Span[10] = 0xEE; // Change another byte

      // Verify the change is reflected in another fetched page instance
      Assert.Equal(0xFF, results[1].Data.Span[0]);
      Assert.Equal(0xEE, results[1].Data.Span[10]);
    }

    // 5. Pin Count (requires test hook in BPM)
    // This part assumes you have the GetFrameByPageId_TestOnly method and PinCount is accessible.
#if DEBUG
    var frame = bpm.GetFrameByPageId_TestOnly(targetPageId);
    Assert.NotNull(frame);
    // Each successful FetchPageAsync call pins the page.
    // If any task failed and returned null (though our test asserts non-null),
    // the count would be lower. Here, all should have succeeded.
    Assert.Equal(concurrentFetches, frame.PinCount);

    // Unpin all fetched pages to clean up frame's pin count for subsequent tests if BPM instance were reused
    foreach (var pageInstance in results)
    {
      if (pageInstance != null)
      {
        await bpm.UnpinPageAsync(pageInstance.Id, false);
      }
    }
    Assert.Equal(0, frame.PinCount); // Verify it goes back to 0
#endif

    // Cleanup the specific BPM instance for this test if it holds unmanaged resources directly
    // or to ensure its state doesn't affect other tests (if not creating a new one per test).
    await bpm.DisposeAsync();
  }

  [Fact]
  public async Task FetchPageAsync_ConcurrentFetchesForDifferentNewPages_LoadInParallel()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManager = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 5 }; // Pool large enough for all pages
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 801;
    int numConcurrentPages = 3;
    var pagesToFetch = new List<PageId>();
    var pageDataOnDisk = new Dictionary<PageId, byte[]>();

    // 1. Prepare data for multiple distinct pages on our mock "disk"
    for (int i = 0; i < numConcurrentPages; i++)
    {
      var pageId = new PageId(tableId, i);
      // Create a unique data pattern for each page
      var pageData = CreateTestBuffer((byte)(i + 1)); // Page 0 filled with 1s, Page 1 with 2s, etc.
      pagesToFetch.Add(pageId);
      pageDataOnDisk.Add(pageId, pageData);
    }
    CreateMultiPageTestFileInMock(mockFileSystem, tableId, pageDataOnDisk);

    var fetchTasks = new List<Task<Page>>();

    // Act
    // 2. Launch all fetch tasks concurrently.
    //    Task.Run helps ensure they are scheduled on the thread pool.
    foreach (var pageId in pagesToFetch)
    {
      fetchTasks.Add(Task.Run(() => bpm.FetchPageAsync(pageId)));
    }

    // 3. Wait for all tasks to complete
    Page[] results = await Task.WhenAll(fetchTasks);

    // Assert
    // 4. Verify all fetches succeeded and returned the correct page data
    Assert.Equal(numConcurrentPages, results.Length);
    Assert.All(results, Assert.NotNull);

    foreach (var fetchedPage in results)
    {
      Assert.True(pageDataOnDisk.TryGetValue(fetchedPage!.Id, out byte[]? expectedData));
      Assert.True(fetchedPage.Data.Span.SequenceEqual(expectedData!), $"Content for page {fetchedPage.Id} did not match.");
    }

    // 5. Verify DiskManager read was called exactly once for the table file
    //    (This requires the ControllableFileSystem to track call counts)
    string filePath = GetExpectedTablePath(tableId);
    // Assumes your ControllableFileSystem has GetReadFileCallCount implemented
    Assert.Equal(numConcurrentPages, mockFileSystem.GetReadFileCallCount(filePath));

    // 6. Verify each page is pinned exactly once
#if DEBUG
    foreach (var pageId in pagesToFetch)
    {
      var frame = bpm.GetFrameByPageId_TestOnly(pageId);
      Assert.NotNull(frame);
      Assert.Equal(1, frame.PinCount);
    }
#endif

    // Cleanup
    await bpm.DisposeAsync();
  }

  [Fact]
  public async Task FetchPageAsync_ConcurrentMixOfCachedAndNewPages_LoadCorrectly()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManager = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 5 }; // Pool large enough to prevent evictions
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 901;
    var cachedPageId = new PageId(tableId, 0);
    var newPageId1 = new PageId(tableId, 1);
    var newPageId2 = new PageId(tableId, 2);

    // Create data for all pages and add them to our "disk"
    var pageDataOnDisk = new Dictionary<PageId, byte[]>
        {
            { cachedPageId, CreateTestBuffer(0xAA) },
            { newPageId1, CreateTestBuffer(0xBB) },
            { newPageId2, CreateTestBuffer(0xCC) }
        };
    CreateMultiPageTestFileInMock(mockFileSystem, tableId, pageDataOnDisk);
    string filePath = GetExpectedTablePath(tableId);


    // --- Pre-populate the cache with cachedPageId ---
    Page? prewarmedPage = await bpm.FetchPageAsync(cachedPageId);
    Assert.NotNull(prewarmedPage);
    await bpm.UnpinPageAsync(cachedPageId, isDirty: false); // Unpin it so its PinCount starts at 0 for our test

    // Reset the read count *after* the setup fetch
    // Assuming your ControllableFileSystem has a method like this
    mockFileSystem.ResetReadFileCallCount(filePath);


    // --- Prepare concurrent tasks (Corrected Method) ---
    int hitTasksCount = 3;

    // 1. Create a list of delegates (Func<Task<Page>>) representing the work to be done.
    var taskFactories = new List<Func<Task<Page>>>();

    // Add delegates for cache hits
    for (int i = 0; i < hitTasksCount; i++)
    {
      // The lambda isn't executed yet, just stored in the list.
      taskFactories.Add(() => bpm.FetchPageAsync(cachedPageId));
    }
    // Add delegates for cache misses
    taskFactories.Add(() => bpm.FetchPageAsync(newPageId1));
    taskFactories.Add(() => bpm.FetchPageAsync(newPageId2));

    // 2. Shuffle the list of delegates. This randomizes the launch order.
    var random = new Random();
    var shuffledFactories = taskFactories.OrderBy(t => random.Next()).ToList();

    // 3. Now, execute the shuffled delegates to create and start the hot tasks.
    //    Using Task.Run for each ensures they are queued on the ThreadPool.
    var runningTasks = shuffledFactories.Select(factory => Task.Run(factory)).ToList();



    // Act
    // Execute all tasks concurrently and wait for them to complete
    Page[] results = await Task.WhenAll(runningTasks);


    // Assert
    Assert.Equal(hitTasksCount + 2, results.Length); // Ensure all tasks completed

    // 1. Check disk reads - expecting 2 (one for each new page)
    Assert.Equal(2, mockFileSystem.GetReadFileCallCount(filePath));

    // 2. Group results by PageId for easier assertions
    var hitResults = results.Where(p => p?.Id == cachedPageId).ToArray();
    var miss1Result = results.FirstOrDefault(p => p?.Id == newPageId1);
    var miss2Result = results.FirstOrDefault(p => p?.Id == newPageId2);

    Assert.Equal(hitTasksCount, hitResults.Length);
    Assert.NotNull(miss1Result);
    Assert.NotNull(miss2Result);

    // 3. Verify data integrity of all returned pages
    Assert.All(hitResults, p => Assert.True(p!.Data.Span.SequenceEqual(pageDataOnDisk[cachedPageId])));
    Assert.True(miss1Result!.Data.Span.SequenceEqual(pageDataOnDisk[newPageId1]));
    Assert.True(miss2Result!.Data.Span.SequenceEqual(pageDataOnDisk[newPageId2]));

    // 4. Check Pin Counts using the test-only helper method
#if DEBUG
    var cachedFrame = bpm.GetFrameByPageId_TestOnly(cachedPageId);
    Assert.NotNull(cachedFrame);
    Assert.Equal(hitTasksCount, cachedFrame.PinCount); // 3 cache hits -> PinCount = 3

    var newFrame1 = bpm.GetFrameByPageId_TestOnly(newPageId1);
    Assert.NotNull(newFrame1);
    Assert.Equal(1, newFrame1.PinCount);

    var newFrame2 = bpm.GetFrameByPageId_TestOnly(newPageId2);
    Assert.NotNull(newFrame2);
    Assert.Equal(1, newFrame2.PinCount);
#endif

    // Cleanup local BPM instance
    await bpm.DisposeAsync();
  }

  [Fact]
  public async Task FetchPageAsync_ConcurrentNewPageFetches_TriggerMultipleCleanEvictionsCorrectly()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManager = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    // Use a small pool size of 2 to force evictions immediately
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 2 };
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 301;
    var pageId0 = new PageId(tableId, 0); // Will become LRU victim
    var pageId1 = new PageId(tableId, 1); // Will become next victim
    var pageId2 = new PageId(tableId, 2); // New page to fetch
    var pageId3 = new PageId(tableId, 3); // Another new page to fetch

    // Create data for all pages on our "disk"
    var pageDataOnDisk = new Dictionary<PageId, byte[]>
    {
        { pageId0, CreateTestBuffer(0xAA) },
        { pageId1, CreateTestBuffer(0xBB) },
        { pageId2, CreateTestBuffer(0xCC) },
        { pageId3, CreateTestBuffer(0xDD) }
    };
    CreateMultiPageTestFileInMock(mockFileSystem, tableId, pageDataOnDisk);
    string filePath = GetExpectedTablePath(tableId);

    // --- Pre-populate the buffer pool and set LRU order ---
    // 1. Fetch P0 and P1 to fill the pool
    var p0 = await bpm.FetchPageAsync(pageId0);
    // Page 1 becomes MRU, so Page 0 will be LRU and the first to be evicted if unpinned.
    var p1 = await bpm.FetchPageAsync(pageId1);
    Assert.NotNull(p0);
    Assert.NotNull(p1);

    // 2. Unpin both pages 0 and 1 so they are eligible for eviction.
    await bpm.UnpinPageAsync(pageId0, isDirty: false);
    await bpm.UnpinPageAsync(pageId1, isDirty: false);

    // 3. Reset disk I/O counters after setup
    mockFileSystem.ResetReadFileCallCount(filePath);

    // --- Prepare concurrent tasks to fetch new pages ---
    var fetchTasks = new List<Task<Page>>
    {
      Task.Run(() => bpm.FetchPageAsync(pageId2)),
      Task.Run(() => bpm.FetchPageAsync(pageId3))
    };

    // Act
    Page[] results = await Task.WhenAll(fetchTasks);

    // Assert
    // 1. Verify both new pages were loaded successfully
    Assert.Equal(2, results.Length);
    Assert.All(results, Assert.NotNull);

    var p2_result = results.FirstOrDefault(p => p?.Id == pageId2);
    var p3_result = results.FirstOrDefault(p => p?.Id == pageId3);
    Assert.NotNull(p2_result);
    Assert.NotNull(p3_result);
    Assert.True(p2_result.Data.Span.SequenceEqual(pageDataOnDisk[pageId2]));
    Assert.True(p3_result.Data.Span.SequenceEqual(pageDataOnDisk[pageId3]));

    // 2. Verify disk I/O counts
    // Should have read P2 and P3 once each.
    Assert.Equal(2, mockFileSystem.GetReadFileCallCount(filePath));
    // Should NOT have written P0 or P1 as they were clean.
    Assert.Equal(0, mockFileSystem.GetWriteFileCallCount(filePath));

    // 3. Verify final pin counts and eviction state using test hooks
#if DEBUG
    // P2 and P3 should be in the pool, pinned once each
    var frame2 = bpm.GetFrameByPageId_TestOnly(pageId2);
    var frame3 = bpm.GetFrameByPageId_TestOnly(pageId3);
    Assert.NotNull(frame2);
    Assert.NotNull(frame3);
    Assert.Equal(1, frame2.PinCount);
    Assert.Equal(1, frame3.PinCount);

    // P0 and P1 should have been evicted and no longer be in the cache
    var frame0 = bpm.GetFrameByPageId_TestOnly(pageId0);
    var frame1 = bpm.GetFrameByPageId_TestOnly(pageId1);
    Assert.Null(frame0);
    Assert.Null(frame1);
#endif

    // Cleanup local BPM instance
    await bpm.DisposeAsync();
  }

  [Fact]
  public async Task FetchPageAsync_ConcurrentNewPageFetches_TriggerMultipleDirtyEvictionsAndFlushes()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManager = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    // Use a small pool size of 2 to force evictions
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 2 };
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 402;
    var pageId0 = new PageId(tableId, 0); // Will become LRU victim, dirty
    var pageId1 = new PageId(tableId, 1); // Will become next victim, dirty
    var pageId2 = new PageId(tableId, 2); // New page to fetch
    var pageId3 = new PageId(tableId, 3); // Another new page to fetch

    // Create initial data for all pages on our "disk"
    var page0InitialData = CreateTestBuffer(0xAA);
    var page1InitialData = CreateTestBuffer(0xBB);
    var page2InitialData = CreateTestBuffer(0xCC);
    var page3InitialData = CreateTestBuffer(0xDD);

    // Create modified data for the dirty pages
    var page0ModifiedData = CreateTestBuffer(0xA1);
    var page1ModifiedData = CreateTestBuffer(0xB1);

    // Set up the multi-page file in our mock FS
    var initialDiskData = new Dictionary<PageId, byte[]>
    {
        { pageId0, page0InitialData },
        { pageId1, page1InitialData },
        { pageId2, page2InitialData },
        { pageId3, page3InitialData }
    };
    CreateMultiPageTestFileInMock(mockFileSystem, tableId, initialDiskData);
    string filePath = GetExpectedTablePath(tableId);

    // --- Pre-populate the buffer pool and make pages dirty ---
    // 1. Fetch P0 and P1
    Page? p0 = await bpm.FetchPageAsync(pageId0);
    Page? p1 = await bpm.FetchPageAsync(pageId1); // MRU, page 0 is LRU now
    Assert.NotNull(p0);
    Assert.NotNull(p1);

    // 2. Modify their data in memory
    page0ModifiedData.CopyTo(p0.Data);
    page1ModifiedData.CopyTo(p1.Data);

    // 3. Unpin them, mark as dirty
    await bpm.UnpinPageAsync(pageId0, isDirty: true); // LRU
    await bpm.UnpinPageAsync(pageId1, isDirty: true); // MRU

    // 4. Reset disk I/O counters after setup
    mockFileSystem.ResetReadFileCallCount(filePath);

    // --- Prepare concurrent tasks to fetch new pages ---
    var fetchTasks = new List<Task<Page>>
    {
        Task.Run(() => bpm.FetchPageAsync(pageId2)),
        Task.Run(() => bpm.FetchPageAsync(pageId3))
    };

    // Act
    Page[] results = await Task.WhenAll(fetchTasks);

    // Assert
    // 1. Verify both new pages were loaded successfully
    Assert.Equal(2, results.Length);
    Assert.All(results, Assert.NotNull);

    var p2_result = results.FirstOrDefault(p => p?.Id == pageId2);
    var p3_result = results.FirstOrDefault(p => p?.Id == pageId3);
    Assert.NotNull(p2_result);
    Assert.NotNull(p3_result);
    Assert.True(p2_result.Data.Span.SequenceEqual(page2InitialData), "Content of newly fetched P2 is incorrect.");
    Assert.True(p3_result.Data.Span.SequenceEqual(page3InitialData), "Content of newly fetched P3 is incorrect.");

    // 2. Verify disk I/O counts
    // Should have read P2 and P3 once each.
    Assert.Equal(2, mockFileSystem.GetReadFileCallCount(filePath));
    // Should have written P0 and P1 as they were dirty and evicted.
    Assert.Equal(2, mockFileSystem.GetWriteFileCallCount(filePath));

    // 3. Verify the flushed data on "disk" by checking the mock FS directly
    // Check that the file exists in our mock FS's internal state
    Assert.True(mockFileSystem.Files.TryGetValue(filePath, out byte[]? finalFileContent));
    Assert.NotNull(finalFileContent); // Should not be null if file exists

    // Get the slice for page 0 from the mock file's content
    var p0_diskContentAfter = finalFileContent.AsSpan(0, PageSize);
    // Get the slice for page 1 from the mock file's content
    var p1_diskContentAfter = finalFileContent.AsSpan(PageSize, PageSize);

    Assert.True(page0ModifiedData.AsSpan().SequenceEqual(p0_diskContentAfter), "Dirty page P0 was not flushed correctly with modifications.");
    Assert.True(page1ModifiedData.AsSpan().SequenceEqual(p1_diskContentAfter), "Dirty page P1 was not flushed correctly with modifications.");


    // 4. Verify eviction state and pin counts using test hooks
#if DEBUG
    // P2 and P3 should be in the pool, pinned once each
    var frame2 = bpm.GetFrameByPageId_TestOnly(pageId2);
    var frame3 = bpm.GetFrameByPageId_TestOnly(pageId3);
    Assert.NotNull(frame2);
    Assert.NotNull(frame3);
    Assert.Equal(1, frame2.PinCount);
    Assert.Equal(1, frame3.PinCount);

    // P0 and P1 should have been evicted and no longer be in the cache
    var frame0 = bpm.GetFrameByPageId_TestOnly(pageId0);
    var frame1 = bpm.GetFrameByPageId_TestOnly(pageId1);
    Assert.Null(frame0);
    Assert.Null(frame1);
#endif

    // Cleanup local BPM instance
    await bpm.DisposeAsync();
  }

#if DEBUG // This test relies on a debug-only test hook in BufferPoolManager

  [Fact]
  public async Task FetchPageAsync_InterleavedFetchAndUnpinForSamePage_MaintainsCorrectPinCount()
  {
    // Arrange
    var mockFileSystem = new BpmMockFileSystem();
    mockFileSystem.EnsureDirectoryExists(_baseTestDir);

    var diskManager = new DiskManager(mockFileSystem, NullLogger<DiskManager>.Instance, _baseTestDir);
    // Pool size can be small, we're focusing on a single page's lifecycle
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 3 };
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManager, NullLogger<BufferPoolManager>.Instance);

    int tableId = 601;
    var targetPageId = new PageId(tableId, 0);

    // Create the page on our mock "disk"
    CreateMultiPageTestFileInMock(mockFileSystem, tableId, new Dictionary<PageId, byte[]>
    {
        { targetPageId, CreateTestBuffer(0xAB) } // Page 0 filled with 0xAB
    });


    // Act & Assert - Stage 1: Two concurrent initial fetches
    var taskA = Task.Run(() => bpm.FetchPageAsync(targetPageId));
    var taskB = Task.Run(() => bpm.FetchPageAsync(targetPageId));
    Page[] initialResults = await Task.WhenAll(taskA, taskB);

    Assert.All(initialResults, Assert.NotNull); // Ensure both fetches succeeded
    Frame? frame = bpm.GetFrameByPageId_TestOnly(targetPageId);
    Assert.NotNull(frame);
    Assert.Equal(2, frame.PinCount); // <<< PinCount should be 2


    // Act & Assert - Stage 2: One unpin
    await bpm.UnpinPageAsync(targetPageId, isDirty: false);

    Assert.Equal(1, frame.PinCount); // <<< PinCount should be 1


    // Act & Assert - Stage 3: Another fetch
    Page? pageC = await bpm.FetchPageAsync(targetPageId);
    Assert.NotNull(pageC);

    Assert.Equal(2, frame.PinCount); // <<< PinCount should be back to 2


    // Act & Assert - Stage 4: Final unpins
    await bpm.UnpinPageAsync(targetPageId, isDirty: false);
    Assert.Equal(1, frame.PinCount); // <<< PinCount should be 1

    await bpm.UnpinPageAsync(targetPageId, isDirty: false);
    Assert.Equal(0, frame.PinCount); // <<< PinCount should be 0


    // Optional: Final verification that it's now evictable
    var pageToForceEviction1 = new PageId(tableId, 1);
    var pageToForceEviction2 = new PageId(tableId, 2);
    var pageToForceEviction3 = new PageId(tableId, 3);
    CreateMultiPageTestFileInMock(mockFileSystem, tableId, new Dictionary<PageId, byte[]>
    {
        { pageToForceEviction1, CreateTestBuffer(0x01) },
        { pageToForceEviction2, CreateTestBuffer(0x02) },
        { pageToForceEviction3, CreateTestBuffer(0x03) }
    });

    // Fill the rest of the pool and force eviction of our target page
    await bpm.FetchPageAsync(pageToForceEviction1);
    await bpm.FetchPageAsync(pageToForceEviction2);
    await bpm.FetchPageAsync(pageToForceEviction3); // This should evict the targetPageId frame

    var evictedFrame = bpm.GetFrameByPageId_TestOnly(targetPageId);
    Assert.Null(evictedFrame); // Verify it's no longer in the cache


    // Cleanup
    await bpm.DisposeAsync();
  }

#endif

  private void CreateMultiPageTestFileInMock(BpmMockFileSystem fs, int tableId, Dictionary<PageId, byte[]> pageDataMap)
  {
    if (!pageDataMap.Any()) return;

    string filePath = GetExpectedTablePath(tableId);
    // Find the highest page index to determine required file size
    int maxPageIndex = pageDataMap.Keys.Max(p => p.PageIndex);
    var fileContent = new byte[(maxPageIndex + 1) * Page.Size];

    // Copy each page's data into its correct offset in the file content array
    foreach (var (pageId, data) in pageDataMap)
    {
      data.CopyTo(fileContent.AsSpan((int)((long)pageId.PageIndex * Page.Size)));
    }
    // Add the complete file content to the mock file system
    fs.AddFile(filePath, fileContent);
  }
}