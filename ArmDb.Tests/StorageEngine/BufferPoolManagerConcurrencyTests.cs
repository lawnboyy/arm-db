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
    var controllableFs = new BpmMockFileSystem();
    // DiskManager needs its base directory to exist even in the mock FS if it checks/creates it.
    controllableFs.EnsureDirectoryExists(_baseTestDir);

    var diskManagerWithControllableFs = new DiskManager(controllableFs, NullLogger<DiskManager>.Instance, _baseTestDir);
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 5 }; // Sufficiently large pool
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManagerWithControllableFs, NullLogger<BufferPoolManager>.Instance);

    int tableId = 501;
    var targetPageId = new PageId(tableId, 0);
    string filePath = GetExpectedTablePath(tableId); // Helper to get consistent path based on _baseTestDir
    byte[] pageDataOnDisk = CreateTestBuffer(0xAA); // Creates a PageSize buffer filled with 0xAA

    // Add the file content to our controllable file system
    controllableFs.AddFileContent(filePath, pageDataOnDisk);
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
    var readStats = controllableFs as BpmMockFileSystem; // Cast if needed, or ensure your _fileSystem field in the test class is already the controllable one
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
#if TEST_ONLY
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
    var controllableFs = new BpmMockFileSystem();
    controllableFs.EnsureDirectoryExists(_baseTestDir);

    var diskManager = new DiskManager(controllableFs, NullLogger<DiskManager>.Instance, _baseTestDir);
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
    CreateMultiPageTestFileInMock(controllableFs, tableId, pageDataOnDisk);

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
    Assert.Equal(numConcurrentPages, controllableFs.GetReadFileCallCount(filePath));

    // 6. Verify each page is pinned exactly once
#if TEST_ONLY
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

  private void CreateMultiPageTestFileInMock(BpmMockFileSystem fs, int tableId, Dictionary<PageId, byte[]> pageDataMap)
  {
    if (!pageDataMap.Any()) return;

    string filePath = GetExpectedTablePath(tableId);
    int maxPageIndex = pageDataMap.Keys.Max(p => p.PageIndex);
    var fileContent = new byte[(maxPageIndex + 1) * Page.Size];

    foreach (var (pageId, data) in pageDataMap)
    {
      data.CopyTo(fileContent.AsSpan((int)((long)pageId.PageIndex * Page.Size)));
    }
    fs.AddFileContent(filePath, fileContent);
  }
}