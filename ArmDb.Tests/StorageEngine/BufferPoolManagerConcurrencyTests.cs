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
}