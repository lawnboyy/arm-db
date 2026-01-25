using ArmDb.Storage;

namespace ArmDb.Tests.Unit.Storage;

public partial class DiskManagerTests : IDisposable
{
  [Fact]
  public async Task AllocateNewDiskPageAsync_ForNewTable_ReturnsPageIndexZeroAndExtendsFile()
  {
    // Arrange
    int tableId = 201;
    var filePath = GetExpectedTablePath(tableId);
    Assert.False(_fileSystem.FileExists(filePath)); // Ensure file doesn't exist initially

    // Act
    PageId newPageId = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    Assert.Equal(tableId, newPageId.TableId);
    Assert.Equal(0, newPageId.PageIndex); // First page should be index 0

    // Verify file was created and extended to hold one page
    Assert.True(_fileSystem.FileExists(filePath));
    Assert.Equal(PageSize, await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_ForExistingOnePageTable_ReturnsPageIndexOneAndExtendsFile()
  {
    // Arrange
    int tableId = 202;
    // Create a file with exactly one page
    await CreateTestTableFileAsync(tableId, 1, p => (byte)p);
    var filePath = GetExpectedTablePath(tableId);
    Assert.Equal(PageSize, await _fileSystem.GetFileLengthAsync(filePath));

    // Act
    PageId newPageId = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    Assert.Equal(tableId, newPageId.TableId);
    Assert.Equal(1, newPageId.PageIndex); // Next page should be index 1

    // Verify file was extended to hold two pages
    Assert.Equal(PageSize * 2, await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_ForExistingMultiPageTable_ReturnsNextPageIndexAndExtendsFile()
  {
    // Arrange
    int tableId = 203;
    int initialPages = 5;
    await CreateTestTableFileAsync(tableId, initialPages, p => (byte)p);
    var filePath = GetExpectedTablePath(tableId);
    Assert.Equal((long)PageSize * initialPages, await _fileSystem.GetFileLengthAsync(filePath));

    // Act
    PageId newPageId = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    Assert.Equal(tableId, newPageId.TableId);
    Assert.Equal(initialPages, newPageId.PageIndex); // Next page index should be initial count

    // Verify file was extended to hold one more page
    Assert.Equal((long)PageSize * (initialPages + 1), await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_ForNonPageAlignedFile_ReturnsNextPageIndexAndExtendsFileCorrectly()
  {
    // Arrange
    int tableId = 204;
    var filePath = GetExpectedTablePath(tableId);
    // Create a file with non-standard length (e.g., 1.5 pages)
    long initialLength = (long)(PageSize * 1.5);
    await using (var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
    {
      fs.SetLength(initialLength);
    }
    Assert.Equal(initialLength, await _fileSystem.GetFileLengthAsync(filePath));

    // Act
    PageId newPageId = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    // Integer division of length / PageSize gives the index of the *last full page*
    // So the next page index should be (int)(initialLength / PageSize) + 1 ? No, just int division.
    // Example: 12288 / 8192 = 1. The next page index is 1.
    // Example: 8193 / 8192 = 1. The next page index is 1.
    int expectedNextIndex = (int)(initialLength / PageSize);
    Assert.Equal(tableId, newPageId.TableId);
    Assert.Equal(expectedNextIndex, newPageId.PageIndex); // Next index based on integer division

    // Verify file was extended to hold the allocated page fully
    long expectedFinalLength = (long)(expectedNextIndex + 1) * PageSize;
    Assert.Equal(expectedFinalLength, await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_CalledTwice_ReturnsIncrementingIndices()
  {
    // Arrange
    int tableId = 205;
    var filePath = GetExpectedTablePath(tableId);
    Assert.False(_fileSystem.FileExists(filePath));

    // Act
    PageId pageId0 = await _diskManager.AllocateNewDiskPageAsync(tableId);
    PageId pageId1 = await _diskManager.AllocateNewDiskPageAsync(tableId);

    // Assert
    Assert.Equal(0, pageId0.PageIndex);
    Assert.Equal(1, pageId1.PageIndex);
    Assert.Equal(tableId, pageId0.TableId);
    Assert.Equal(tableId, pageId1.TableId);
    Assert.Equal(PageSize * 2, await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_ConcurrentCallsForSameTable_ReturnsUniqueAndSequentialPageIndices()
  {
    // Arrange
    int tableId = 301; // A unique table ID for this test
    int numberOfAllocations = 5; // How many pages to allocate concurrently
    var allocationTasks = new List<Task<PageId>>(numberOfAllocations);
    var filePath = GetExpectedTablePath(tableId);

    // Ensure a clean state for the file (optional, as AllocateNew... should create it)
    if (_fileSystem.FileExists(filePath))
    {
      await _fileSystem.DeleteFileAsync(filePath);
    }

    // Act
    // Launch all allocation tasks without awaiting them individually
    for (int i = 0; i < numberOfAllocations; i++)
    {
      allocationTasks.Add(_diskManager.AllocateNewDiskPageAsync(tableId));
    }

    // Now await all tasks to complete
    PageId[] allocatedPageIds = await Task.WhenAll(allocationTasks);

    // Assert
    // 1. Check if all tasks completed and we got the expected number of PageIds
    Assert.Equal(numberOfAllocations, allocatedPageIds.Length);

    // 2. Extract PageIndex values and check for uniqueness
    var pageIndices = allocatedPageIds.Select(pid => pid.PageIndex).ToList();
    Assert.Equal(numberOfAllocations, pageIndices.Distinct().Count());

    // 3. Check for sequential PageIndex values starting from 0
    pageIndices.Sort(); // Sort them to check sequence
    for (int i = 0; i < numberOfAllocations; i++)
    {
      Assert.Equal(i, pageIndices[i]);
    }

    // 4. Verify all PageIds are for the correct table
    Assert.All(allocatedPageIds, pid => Assert.Equal(tableId, pid.TableId));

    // 5. Verify the final file length on disk
    Assert.True(_fileSystem.FileExists(filePath));
    long expectedFileLength = (long)numberOfAllocations * PageSize;
    Assert.Equal(expectedFileLength, await _fileSystem.GetFileLengthAsync(filePath));
  }

  [Fact]
  public async Task AllocateNewDiskPageAsync_ConcurrentCallsForDifferentTables_SucceedsIndependently()
  {
    // Arrange
    int tableId1 = 401;
    int tableId2 = 402;
    int allocationsPerTable = 3;
    var tasks = new List<Task<PageId>>();

    // Ensure clean state
    if (_fileSystem.FileExists(GetExpectedTablePath(tableId1))) await _fileSystem.DeleteFileAsync(GetExpectedTablePath(tableId1));
    if (_fileSystem.FileExists(GetExpectedTablePath(tableId2))) await _fileSystem.DeleteFileAsync(GetExpectedTablePath(tableId2));

    // Act: Interleave calls for different tables
    for (int i = 0; i < allocationsPerTable; i++)
    {
      tasks.Add(_diskManager.AllocateNewDiskPageAsync(tableId1));
      tasks.Add(_diskManager.AllocateNewDiskPageAsync(tableId2));
    }
    PageId[] results = await Task.WhenAll(tasks);

    // Assert for tableId1
    var table1PageIds = results.Where(pid => pid.TableId == tableId1).Select(pid => pid.PageIndex).OrderBy(idx => idx).ToList();
    Assert.Equal(allocationsPerTable, table1PageIds.Count);
    for (int i = 0; i < allocationsPerTable; i++) Assert.Equal(i, table1PageIds[i]);
    Assert.Equal((long)allocationsPerTable * PageSize, await _fileSystem.GetFileLengthAsync(GetExpectedTablePath(tableId1)));

    // Assert for tableId2
    var table2PageIds = results.Where(pid => pid.TableId == tableId2).Select(pid => pid.PageIndex).OrderBy(idx => idx).ToList();
    Assert.Equal(allocationsPerTable, table2PageIds.Count);
    for (int i = 0; i < allocationsPerTable; i++) Assert.Equal(i, table2PageIds[i]);
    Assert.Equal((long)allocationsPerTable * PageSize, await _fileSystem.GetFileLengthAsync(GetExpectedTablePath(tableId2)));
  }
}