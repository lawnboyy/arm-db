using ArmDb.Common.Abstractions;
using ArmDb.Common.Utils;
using ArmDb.StorageEngine;
using Microsoft.Extensions.Logging.Abstractions;

namespace ArmDb.UnitTests.StorageEngine;

public partial class DiskManagerTests : IDisposable
{
  private readonly IFileSystem _fileSystem;
  private readonly DiskManager _diskManager;
  private readonly string _baseTestDir;
  private const int PageSize = Page.Size; // Use defined page size
  private readonly byte[] _sampleData; // Reusable test data

  public DiskManagerTests() // Constructor name matches class
  {
    _fileSystem = new FileSystem();
    _baseTestDir = Path.Combine(Path.GetTempPath(), $"ArmDb_DM_Tests_{Guid.NewGuid()}");
    var logger = NullLogger<DiskManager>.Instance;
    _diskManager = new DiskManager(_fileSystem, logger, _baseTestDir);
    _sampleData = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray(); // Initialize sample data
  }

  [Fact]
  public async Task ReadDiskPageAsync_ReadsExistingPageZero_ReturnsCorrectData()
  {
    // Arrange
    int tableId = 1;
    var pageId = new PageId(tableId, 0);
    byte pageFillValue = 0xAA;
    await CreateTestTableFileAsync(tableId, 1, p => pageFillValue);
    var buffer = new Memory<byte>(new byte[PageSize]);

    // Act
    int bytesRead = await _diskManager.ReadDiskPageAsync(pageId, buffer);

    // Assert
    Assert.Equal(PageSize, bytesRead);
    Assert.True(buffer.Span.ToArray().All(b => b == pageFillValue), "Buffer content mismatch");
  }

  [Fact]
  public async Task ReadDiskPageAsync_ReadsExistingMiddlePage_ReturnsCorrectData()
  {
    // Arrange
    int tableId = 2;
    int targetPageIndex = 2;
    var pageId = new PageId(tableId, targetPageIndex);
    await CreateTestTableFileAsync(tableId, 5, p => (byte)(p + 1));
    var buffer = new Memory<byte>(new byte[PageSize]);
    byte expectedFillValue = (byte)(targetPageIndex + 1);

    // Act
    int bytesRead = await _diskManager.ReadDiskPageAsync(pageId, buffer);

    // Assert
    Assert.Equal(PageSize, bytesRead);
    Assert.True(buffer.Span.ToArray().All(b => b == expectedFillValue), $"Buffer not filled with {expectedFillValue}");
  }

  [Fact]
  public async Task ReadDiskPageAsync_BufferNotPageSize_ThrowsArgumentException()
  {
    // Arrange
    int tableId = 3;
    var pageId = new PageId(tableId, 0);
    await CreateTestTableFileAsync(tableId, 1, p => 0xCC);
    var smallBuffer = new Memory<byte>(new byte[PageSize - 10]);
    var largeBuffer = new Memory<byte>(new byte[PageSize + 10]);

    // Act & Assert
    await Assert.ThrowsAsync<ArgumentException>("buffer", () => _diskManager.ReadDiskPageAsync(pageId, smallBuffer));
    await Assert.ThrowsAsync<ArgumentException>("buffer", () => _diskManager.ReadDiskPageAsync(pageId, largeBuffer));
  }

  [Fact]
  public async Task ReadDiskPageAsync_TableFileNotFound_ThrowsFileNotFoundException()
  {
    // Arrange
    int tableId = 99;
    var pageId = new PageId(tableId, 0);
    var buffer = new Memory<byte>(new byte[PageSize]);
    Assert.False(_fileSystem.FileExists(GetExpectedTablePath(tableId)));

    // Act & Assert
    await Assert.ThrowsAsync<FileNotFoundException>(() =>
        _diskManager.ReadDiskPageAsync(pageId, buffer));
  }

  [Fact]
  public async Task ReadDiskPageAsync_PageIndexOutOfBounds_ThrowsIOExceptionDueToShortRead()
  {
    // Arrange
    int tableId = 5;
    await CreateTestTableFileAsync(tableId, 2, p => (byte)(p + 1));
    var pageId = new PageId(tableId, 2); // Request page index 2 (valid indices 0, 1)
    var buffer = new Memory<byte>(new byte[PageSize]);

    // Act & Assert
    await Assert.ThrowsAsync<IOException>(() =>
        _diskManager.ReadDiskPageAsync(pageId, buffer));
  }

  // --- IDisposable Implementation for Cleanup ---
  private bool _disposed = false;
  public void Dispose()
  {
    Dispose(true);
    GC.SuppressFinalize(this);
  }

  protected virtual void Dispose(bool disposing)
  {
    if (!_disposed)
    {
      try { if (Directory.Exists(_baseTestDir)) { Directory.Delete(_baseTestDir, recursive: true); } }
      catch (Exception ex) { Console.WriteLine($"Error cleaning up test directory '{_baseTestDir}': {ex.Message}"); }
      _disposed = true;
    }
  }

  // Finalizer matches the new class name
  ~DiskManagerTests()
  {
    Dispose(disposing: false);
  }

  private string GetExpectedTablePath(int tableId) => Path.Combine(_baseTestDir, $"{tableId}{DiskManager.TableFileExtension}");

  // Helper to create a test file with specific page content
  private async Task CreateTestTableFileAsync(int tableId, int numPages, Func<int, byte> pageContentPattern)
  {
    var filePath = GetExpectedTablePath(tableId);
    await using (var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, PageSize, useAsync: true))
    {
      if (numPages > 0)
      {
        fs.SetLength((long)numPages * PageSize);
        for (int p = 0; p < numPages; p++)
        {
          fs.Position = (long)p * PageSize;
          byte[] pageData = new byte[PageSize];
          byte fillValue = pageContentPattern(p);
          Array.Fill(pageData, fillValue);
          await fs.WriteAsync(pageData, 0, PageSize);
        }
      }
      else
      {
        fs.SetLength(0);
      }
    }
  }
}