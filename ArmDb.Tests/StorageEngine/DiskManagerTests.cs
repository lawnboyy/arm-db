using ArmDb.Common.Abstractions;
using ArmDb.Common.Utils;
using ArmDb.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace ArmDb.UnitTests.StorageEngine; // Your test project's namespace

// Note: These are integration tests hitting the real file system.
public partial class DiskManagerTests : IDisposable
{
  // --- Fields (Keep at top) ---
  private readonly IFileSystem _fileSystem;
  private readonly DiskManager _diskManager;
  private readonly string _baseTestDir;
  private const int PageSize = Page.Size; // Use defined page size
  private readonly byte[] _sampleData; // Reusable test data

  // --- Constructor (Setup) ---
  public DiskManagerTests()
  {
    _fileSystem = new FileSystem();
    _baseTestDir = Path.Combine(Path.GetTempPath(), $"ArmDb_DM_Tests_{Guid.NewGuid()}");
    var logger = NullLogger<DiskManager>.Instance;
    _diskManager = new DiskManager(_fileSystem, logger, _baseTestDir);
    _sampleData = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray(); // Initialize sample data
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

  private string GetTestPath(string fileName) => Path.Combine(_baseTestDir, fileName);

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