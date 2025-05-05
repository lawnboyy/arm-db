using ArmDb.Common.Abstractions;
using Microsoft.Extensions.Logging;

namespace ArmDb.StorageEngine;
/// <summary>
/// Manages the persistence of pages to disk files for specific tables.
/// Assumes a file-per-table storage strategy where each table's data
/// resides in a file named "[TableId].tbl".
/// Responsible for mapping PageIds to file offsets and performing raw page I/O via IFileSystem.
/// </summary>
internal sealed class DiskManager
{
  private readonly IFileSystem _fileSystem;
  private readonly ILogger<DiskManager> _logger;
  private readonly int _pageSize;
  private readonly string _baseDataDirectory; // Set in constructor and readonly

  // File extension for table data files
  internal const string TableFileExtension = ".tbl";

  /// <summary>
  /// Initializes a new instance of the DiskManager class, ensuring the base data directory exists.
  /// </summary>
  /// <param name="fileSystem">The file system abstraction implementation.</param>
  /// <param name="logger">The logger instance.</param>
  /// <param name="baseDataDirectory">The root directory where all database table files will reside.</param>
  /// <exception cref="ArgumentNullException">Thrown if fileSystem, logger, or baseDataDirectory is null.</exception>
  /// <exception cref="ArgumentException">Thrown if baseDataDirectory is empty or whitespace, or represents an invalid path.</exception>
  /// <exception cref="IOException">Propagated if directory creation fails due to I/O errors.</exception>
  /// <exception cref="UnauthorizedAccessException">Propagated if permissions are insufficient to access or create the directory.</exception>
  /// <exception cref="NotSupportedException">Propagated if the path format is not supported.</exception>
  /// <exception cref="PathTooLongException">Propagated if the path exceeds system limits.</exception>
  /// <exception cref="InvalidOperationException">Wraps underlying exceptions during directory validation/creation.</exception>
  internal DiskManager(IFileSystem fileSystem, ILogger<DiskManager> logger, string baseDataDirectory)
  {
    ArgumentNullException.ThrowIfNull(fileSystem);
    ArgumentNullException.ThrowIfNull(logger);
    ArgumentException.ThrowIfNullOrWhiteSpace(baseDataDirectory);

    _fileSystem = fileSystem;
    _logger = logger;
    _pageSize = Page.Size; // Cache page size from Page class definition

    try
    {
      // Normalize the path (resolve relative paths, ., ..) and store it
      // Path.GetFullPath can throw exceptions for invalid chars/formats etc.
      string absolutePath = Path.GetFullPath(baseDataDirectory);
      _baseDataDirectory = absolutePath;

      _logger.LogDebug("Ensuring base data directory exists at: {Directory}", _baseDataDirectory);

      // Ensure the directory exists (synchronous call via interface)
      // This will create it if it doesn't exist.
      _fileSystem.EnsureDirectoryExists(_baseDataDirectory);

      // If we reach here, initialization was successful
      _logger.LogInformation("DiskManager initialized. Base data directory set to: {Directory}", _baseDataDirectory);
    }
    catch (Exception ex) when (ex is not (IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException or PathTooLongException))
    {
      // Catch unexpected exceptions during path resolution or directory creation
      _logger.LogCritical(ex, "Unexpected error during DiskManager initialization with base directory '{Directory}'.", baseDataDirectory);
      throw new InvalidOperationException($"Unexpected error initializing DiskManager with base directory '{baseDataDirectory}'. See inner exception.", ex);
    }
    // Let expected exceptions (IO, Unauthorized, Argument, etc.) from EnsureDirectoryExists or GetFullPath propagate directly
  }

  /// <summary>
  /// Checks if the data file for the specified table ID exists.
  /// </summary>
  /// <param name="tableId">The ID of the table.</param>
  /// <returns>True if the file exists, false otherwise.</returns>
  internal bool TableFileExists(int tableId)
  {
    string filePath = GetTableFilePath(tableId);
    return _fileSystem.FileExists(filePath);
  }

  /// <summary>
  /// Ensures the physical file for a table exists, creating an empty one if necessary.
  /// </summary>
  /// <param name="tableId">The ID of the table.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  internal Task CreateTableFileAsync(int tableId)
  {
    string filePath = GetTableFilePath(tableId);
    // WriteFileAsync with OpenOrCreate handles file creation implicitly when writing 0 bytes.
    _logger.LogDebug("Ensuring table file exists for TableId {TableId} at {Path}", tableId, filePath);
    return _fileSystem.WriteFileAsync(filePath, 0, ReadOnlyMemory<byte>.Empty);
  }

  /// <summary>
  /// Reads a specific page (Page.Size bytes) from the corresponding table file on disk
  /// into the provided buffer.
  /// </summary>
  /// <param name="pageId">The PageId (TableId, PageIndex) to read.</param>
  /// <param name="buffer">The memory buffer to read data into. Must be exactly Page.Size.</param>
  /// <returns>Task representing the async operation, returning the number of bytes read (should equal Page.Size on success).</returns>
  /// <exception cref="ArgumentException">Thrown if buffer size is incorrect.</exception>
  /// <exception cref="System.IO.FileNotFoundException">Propagated if the table file does not exist.</exception>
  /// <exception cref="System.IO.IOException">
  /// Thrown on I/O errors, or if the read operation encounters an unexpected end-of-file
  /// (indicating potential file corruption or truncation as a full page was expected).
  /// </exception>
  /// <exception cref="System.UnauthorizedAccessException">Propagated if permissions are insufficient.</exception>
  internal async Task<int> ReadDiskPageAsync(PageId pageId, Memory<byte> buffer)
  {
    // 1. Validate Buffer Size
    if (buffer.Length != _pageSize)
    {
      throw new ArgumentException($"Read buffer must be exactly Page Size ({_pageSize} bytes), but was {buffer.Length} bytes.", nameof(buffer));
    }
    // PageId validity (PageIndex >= 0) assumed handled by PageId constructor

    // 2. Determine File Path and Offset
    string filePath = GetTableFilePath(pageId.TableId);
    long fileOffset = (long)pageId.PageIndex * _pageSize;

    _logger.LogTrace("Attempting to read disk page {PageId} from {File} at offset {Offset}", pageId, filePath, fileOffset);

    int bytesRead;
    try
    {
      // 3. Perform Read using IFileSystem
      bytesRead = await _fileSystem.ReadFileAsync(filePath, fileOffset, buffer);

      // 4. Check for Short Reads (Critical Error)
      // If we asked for a specific PageId, we expect a full page unless it's truly the EOF
      // marker which ReadFileAsync might return 0 for. Reading less than a page size
      // when expecting a page implies file corruption/truncation.
      if (bytesRead < _pageSize)
      {
        // Check if we were trying to read at/past the actual EOF, which might return 0 legitimately
        // However, if we request PageIndex 5, we expect bytesRead==PageSize or an exception.
        // Reading 0 or < PageSize means the file isn't as long as PageIndex suggests it should be.
        _logger.LogError("Short read for {PageId} in file {File}. Expected {ExpectedSize} bytes, but read {ActualRead} bytes. File may be corrupted or truncated.",
            pageId, filePath, _pageSize, bytesRead);

        // Throw a specific error indicating page data inconsistency
        throw new IOException($"Short read for page {pageId}. Expected {_pageSize} bytes but read only {bytesRead}. File may be corrupted or does not contain the requested page index.");
      }

      _logger.LogTrace("Successfully read {BytesRead} bytes for {PageId}", bytesRead, pageId);
      return bytesRead; // Should always be == _pageSize if no exception thrown
    }
    catch (FileNotFoundException fnfEx)
    {
      _logger.LogError(fnfEx, "Table file not found when trying to read page {PageId} from {File}", pageId, filePath);
      // Let specific, informative exceptions propagate
      throw;
    }
    catch (IOException ioEx) // Catch other IO errors
    {
      _logger.LogError(ioEx, "I/O Error reading page {PageId} from {File} at offset {Offset}", pageId, filePath, fileOffset);
      throw; // Propagate
    }
    catch (Exception ex) // Catch unexpected errors
    {
      _logger.LogCritical(ex, "Unexpected error reading page {PageId} from {File} at offset {Offset}", pageId, filePath, fileOffset);
      // Wrap in a storage-specific exception? For now, rethrow.
      throw new InvalidOperationException($"An unexpected error occurred while reading page {pageId}.", ex);
    }
  }

  /// <summary>
  /// Writes a specific page's data from a buffer to the corresponding table file on disk.
  /// </summary>
  /// <param name="pageId">The PageId (TableId, PageIndex) to write.</param>
  /// <param name="buffer">The read-only memory buffer containing the page data to write (must be Page.Size).</param>
  /// <returns>Task representing the async operation.</returns>
  /// <exception cref="ArgumentException">Thrown if buffer size is incorrect.</exception>
  internal Task WriteDiskPageAsync(PageId pageId, ReadOnlyMemory<byte> buffer)
  {
    if (buffer.Length != _pageSize)
      throw new ArgumentException($"Buffer must be Page Size ({_pageSize} bytes).", nameof(buffer));

    string filePath = GetTableFilePath(pageId.TableId);
    long fileOffset = (long)pageId.PageIndex * _pageSize;
    _logger.LogTrace("Writing disk page: {PageId} to {File} at offset {Offset}", pageId, filePath, fileOffset);
    // Delegate directly to IFileSystem implementation
    return _fileSystem.WriteFileAsync(filePath, fileOffset, buffer);
  }

  /// <summary>
  /// Allocates space for a new page for the given table, extending the file if necessary,
  /// and returns the PageId for the newly allocated page.
  /// </summary>
  /// <param name="tableId">The ID of the table to allocate a page for.</param>
  /// <returns>The PageId of the newly allocated page.</returns>
  internal async Task<PageId> AllocateNewDiskPageAsync(int tableId)
  {
    string filePath = GetTableFilePath(tableId);
    _logger.LogTrace("Allocating new disk page for table {TableId} in file {File}", tableId, filePath);

    long currentLength;
    try
    {
      // Need the length to determine the next page index
      currentLength = await _fileSystem.GetFileLengthAsync(filePath);
    }
    catch (FileNotFoundException)
    {
      _logger.LogWarning("File {File} not found during page allocation for table {TableId}. Assuming new file (length 0). CreateTableFileAsync should ideally be called first.", filePath, tableId);
      currentLength = 0; // Treat as empty/new file
    }

    // Calculate next index. Integer division handles alignment.
    int nextPageIndex = (int)(currentLength / _pageSize);
    // If file length is not a multiple of page size, something is wrong, but
    // we'll allocate starting after the last full page for recovery perhaps.
    if (currentLength % _pageSize != 0)
    {
      _logger.LogWarning("File {File} size ({Length}) is not a multiple of page size ({PageSize}). Potential corruption or incomplete write.", filePath, currentLength, _pageSize);
      // Still allocate based on integer division, essentially allocating the potentially partial page.
    }

    // Pre-extend the file to ensure space (recommended)
    long requiredLength = (long)(nextPageIndex + 1) * _pageSize;
    if (currentLength < requiredLength)
    {
      _logger.LogTrace("Extending file {File} to {Length} bytes for new page index {PageIndex}", filePath, requiredLength, nextPageIndex);
      await _fileSystem.SetFileLengthAsync(filePath, requiredLength);
    }

    var newPageId = new PageId(tableId, nextPageIndex);
    _logger.LogDebug("Allocated new page identifier {PageId}", newPageId);
    return newPageId;
  }

  // --- Private Helper for file paths (Check removed) ---
  private string GetTableFilePath(int tableId)
  {
    // Constructor ensures _baseDataDirectory is initialized
    return _fileSystem.CombinePath(_baseDataDirectory, $"{tableId}{TableFileExtension}");
  }

  // Add DeleteTableFileAsync later...
}