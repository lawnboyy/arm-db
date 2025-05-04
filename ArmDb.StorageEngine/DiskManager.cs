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
  private string _baseDataDirectory; // Set during Initialize
  private bool _initialized;

  // Define the file extension used for table data files
  internal const string TableFileExtension = ".tbl";

  /// <summary>
  /// Initializes a new instance of the <see cref="DiskManager"/> class.
  /// </summary>
  /// <param name="fileSystem">The file system abstraction implementation.</param>
  /// <param name="logger">The logger instance.</param>
  /// <exception cref="ArgumentNullException">Thrown if fileSystem or logger is null.</exception>
  internal DiskManager(IFileSystem fileSystem, ILogger<DiskManager> logger)
  {
    ArgumentNullException.ThrowIfNull(fileSystem);
    ArgumentNullException.ThrowIfNull(logger);

    _fileSystem = fileSystem;
    _logger = logger;
    _pageSize = Page.Size; // Cache page size from Page class definition

    // Initial state: not initialized, no base path set.
    _initialized = false;
    _baseDataDirectory = string.Empty; // Or null, requires null checks later

    _logger.LogTrace("DiskManager instance created. Call Initialize() before use.");
  }

  // --- Initialize Method (To be added next) ---
  // public void Initialize(string baseDataDirectory) { ... }

  // --- Other methods (GetTableFilePath, ReadDiskPageAsync etc.) to follow ---

  // --- Private Helper for file paths (can be added now) ---
  private string GetTableFilePath(int tableId)
  {
    // This helper should only be called after initialization is complete
    if (!_initialized || string.IsNullOrEmpty(_baseDataDirectory))
    {
      throw new InvalidOperationException("DiskManager has not been initialized. Call Initialize first.");
    }
    // Consider validating tableId? Usually done by higher layers.
    // Format: [BaseDataDirectory]/[TableId].tbl
    return _fileSystem.CombinePath(_baseDataDirectory, $"{tableId}{TableFileExtension}");
  }
}
