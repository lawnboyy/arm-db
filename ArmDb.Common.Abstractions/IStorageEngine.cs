using ArmDb.SchemaDefinition;
using ArmDb.DataModel;

namespace ArmDb.Common.Abstractions
{
  /// <summary>
  /// Defines the essential public contract for the Storage Engine component,
  /// responsible for managing physical data storage, pages, records, and indexes.
  /// </summary>
  public interface IStorageEngine : IAsyncDisposable
  {
    /// <summary>
    /// Initializes the storage engine, setting the root data directory,
    /// initializing the buffer pool, and preparing necessary resources.
    /// Must be called once before other operations.
    /// </summary>
    Task InitializeAsync(string dataDirectoryPath, bool ensureDirectoryExists = true);

    /// <summary>
    /// Checks if the physical storage artifacts for a table with the given name exist.
    /// </summary>
    Task<bool> TableExistsAsync(string tableName);

    /// <summary>
    /// Creates the physical storage structures for a new table based on its definition.
    /// </summary>
    Task CreateTableAsync(TableDefinition tableDefinition);

    /// <summary>
    /// Inserts a single row of data into the specified table.
    /// </summary>
    Task InsertRowAsync(string tableName, Record row);

    // Add methods for Read, Update, Delete, Scan, etc. later
  }
}