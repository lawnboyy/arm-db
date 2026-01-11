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
    /// Creates a new database by storing database metadata in the sys_databases table.
    /// </summary>
    /// <param name="databaseName">The name of the new database</param>
    /// <returns>The ID of the new database</returns>
    Task<int> CreateDatabaseAsync(string databaseName);

    /// <summary>
    /// Creates the physical storage structures for a new table based on its definition.
    /// </summary>
    Task CreateTableAsync(int databaseId, string tableName, TableDefinition tableDefinition);

    /// <summary>
    /// Gets the schema definition for a table.
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    Task<TableDefinition?> GetTableDefinitionAsync(string tableName);

    /// <summary>
    /// Inserts a single row of data into the specified table.
    /// </summary>
    Task InsertRowAsync(string tableName, Record row);

    /// <summary>
    /// Scans a table optionally constrained by the given min and/or max values and returns an asynchronous enumerator
    /// of the results.
    /// </summary>
    /// <param name="min">The minimum key value.</param>
    /// <param name="minInclusive">The result set will include the minimum key if true, otherwise it is exclued.</param>
    /// <param name="max">The maximum key value.</param>
    /// <param name="maxInclusive">The result set will include the maximum key if true, otherwise it is exclued.</param>
    /// <returns></returns>
    IAsyncEnumerable<Record> ScanAsync(string tableName, Key? min = null, bool minInclusive = false, Key? max = null, bool maxInclusive = false);

    // Add methods for Read, Update, Delete, Scan, etc. later
  }
}