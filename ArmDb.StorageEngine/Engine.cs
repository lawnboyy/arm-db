using System.Collections.Concurrent;
using ArmDb.Common.Abstractions;
using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using Microsoft.Extensions.Logging;

namespace ArmDb.Storage;

internal sealed class StorageEngine : IStorageEngine
{
  private readonly BufferPoolManager _bpm;
  private readonly ILogger<StorageEngine> _logger;
  private readonly ConcurrentDictionary<string, BTree> _tables = new();
  private readonly ConcurrentDictionary<string, TableDefinition> _tableDefinitions = new();


  internal StorageEngine(BufferPoolManager bpm, ILogger<StorageEngine> logger)
  {
    _bpm = bpm;
    _logger = logger;
  }

  public async Task CreateTableAsync(string tableName, TableDefinition tableDef)
  {
    // Instantiate a new B-Tree for this table...
    var btree = await BTree.CreateAsync(_bpm, tableDef);
    // Add the B-Tree to the tables lookup...
    _tables[tableName] = btree;
    // Add the table schema to our table definition lookup...
    _tableDefinitions[tableName] = tableDef;
  }

  public async Task<TableDefinition> GetTableDefinitionAsync(string tableName)
  {
    return _tableDefinitions[tableName];
  }

  public ValueTask DisposeAsync()
  {
    return new ValueTask();
  }

  public async Task InsertRowAsync(string tableName, Record row)
  {
    // Lookup the B-Tree for this table...
    var btree = _tables[tableName];
    // Insert the record...
    await btree.InsertAsync(row);
  }

  public async IAsyncEnumerable<Record> ScanAsync(string tableName, Key? min = null, bool minInclusive = false, Key? max = null, bool maxInclusive = false)
  {
    // Lookup the table...
    var btree = _tables[tableName];

    await foreach (var row in btree.ScanAsync(min, minInclusive, max, maxInclusive))
    {
      yield return row;
    }
  }
}

