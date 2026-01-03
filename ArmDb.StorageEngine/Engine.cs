using System.Collections.Concurrent;
using ArmDb.Common.Abstractions;
using ArmDb.SchemaDefinition;
using Microsoft.Extensions.Logging;

namespace ArmDb.StorageEngine;

internal sealed class Engine : IStorageEngine
{
  private readonly BufferPoolManager _bpm;
  private readonly ILogger<Engine> _logger;
  private readonly ConcurrentDictionary<string, BTree> _tables = new();
  private readonly ConcurrentDictionary<string, TableDefinition> _tableDefinitions = new();


  internal Engine(BufferPoolManager bpm, ILogger<Engine> logger)
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
}

