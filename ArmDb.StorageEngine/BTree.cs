using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

internal sealed class BTree
{
  private readonly BufferPoolManager _bpm;
  private readonly TableDefinition _tableDefinition;

  internal BTree(BufferPoolManager bmp, TableDefinition tableDefinition)
  {
    _bpm = bmp;
    _tableDefinition = tableDefinition;
  }

  // internal BTree CreateAsync()
  // {

  // }
}