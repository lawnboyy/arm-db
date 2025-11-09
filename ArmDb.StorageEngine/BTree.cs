using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

internal sealed class BTree
{
  private readonly BufferPoolManager _bpm;
  private readonly TableDefinition _tableDefinition;

  private BTree(BufferPoolManager bmp, TableDefinition tableDefinition)
  {
    _bpm = bmp;
    _tableDefinition = tableDefinition;
  }

  internal static async Task<BTree> CreateAsync()
  {
    throw new NotImplementedException();
  }
}