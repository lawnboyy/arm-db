using ArmDb.DataModel;
using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

/// <summary>
/// BTree class for orchestrating the operations of a clustered index.
/// </summary>
internal sealed class BTree
{
  private readonly BufferPoolManager _bpm;
  private readonly TableDefinition _tableDefinition;
  private PageId _rootPageId;

  private BTree(BufferPoolManager bmp, TableDefinition tableDefinition, PageId rootPageId)
  {
    _bpm = bmp;
    _tableDefinition = tableDefinition;
    _rootPageId = rootPageId;
  }

  internal static async Task<BTree> CreateAsync(BufferPoolManager bpm, TableDefinition tableDef, PageId? rootPageId = null)
  {
    ArgumentNullException.ThrowIfNull(bpm, nameof(bpm));
    ArgumentNullException.ThrowIfNull(tableDef, nameof(tableDef));

    // If are handed a root page, then just unpin it and return the new B-Tree...
    if (rootPageId.HasValue)
    {
      // Return a new BTree instance...
      return new BTree(bpm, tableDef, rootPageId.Value);
    }

    // Use the buffer pool manager to allocate a new page for this table.
    var rootPage = await bpm.CreatePageAsync(tableDef.TableId);

    // Format the new leaf page...
    SlottedPage.Initialize(rootPage, PageType.LeafNode);

    // Now we are done with the page, so we can unpin it.
    await bpm.UnpinPageAsync(rootPage.Id, true);

    // Return a new BTree instance...
    return new BTree(bpm, tableDef, rootPage.Id);
  }

  /// <summary>
  /// Searches the B-Tree by traversing internal nodes to find the leaf node that would contain the
  /// given key value. If the record is found in the leaf node, it is returned. Otherwise, a null
  /// value is returned. The bulk of the work is delegated to the private recursive method.
  /// </summary>
  /// <param name="key">Key of the record to search for</param>
  /// <returns>The record if found, null otherwise.</returns>
  internal async Task<Record?> SearchAsync(Key key)
  {
    ArgumentNullException.ThrowIfNull(key, nameof(key));
    return await SearchRecursiveAsync(_rootPageId, key);
  }

  private async Task<Record?> SearchRecursiveAsync(PageId pageId, Key key)
  {
    // Fetch the page (this will pin the page)...
    var page = await _bpm.FetchPageAsync(pageId);

    // Pull the page header and check the type...
    var header = new PageHeader(page);

    // Base Case
    // If this is a leaf node, then search for the record and return
    if (header.PageType == PageType.LeafNode)
    {
      var leafNode = new BTreeLeafNode(page, _tableDefinition);
      var record = leafNode.Search(key);

      // Unpin the page now that we are done with it.
      await _bpm.UnpinPageAsync(pageId, false);

      return record;
    }
    // Otherwise, it's an internal node and we need to recurse further...
    else if (header.PageType == PageType.InternalNode)
    {
      // Find the child node associated with this key...
      var internalNode = new BTreeInternalNode(page, _tableDefinition);
      var childPageId = internalNode.LookupChildPage(key);

      // Unpin the page now that we are done with it...
      await _bpm.UnpinPageAsync(pageId, false);

      // Recursively call this search method...
      return await SearchRecursiveAsync(childPageId, key);
    }

    throw new InvalidDataException("B-Tree Node type was invalid!");
  }

#if DEBUG
  // Test hook to get the root page ID
  internal PageId GetRootPageIdForTest() => _rootPageId;
#endif
}