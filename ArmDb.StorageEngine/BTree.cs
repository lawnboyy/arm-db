using System.Text;
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

    // If we are handed a root page, then just unpin it and return the new B-Tree...
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
    bpm.UnpinPage(rootPage.Id, true);

    // Return a new BTree instance...
    return new BTree(bpm, tableDef, rootPage.Id);
  }

  /// <summary>
  /// 
  /// </summary>
  /// <param name="record"></param>
  /// <returns></returns>
  internal async Task InsertAsync(Record record)
  {
    // Get the primary key for this record so we can navigate the tree to find the insertion point...
    var key = record.GetPrimaryKey(_tableDefinition);
    var leafSplitResult = await InsertRecursiveAsync(_rootPageId, record, key);

    // If we have a split result, we must recursively promote separator keys and propagate any additional splits up the tree...
    if (leafSplitResult != null)
    {
      var result = await PromoteKeyRecursive(leafSplitResult);
      if (result != null)
      {
        throw new Exception("Got a split result back after completing recursive key promotion!");
      }
    }
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

  private async Task<SplitResult?> InsertRecursiveAsync(PageId pageId, Record record, Key key, BTreeInternalNode? parentNode = null)
  {
    // Fetch the page (this will pin the page)...
    var page = await _bpm.FetchPageAsync(pageId);

    var header = new PageHeader(page);

    // Base Case: Leaf
    // Try to insert the new record in this leaf node...
    if (header.PageType == PageType.LeafNode)
    {
      // Wrap the page in a leaf node...
      var leafNode = new BTreeLeafNode(page, _tableDefinition);
      if (leafNode.TryInsert(record))
      {
        return null;
      }

      // Page is full and we could not insert, so we'll need to split this node...
      // First we need to allocate a new leaf node to house half the contents of the existing node...
      var (newLeafNode, newLeafPageId) = await CreateNewLeafNode();

      // Fetch the right sibling...
      var rightSiblingLeafNode = await FetchRightLeafSiblingAsync(leafNode);

      // Now perform the split...
      var newSeparatorKey = leafNode.SplitAndInsert(record, newLeafNode, rightSiblingLeafNode);

      // If there is no parent, then we have a single level tree with a single leaf and must create
      // a new internal node as the new root to promote the separator key to.
      if (leafNode.ParentPageIndex == PageHeader.INVALID_PAGE_INDEX)
      {
        var (newRootNode, newPageId) = await CreateNewInternalNode();
        // Insert our new separator key+child pointer record...
        if (!newRootNode.TryInsert(newSeparatorKey, pageId))
        {
          // This should not happen since this is a brand new root node...
          throw new Exception("Could not insert into a new empty node!");
        }
        // Set the right most pointer to the new right sibling leaf node...
        newRootNode.SetRightmostChildId(newLeafNode.PageIndex);

        // Unpin all pages...
        _bpm.UnpinPage(page.Id, true);
        _bpm.UnpinPage(newLeafPageId, true);

        // Set the new root ID
        _rootPageId = newPageId;

        return null;
      }
      else
      {
        if (parentNode == null)
        {
          throw new Exception("Cannot promote new separator key because the parent node was null!");
        }

        // Return the result of the leaf split so we can promote a new separator key...        
        return new SplitResult(newSeparatorKey, parentNode, pageId, newLeafPageId);
      }
    }
    // Otherwise, it's an internal node and we need to recurse further...
    else if (header.PageType == PageType.InternalNode)
    {
      // Find the child node associated with this key...
      var internalNode = new BTreeInternalNode(page, _tableDefinition);
      var childPageId = internalNode.LookupChildPage(key);

      // Unpin the page now that we are done with it...
      _bpm.UnpinPage(pageId, false);

      // Recursively call this search method...
      return await InsertRecursiveAsync(childPageId, record, key, internalNode);
    }

    throw new InvalidDataException("B-Tree Node type was invalid!");
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
      _bpm.UnpinPage(pageId, false);

      return record;
    }
    // Otherwise, it's an internal node and we need to recurse further...
    else if (header.PageType == PageType.InternalNode)
    {
      // Find the child node associated with this key...
      var internalNode = new BTreeInternalNode(page, _tableDefinition);
      var childPageId = internalNode.LookupChildPage(key);

      // Unpin the page now that we are done with it...
      _bpm.UnpinPage(pageId, false);

      // Recursively call this search method...
      return await SearchRecursiveAsync(childPageId, key);
    }

    throw new InvalidDataException("B-Tree Node type was invalid!");
  }

  private async Task<SplitResult?> PromoteKeyRecursive(SplitResult splitResult)
  {
    var (keyToPromote, nodeToInsertPromotedKey, childPageId, rightSiblingChildId) = splitResult;

    // Base case: Root Node Reached (no parent)
    if (nodeToInsertPromotedKey.ParentPageIndex == PageHeader.INVALID_PAGE_INDEX)
    {
      // If this is the root node, try to insert the new separator key...
      if (nodeToInsertPromotedKey.TryInsert(keyToPromote, childPageId))
      {
        // First find the slot index where the new key will go... The existing key of that slot needs to point to the new right sibling child.
        var slotInsertionIndex = nodeToInsertPromotedKey.FindPrimaryKeySlotIndex(keyToPromote);
        var nextKeyToTheRightIndex = slotInsertionIndex + 1;
        // Now update the separator key to the right of the new promoted key to point to the new right sibling child.
        nodeToInsertPromotedKey.SetChildPointer(nextKeyToTheRightIndex, rightSiblingChildId.PageIndex);
      }
      else
      {
        // If the root is full, we must split it and form a new root node.
        // Page is full and we could not insert, so we'll need to split the root node...
        UpdatePointerOfKeyOnTheRight(keyToPromote, nodeToInsertPromotedKey, rightSiblingChildId);

        // First we need to allocate a new internal node to house half the contents of the existing node...
        var (newInternalNode, newInternalNodeId) = await CreateNewInternalNode();
        // Split the node and get the new separator key which will need to go into a new root node...
        var newSeparatorKey = nodeToInsertPromotedKey.SplitAndInsert(keyToPromote, childPageId, newInternalNode);

        // Now create a new root node, insert the new separator key that points to the original internal node that was split.
        var (newRootNode, newRootPageId) = await CreateNewInternalNode();

        // Insert our new separator key+child pointer record...
        if (!newRootNode.TryInsert(newSeparatorKey, childPageId))
        {
          // This should not happen since this is a brand new root node...
          throw new Exception("Could not insert into a new empty node!");
        }
        // Set the right most pointer to the new right sibling internal node...
        newRootNode.SetRightmostChildId(newInternalNodeId.PageIndex);

        // Unpin all pages...
        _bpm.UnpinPage(newRootPageId, true);
        _bpm.UnpinPage(newInternalNodeId, true);

        // Set the new root ID
        _rootPageId = newRootPageId;

        return null;
      }

      // If we complete the base case, no further splits are possible or necessary, so return null;
      return null;
    }
    // This is an internal node. We'll need to recursely promote if the given node is full...
    else
    {
      // If the node is not full, we insert the new separator key which shall point to the original child,
      // and point the next greatest separator key to the right sibling.    
      UpdatePointerOfKeyOnTheRight(keyToPromote, nodeToInsertPromotedKey, rightSiblingChildId);

      if (nodeToInsertPromotedKey.TryInsert(keyToPromote, childPageId))
      {
        // If we successfully insert a promoted key, then we need no further splits and
        // are done.
        return null;
      }
      else
      {
        // Handle case in which we could not insert the promoted key into the internal node
        // which means we need to perform a recursive split.

        // First we need to allocate a new internal node to house half the contents of the existing node...
        var (newInternalNode, newInternalNodeId) = await CreateNewInternalNode();
        // Split the node...
        var newSeparatorKey = nodeToInsertPromotedKey.SplitAndInsert(keyToPromote, childPageId, newInternalNode);

        var parentPage = await _bpm.FetchPageAsync(new PageId(_tableDefinition.TableId, nodeToInsertPromotedKey.ParentPageIndex));
        var parentNode = new BTreeInternalNode(parentPage, _tableDefinition);
        var recursiveSplit = new SplitResult(newSeparatorKey, parentNode, nodeToInsertPromotedKey.PageId, newInternalNodeId);

        return await PromoteKeyRecursive(recursiveSplit);
      }
    }
  }

  private void UpdatePointerOfKeyOnTheRight(Key keyToPromote, BTreeInternalNode nodeToInsertPromotedKey, PageId rightSiblingChildId)
  {
    // First find the slot index where the new key will go... The existing key in that slot needs to point to the new right sibling child.
    var slotInsertionIndex = nodeToInsertPromotedKey.FindPrimaryKeySlotIndex(keyToPromote);
    // The slot index should be negative, otherwise the separator key already exists in the node and
    // we have an error condition.
    if (slotInsertionIndex >= 0)
    {
      // TODO: Throw an exception here as the separator key to insert is already present in the internal node.
    }

    // Convert the slot index using the bitwise complement.
    var nextKeyToTheRightIndex = ~slotInsertionIndex;

    // Check if the insertion point is the last index which indicates that the key is the largest separator key in the node and there
    // is no key to the right. In this case, we need to update the rightmost pointer of the node to point to the right sibling child page.
    if (nextKeyToTheRightIndex == nodeToInsertPromotedKey.ItemCount)
    {
      nodeToInsertPromotedKey.SetRightmostChildId(rightSiblingChildId.PageIndex);
    }
    else // Otherwise, update the separator key to the right of the new promoted key to point to the new right sibling child.
    {
      nodeToInsertPromotedKey.SetChildPointer(nextKeyToTheRightIndex, rightSiblingChildId.PageIndex);
    }
  }

  private async Task<(BTreeLeafNode newInternalNode, PageId newPageId)> CreateNewLeafNode()
  {
    var newPage = await _bpm.CreatePageAsync(_tableDefinition.TableId);
    var newPageId = newPage.Id;
    SlottedPage.Initialize(newPage, PageType.LeafNode);
    var newLeafNode = new BTreeLeafNode(newPage, _tableDefinition);
    return (newLeafNode, newPageId);
  }

  private async Task<(BTreeInternalNode newInternalNode, PageId newPageId)> CreateNewInternalNode()
  {
    var newPage = await _bpm.CreatePageAsync(_tableDefinition.TableId);
    var newPageId = newPage.Id;
    SlottedPage.Initialize(newPage, PageType.InternalNode);
    var newInternalNode = new BTreeInternalNode(newPage, _tableDefinition);
    return (newInternalNode, newPageId);
  }

  private async Task<BTreeLeafNode?> FetchRightLeafSiblingAsync(BTreeLeafNode leafNode)
  {
    // Fetch the right sibling...
    var rightSiblingIndex = leafNode.NextPageIndex;
    BTreeLeafNode? rightSiblingLeafNode = null;
    if (rightSiblingIndex != PageHeader.INVALID_PAGE_INDEX)
    {
      var rightSiblingPage = await _bpm.FetchPageAsync(new PageId(_tableDefinition.TableId, rightSiblingIndex));
      rightSiblingLeafNode = new BTreeLeafNode(rightSiblingPage, _tableDefinition);
    }

    return rightSiblingLeafNode;
  }

  // private async Task<BTreeLeafNode?> FetchRightInternalSiblingAsync(BTreeInternalNode internalNode)
  // {
  //   // Fetch the right sibling...
  //   var rightSiblingIndex = internalNode.NextPageIndex;
  //   BTreeLeafNode? rightSiblingLeafNode = null;
  //   if (rightSiblingIndex != PageHeader.INVALID_PAGE_INDEX)
  //   {
  //     var rightSiblingPage = await _bpm.FetchPageAsync(new PageId(_tableDefinition.TableId, rightSiblingIndex));
  //     rightSiblingLeafNode = new BTreeLeafNode(rightSiblingPage, _tableDefinition);
  //   }

  //   return rightSiblingLeafNode;
  // }

  private record SplitResult(Key keyToPromote, BTreeInternalNode nodeToInsertPromotedKey, PageId childPageId, PageId rightSiblingChildId)
  {
  }

#if DEBUG
  // Test hook to get the root page ID
  internal PageId GetRootPageIdForTest() => _rootPageId;
#endif
}