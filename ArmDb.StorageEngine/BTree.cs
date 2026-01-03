using ArmDb.DataModel;
using ArmDb.SchemaDefinition;

namespace ArmDb.Storage;

/// <summary>
/// BTree class for orchestrating the operations of a clustered index.
/// </summary>
internal sealed class BTree
{
  private readonly BufferPoolManager _bpm;
  private readonly TableDefinition _tableDefinition;
  private PageId _rootPageId;
  private Page _tableHeaderPage;

  private BTree(BufferPoolManager bmp, TableDefinition tableDefinition, PageId rootPageId, Page tableHeaderPage)
  {
    _bpm = bmp;
    _tableDefinition = tableDefinition;
    _rootPageId = rootPageId;
    _tableHeaderPage = tableHeaderPage;
  }

  internal static async Task<BTree> CreateAsync(BufferPoolManager bpm, TableDefinition tableDef, PageId? rootPageId = null, Page? metadataPage = null)
  {
    ArgumentNullException.ThrowIfNull(bpm, nameof(bpm));
    ArgumentNullException.ThrowIfNull(tableDef, nameof(tableDef));

    // If we receive a root page ID of 0, we must reject it as that page index is reserved for the table metadata...
    if (rootPageId.HasValue && rootPageId.Value.PageIndex == 0)
    {
      throw new InvalidOperationException("Attempt to create a new B-Tree with a given root page with index 0. The page with index 0 is reserved for table metadata only.");
    }

    // Allocate the table header page for metadata (holds the index of the root page)...
    if (metadataPage == null)
    {
      metadataPage = await bpm.CreatePageAsync(tableDef.TableId);
      SlottedPage.Initialize(metadataPage, PageType.TableHeader);
      if (metadataPage.Id.PageIndex != 0)
      {
        throw new InvalidOperationException($"New metadata page for table with ID: {tableDef.TableId} has an non-zero index. Does this table already exist?");
      }
    }

    // If we are handed a root page, then just unpin it and return the new B-Tree...
    if (rootPageId.HasValue)
    {
      var tableHeader = new PageHeader(metadataPage);
      tableHeader.RootPageIndex = rootPageId.Value.PageIndex;
      bpm.UnpinPage(metadataPage.Id, true);
      // Return a new BTree instance...
      return new BTree(bpm, tableDef, rootPageId.Value, metadataPage);
    }
    else
    {
      // Use the buffer pool manager to allocate a new page for this table.
      var rootPage = await bpm.CreatePageAsync(tableDef.TableId);
      var tableHeader = new PageHeader(metadataPage);
      tableHeader.RootPageIndex = rootPage.Id.PageIndex;

      // Format the new leaf page...
      SlottedPage.Initialize(rootPage, PageType.LeafNode);

      // Now we are done with the page, so we can unpin it.
      bpm.UnpinPage(rootPage.Id, true);

      // Unpin the table header page.
      bpm.UnpinPage(metadataPage.Id, true);

      // Return a new BTree instance...
      return new BTree(bpm, tableDef, rootPage.Id, metadataPage);
    }
  }

  /// <summary>
  /// Inserts a new record into the tree. Traverses the tree comparing the record key to the 
  /// internal nodes' separator keys to find the leaf where the record key belongs. If there
  /// is sufficient space in the leaf, the new record is inserted in the proper order in the
  /// slotted page's slot array. If there is insufficient space in the leaf, then the leaf is
  /// split into two separate nodes and the contents, including the new inserted record, are 
  /// split between them. The split promotes a key upwards recursively to the first parent
  /// node with sufficient space to insert the new promoted separator key.
  /// </summary>
  /// <param name="record">The record to insert.</param>
  /// <returns>A task representing the asynchronous work yet to be completed.</returns>
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
  /// Performs a seek and scan for all values between the minimum and maximum key provided. If no
  /// key boundaries are given, then a full table scan is performed.
  /// </summary>
  /// <param name="min"></param>
  /// <param name="max"></param>
  /// <returns></returns>
  internal async IAsyncEnumerable<Record> ScanAsync(
    Key? min = null, bool minInclusive = false, Key? max = null, bool maxInclusive = false)
  {
    var comparer = new KeyComparer();
    // Edge case: the min is greater than the max..
    if (min != null && max != null && comparer.Compare(min, max) > 0)
    {
      // If the min is greater than the max, then immediately return an empty set...
      yield break;
    }

    // Case 1: If no min or max keys are provided, then we do a full table scan, starting with the absolute min in the table...
    BTreeLeafNode minLeaf = (min == null)
      ? await GetLeftmostLeaf(_rootPageId) // Start at the absolute min value in the left most leaf
      : await FindLeafAsync(_rootPageId, min); // Find the leaf that contains the min value

    var currentLeaf = minLeaf;

    var startIndex = 0;
    if (min != null)
    {
      var unconvertedStartIndex = currentLeaf.FindPrimaryKeySlotIndex(min);
      startIndex = unconvertedStartIndex < 0 ? ~unconvertedStartIndex : unconvertedStartIndex;
      // If the min key is found and we are excluding it, we should bump the start index to the
      // next key (TODO: Handle edge case in which the start index is the last key in the leaf).
      // If the min key doesn't exist, then the insertion index will be the index
      // of the next largest key which is where we want our start index.
      if (!minInclusive && unconvertedStartIndex >= 0)
        startIndex += 1;
    }

    while (currentLeaf != null)
    {
      var rawRecords = currentLeaf.GetAllRawRecords();

      for (int i = startIndex; i < currentLeaf.ItemCount; i++)
      {
        var record = RecordSerializer.Deserialize(_tableDefinition.Columns, rawRecords[i]);
        // If we have a max key, then we need to break out of the iterator as soon as we
        // find a key that is larger than the max.
        if (max != null)
        {
          var recordKey = record.GetPrimaryKey(_tableDefinition);
          if (
            (maxInclusive && comparer.Compare(recordKey, max) > 0) ||
            (!maxInclusive && comparer.Compare(recordKey, max) >= 0)
          )
          {
            yield break;
          }
        }

        RecordSerializer.DeserializePrimaryKey(_tableDefinition, rawRecords[i]);
        yield return record;
      }

      var nextLeafPage = (currentLeaf.NextPageIndex == PageHeader.INVALID_PAGE_INDEX)
        ? null
        : await _bpm.FetchPageAsync(new PageId(_tableDefinition.TableId, currentLeaf.NextPageIndex));

      currentLeaf = nextLeafPage == null ? null : new BTreeLeafNode(nextLeafPage, _tableDefinition);

      // Reset the start index to 0 after traversing the first leaf node...
      startIndex = 0;
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

      try
      {
        if (leafNode.TryInsert(record))
        {
          _bpm.UnpinPage(page.Id, true);
          return null;
        }
      }
      catch
      {
        _bpm.UnpinPage(page.Id, false);
        throw;
      }

      // Page is full and we could not insert, so we'll need to split this node...
      // First we need to allocate a new leaf node to house half the contents of the existing node...
      var (newLeafNode, newLeafPageId) = await CreateNewLeafNode();

      // Fetch the right sibling...
      var rightSiblingLeafNode = await FetchRightLeafSiblingAsync(leafNode);

      // Now perform the split...
      Key newSeparatorKey;
      try
      {
        newSeparatorKey = leafNode.SplitAndInsert(record, newLeafNode, rightSiblingLeafNode);
      }
      catch (InvalidOperationException)
      {
        // Unpin our leaf node and rethrow so previous nodes in the call stack can also unpin and rethrow...
        _bpm.UnpinPage(leafNode.PageId, false);
        throw;
      }

      // If we have a parent, set the new leaf node's parent index here.
      if (parentNode != null)
        newLeafNode.SetParentPageIndex(parentNode.PageId.PageIndex);

      // If there is no parent, then we have a single level tree with a single leaf and must create
      // a new internal node as the new root to promote the separator key to.
      if (leafNode.ParentPageIndex == PageHeader.INVALID_PAGE_INDEX)
      {
        var (newRootNode, newPageId) = await CreateNewInternalNode();
        try
        {
          // Insert our new separator key+child pointer record...
          if (!newRootNode.TryInsert(newSeparatorKey, pageId))
          {
            // This should not happen since this is a brand new root node...
            throw new Exception("Could not insert into a new empty node!");
          }
          // Set the right most pointer to the new right sibling leaf node...
          newRootNode.SetRightmostChildId(newLeafNode.PageIndex);

          // Set the parent pointers of our split nodes to the new root node.
          leafNode.SetParentPageIndex(newRootNode.PageId.PageIndex);
          newLeafNode.SetParentPageIndex(newRootNode.PageId.PageIndex);

          // Unpin all pages...
          _bpm.UnpinPage(page.Id, true);
          _bpm.UnpinPage(newLeafPageId, true);

          if (parentNode != null)
            _bpm.UnpinPage(parentNode.PageId, false);

          // Set the new root ID
          _rootPageId = newPageId;
          // Update the table metadata to point to the new root page ID
          var tableMetadataPageHeader = new PageHeader(_tableHeaderPage);
          tableMetadataPageHeader.RootPageIndex = newPageId.PageIndex;

          return null;
        }
        catch
        {
          // Unpin all pages...
          _bpm.UnpinPage(page.Id, false);
          _bpm.UnpinPage(newLeafPageId, false);
          if (parentNode != null)
            _bpm.UnpinPage(parentNode.PageId, false);
          throw;
        }
      }
      else
      {
        if (parentNode == null)
        {
          throw new Exception("Cannot promote new separator key because the parent node was null!");
        }
        // Unpin the current page...
        _bpm.UnpinPage(page.Id, true);

        // Return the result of the leaf split so we can promote a new separator key...      
        // TODO: We are passing the parentNode back here as the node to promote the separator key to, but 
        // it is unpinned and should be pinned.  
        return new SplitResult(newSeparatorKey, parentNode, pageId, newLeafPageId);
      }
    }
    // Otherwise, it's an internal node and we need to recurse further...
    else if (header.PageType == PageType.InternalNode)
    {
      // Find the child node associated with this key...
      var internalNode = new BTreeInternalNode(page, _tableDefinition);
      var childPageId = internalNode.LookupChildPage(key);

      // Recursively call this search method...
      SplitResult? result = null;
      try
      {
        result = await InsertRecursiveAsync(childPageId, record, key, internalNode);
      }
      catch (InvalidOperationException)
      {
        // It's possible the record to insert was too large to fit on a page so we can't complete
        // the operation. We need to make sure we unpin our page here and re-throw.
        _bpm.UnpinPage(internalNode.PageId, false);
        throw;
      }

      // Make sure to unpin the current page unless it is part of a split result.
      if (result != null && result.nodeToInsertPromotedKey.PageId != pageId)
        _bpm.UnpinPage(pageId, false);

      return result;
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

  private async Task<BTreeLeafNode> FindLeafAsync(PageId pageId, Key key)
  {
    // Fetch the page (this will pin the page)...
    var page = await _bpm.FetchPageAsync(pageId);

    // Pull the page header and check the type...
    var header = new PageHeader(page);

    // Base Case
    // Once we reach a leaf node, simply return it. It doesn't matter if the 
    // leaf node actually contains the key or not; we only want the leaf that
    // should contain the key.
    if (header.PageType == PageType.LeafNode)
    {
      var leafNode = new BTreeLeafNode(page, _tableDefinition);
      return leafNode;
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
      return await FindLeafAsync(childPageId, key);
    }

    throw new InvalidDataException("B-Tree Node type was invalid!");
  }

  private async Task<SplitResult?> PromoteKeyRecursive(SplitResult splitResult)
  {
    var (keyToPromote, nodeToInsertPromotedKey, childPageId, rightSiblingChildId) = splitResult;

    // Base case: Root Node Reached (no parent)
    if (nodeToInsertPromotedKey.ParentPageIndex == PageHeader.INVALID_PAGE_INDEX)
    {
      // Before we insert the key, we must update the child pointer at the insertion point. If there
      // is a separator key record here, then it needs to point to the new right sibling child that
      // was created because of a split. If there is no separator key record here, then the promoted
      // key is the largest key in the node, so we must update the node's rightmost pointer to point
      // to the newly created right sibling child node.
      UpdateRightSidePointer(keyToPromote, nodeToInsertPromotedKey, rightSiblingChildId);
      // If this is the root node, try to insert the new separator key...
      if (!nodeToInsertPromotedKey.TryInsert(keyToPromote, childPageId))
      {
        // If the root is full, we must split it and form a new root node...

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

        // Set the parent pointers of our split nodes to the new root node.
        nodeToInsertPromotedKey.SetParentPageIndex(newRootNode.PageId.PageIndex);
        newInternalNode.SetParentPageIndex(newRootNode.PageId.PageIndex);

        // Unpin all new pages...
        _bpm.UnpinPage(newRootPageId, true);
        _bpm.UnpinPage(newInternalNodeId, true);

        // Set the new root ID
        _rootPageId = newRootPageId;
        // Update the table metadata to point to the new root page ID
        var tableMetadataPageHeader = new PageHeader(_tableHeaderPage);
        tableMetadataPageHeader.RootPageIndex = newRootPageId.PageIndex;
      }
      else
      {
        // Be sure to unpin the root now that we are done.
        _bpm.UnpinPage(nodeToInsertPromotedKey.PageId, true);
      }

      // If we complete the base case, no further splits are possible or necessary, so return null;
      return null;
    }
    // This is an internal node. We'll need to recursely promote if the given node is full...
    else
    {
      // If the node is not full, we insert the new separator key which shall point to the original child,
      // and point the next greatest separator key to the right sibling.    
      UpdateRightSidePointer(keyToPromote, nodeToInsertPromotedKey, rightSiblingChildId);

      if (nodeToInsertPromotedKey.TryInsert(keyToPromote, childPageId))
      {
        _bpm.UnpinPage(nodeToInsertPromotedKey.PageId, true);
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

        _bpm.UnpinPage(nodeToInsertPromotedKey.PageId, true);

        return await PromoteKeyRecursive(recursiveSplit);
      }
    }
  }

  private void UpdateRightSidePointer(Key keyToPromote, BTreeInternalNode nodeToInsertPromotedKey, PageId rightSiblingChildId)
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

  private record SplitResult(Key keyToPromote, BTreeInternalNode nodeToInsertPromotedKey, PageId childPageId, PageId rightSiblingChildId)
  {
  }

  private async Task<BTreeLeafNode> GetLeftmostLeaf(PageId pageId)
  {
    // Start at the root and traverse the leftmost child recursively until we hit the leftmost leaf node...
    // Fetch the page (this will pin the page)...
    var page = await _bpm.FetchPageAsync(pageId);

    // Pull the page header and check the type...
    var header = new PageHeader(page);

    // If we have reached a leaf node then return it...
    if (header.PageType == PageType.LeafNode)
    {
      var leafNode = new BTreeLeafNode(page, _tableDefinition);
      return leafNode;
    }
    else if (header.PageType == PageType.InternalNode)
    {
      // Find the child node associated with this key...
      var internalNode = new BTreeInternalNode(page, _tableDefinition);
      var childPageId = internalNode.GetLeftmostChildPointer();

      // Unpin the page now that we are done with it...
      _bpm.UnpinPage(pageId, false);

      // Recursively call this search method...
      return await GetLeftmostLeaf(childPageId);
    }

    throw new InvalidDataException("Found invalud B-Tree Node type attempting to traverse the leftmost path!");
  }

#if DEBUG
  // Test hook to get the root page ID
  internal PageId GetRootPageIdForTest() => _rootPageId;
#endif

#if DEBUG
  // Test hook to get the table header page
  internal Page GetTableHeaderPageForTest() => _tableHeaderPage;
#endif
}