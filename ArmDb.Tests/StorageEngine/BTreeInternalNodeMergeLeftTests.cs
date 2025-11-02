using ArmDb.DataModel;
using ArmDb.StorageEngine;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeInternalNodeTests
{
  [Fact]
  public void MergeLeft_WithValidNodes_CorrectlyMergesAndWipes()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var leftPage = CreateTestPage(10);
    var rightPage = CreateTestPage(11);
    SlottedPage.Initialize(leftPage, PageType.InternalNode);
    SlottedPage.Initialize(rightPage, PageType.InternalNode);

    var leftNode = new BTreeInternalNode(leftPage, tableDef);
    var rightNode = new BTreeInternalNode(rightPage, tableDef);

    // 1. Populate leftNode
    leftNode.InsertEntryForTest(new Key([DataValue.CreateInteger(100)]), new PageId(1, 10));
    var leftHeader = new PageHeader(leftPage);
    leftHeader.RightmostChildPageIndex = 20;

    // 2. Populate rightNode
    rightNode.InsertEntryForTest(new Key([DataValue.CreateInteger(300)]), new PageId(1, 30));
    var rightHeader = new PageHeader(rightPage);
    rightHeader.RightmostChildPageIndex = 40;

    // 3. Define the demoted key and its associated pointer from the parent
    // This key (200) separated leftNode and rightNode
    // Its pointer should be the *original* rightmost pointer of the *left* node (20)
    var demotedSeparatorKey = new Key([DataValue.CreateInteger(200)]);
    var demotedSeparatorKeyChildPage = new PageId(1, 20); // This is leftNode's original rightmost pointer

    // Act
    // Call the method on the right node, merging it into the left.
    rightNode.MergeLeft(leftNode, demotedSeparatorKey, demotedSeparatorKeyChildPage);

    // Assert
    // 1. Verify leftNode now contains all entries
    // It should have its original (100,10), the demoted key (200,20), and rightNode's (300,30)
    var leftEntries = leftNode.GetAllRawEntriesForTest();
    Assert.Equal(3, leftEntries.Count);
    Assert.Equal(new Key([DataValue.CreateInteger(100)]), leftEntries[0].Key);
    Assert.Equal(demotedSeparatorKey, leftEntries[1].Key);
    Assert.Equal(new PageId(1, 20), leftEntries[1].PageId); // Check pointer
    Assert.Equal(new Key([DataValue.CreateInteger(300)]), leftEntries[2].Key);

    // 2. Verify leftNode's RightmostChildPageIndex is updated
    // It should now be the RightmostChildPageIndex from the (now merged) rightNode
    var leftHeaderAfter = new PageHeader(leftPage);
    Assert.Equal(40, leftHeaderAfter.RightmostChildPageIndex);

    // 3. Verify rightNode has been wiped
    Assert.Equal(0, rightNode.ItemCount);
    var rightHeaderAfter = new PageHeader(rightPage);
    // Note that the BTreeInternalNodeTests.Repopulate.cs file is open on the right hand side. I'm going to create a new partial class in a new file, `BTreeInternalNodeTests.Merge.cs`
    Assert.Equal(0, rightHeaderAfter.ItemCount);
  }

  [Fact]
  public void MergeLeft_WhenLeftSiblingIsFull_ThrowsInvalidOperationException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var leftPage = CreateTestPage(20);
    var rightPage = CreateTestPage(21);
    SlottedPage.Initialize(leftPage, PageType.InternalNode);
    SlottedPage.Initialize(rightPage, PageType.InternalNode);

    var leftNode = new BTreeInternalNode(leftPage, tableDef);
    var rightNode = new BTreeInternalNode(rightPage, tableDef);

    // 1. Fill the left node so it has almost no space left
    var testKey = new Key([DataValue.CreateInteger(1)]);
    var testPageId = new PageId(1, 1);
    var entryBytes = BTreeInternalNode.SerializeRecord(testKey, testPageId, tableDef);
    int entrySize = entryBytes.Length + Slot.Size;
    int maxEntries = (Page.Size - PageHeader.HEADER_SIZE) / entrySize;

    for (int i = 0; i < maxEntries - 1; i++) // Fill to nearly full
    {
      // Use different keys to be realistic
      var key = new Key([DataValue.CreateInteger(i * 10)]);
      var pageId = new PageId(1, i);
      leftNode.InsertEntryForTest(key, pageId);
    }

    // 2. Add entries to the right node (the one to be merged)
    rightNode.InsertEntryForTest(new Key([DataValue.CreateInteger(900)]), new PageId(1, 90));
    rightNode.InsertEntryForTest(new Key([DataValue.CreateInteger(910)]), new PageId(1, 91));

    // 3. Define the demoted key
    var demotedKey = new Key([DataValue.CreateInteger(800)]);
    var demotedPageId = new PageId(1, 80);

    // Act & Assert
    // The attempt to merge should fail because the left node cannot
    // accommodate the demoted key + the right node's entries.
    var ex = Assert.Throws<InvalidOperationException>(() =>
        rightNode.MergeLeft(leftNode, demotedKey, demotedPageId)
    );

    Assert.Contains("Cannot merge into left node due to insufficient space.", ex.Message, StringComparison.OrdinalIgnoreCase);
  }
}
