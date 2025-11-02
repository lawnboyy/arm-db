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

  [Fact]
  public void MergeLeft_WithNullLeftSibling_ThrowsArgumentNullException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var rightPage = CreateTestPage(11);
    SlottedPage.Initialize(rightPage, PageType.InternalNode);

    var rightNode = new BTreeInternalNode(rightPage, tableDef);
    BTreeInternalNode? leftSibling = null;

    var demotedKey = new Key([DataValue.CreateInteger(200)]);
    var demotedPageId = new PageId(1, 20);

    // Act & Assert
    Assert.Throws<ArgumentNullException>("leftSibling", () =>
        rightNode.MergeLeft(leftSibling!, demotedKey, demotedPageId)
    );
  }

  [Fact]
  public void MergeLeft_WithNullDemotedKey_ThrowsArgumentNullException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var leftPage = CreateTestPage(10);
    var rightPage = CreateTestPage(11);
    SlottedPage.Initialize(leftPage, PageType.InternalNode);
    SlottedPage.Initialize(rightPage, PageType.InternalNode);

    var leftNode = new BTreeInternalNode(leftPage, tableDef);
    var rightNode = new BTreeInternalNode(rightPage, tableDef);

    Key? demotedKey = null;
    var demotedPageId = new PageId(1, 20);

    // Act & Assert
    Assert.Throws<ArgumentNullException>("demotedSeparatorKey", () =>
        rightNode.MergeLeft(leftNode, demotedKey!, demotedPageId)
    );
  }

  [Fact]
  public void MergeLeft_WhenRightSiblingIsEmpty_CorrectlyMergesAndUpdates()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var leftPage = CreateTestPage(10);
    var rightPage = CreateTestPage(11); // 'this' node (empty)
    SlottedPage.Initialize(leftPage, PageType.InternalNode);
    SlottedPage.Initialize(rightPage, PageType.InternalNode);

    var leftNode = new BTreeInternalNode(leftPage, tableDef);
    var rightNode = new BTreeInternalNode(rightPage, tableDef);

    // 1. Populate leftNode
    leftNode.InsertEntryForTest(new Key([DataValue.CreateInteger(100)]), new PageId(1, 10));
    var leftHeader = new PageHeader(leftPage);
    leftHeader.RightmostChildPageIndex = 20;

    // 2. rightNode is empty (ItemCount is 0)
    var rightHeader = new PageHeader(rightPage);
    rightHeader.RightmostChildPageIndex = 40; // It has a rightmost pointer

    Assert.Equal(0, rightNode.ItemCount); // Verify setup

    // 3. Define the demoted key and its associated pointer
    var demotedSeparatorKey = new Key([DataValue.CreateInteger(200)]);
    var demotedSeparatorKeyChildPage = new PageId(1, 20); // leftNode's original rightmost pointer

    // Act
    // Merge the empty rightNode into the leftNode
    rightNode.MergeLeft(leftNode, demotedSeparatorKey, demotedSeparatorKeyChildPage);

    // Assert
    // 1. Verify leftNode has its original entry + the demoted key entry
    var leftEntries = leftNode.GetAllRawEntriesForTest();
    Assert.Equal(2, leftEntries.Count);
    Assert.Equal(new Key([DataValue.CreateInteger(100)]), leftEntries[0].Key);
    Assert.Equal(demotedSeparatorKey, leftEntries[1].Key);
    Assert.Equal(demotedSeparatorKeyChildPage, leftEntries[1].PageId);

    // 2. Verify leftNode's RightmostChildPageIndex is updated
    // It should now be the RightmostChildPageIndex from the (empty) rightNode
    var leftHeaderAfter = new PageHeader(leftPage);
    Assert.Equal(40, leftHeaderAfter.RightmostChildPageIndex);

    // 3. Verify rightNode has been wiped
    Assert.Equal(0, rightNode.ItemCount);
  }

  [Fact]
  public void MergeLeft_WhenLeftSiblingIsEmpty_CorrectlyMergesAndUpdates()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var leftPage = CreateTestPage(10); // 'target' node (empty)
    var rightPage = CreateTestPage(11); // 'this' node
    SlottedPage.Initialize(leftPage, PageType.InternalNode);
    SlottedPage.Initialize(rightPage, PageType.InternalNode);

    var leftNode = new BTreeInternalNode(leftPage, tableDef);
    var rightNode = new BTreeInternalNode(rightPage, tableDef);

    // 1. leftNode is empty (ItemCount is 0)
    var leftHeader = new PageHeader(leftPage);
    leftHeader.RightmostChildPageIndex = 20; // It has a rightmost pointer
    Assert.Equal(0, leftNode.ItemCount); // Verify setup

    // 2. Populate rightNode
    rightNode.InsertEntryForTest(new Key([DataValue.CreateInteger(300)]), new PageId(1, 30));
    var rightHeader = new PageHeader(rightPage);
    rightHeader.RightmostChildPageIndex = 40;

    // 3. Define the demoted key and its associated pointer
    var demotedSeparatorKey = new Key([DataValue.CreateInteger(200)]);
    var demotedSeparatorKeyChildPage = new PageId(1, 20); // leftNode's original rightmost pointer

    // Act
    // Merge the rightNode into the empty leftNode
    rightNode.MergeLeft(leftNode, demotedSeparatorKey, demotedSeparatorKeyChildPage);

    // Assert
    // 1. Verify leftNode now contains the demoted key + rightNode's entries
    var leftEntries = leftNode.GetAllRawEntriesForTest();
    Assert.Equal(2, leftEntries.Count);
    Assert.Equal(demotedSeparatorKey, leftEntries[0].Key);
    Assert.Equal(demotedSeparatorKeyChildPage, leftEntries[0].PageId);
    Assert.Equal(new Key([DataValue.CreateInteger(300)]), leftEntries[1].Key);
    Assert.Equal(new PageId(1, 30), leftEntries[1].PageId);

    // 2. Verify leftNode's RightmostChildPageIndex is updated
    var leftHeaderAfter = new PageHeader(leftPage);
    Assert.Equal(40, leftHeaderAfter.RightmostChildPageIndex);

    // 3. Verify rightNode has been wiped
    Assert.Equal(0, rightNode.ItemCount);
  }

  [Fact]
  public void MergeLeft_WithCompositeKey_CorrectlyMergesAndUpdates()
  {
    // Arrange
    var tableDef = CreateCompositePKTable();
    var leftPage = CreateTestPage(10);
    var rightPage = CreateTestPage(11); // 'this' node
    SlottedPage.Initialize(leftPage, PageType.InternalNode);
    SlottedPage.Initialize(rightPage, PageType.InternalNode);

    var leftNode = new BTreeInternalNode(leftPage, tableDef);
    var rightNode = new BTreeInternalNode(rightPage, tableDef);

    // 1. Populate leftNode
    var keyA = new Key([DataValue.CreateString("ABC"), DataValue.CreateInteger(10)]);
    var pageIdA = new PageId(1, 10);
    leftNode.InsertEntryForTest(keyA, pageIdA);
    var leftHeader = new PageHeader(leftPage);
    leftHeader.RightmostChildPageIndex = 20;

    // 2. Populate rightNode
    var keyC = new Key([DataValue.CreateString("CDEF"), DataValue.CreateInteger(10)]);
    var pageIdC = new PageId(1, 30);
    rightNode.InsertEntryForTest(keyC, pageIdC);
    var rightHeader = new PageHeader(rightPage);
    rightHeader.RightmostChildPageIndex = 40;

    // 3. Define the demoted key and its associated pointer
    var demotedSeparatorKey = new Key([DataValue.CreateString("BAB"), DataValue.CreateInteger(10)]);
    var demotedSeparatorKeyChildPage = new PageId(1, 20); // leftNode's original rightmost pointer

    // Act
    rightNode.MergeLeft(leftNode, demotedSeparatorKey, demotedSeparatorKeyChildPage);

    // Assert
    // 1. Verify leftNode now contains all entries
    var leftEntries = leftNode.GetAllRawEntriesForTest();
    Assert.Equal(3, leftEntries.Count);
    Assert.Equal(keyA, leftEntries[0].Key);
    Assert.Equal(demotedSeparatorKey, leftEntries[1].Key);
    Assert.Equal(demotedSeparatorKeyChildPage, leftEntries[1].PageId);
    Assert.Equal(keyC, leftEntries[2].Key);

    // 2. Verify leftNode's RightmostChildPageIndex is updated
    var leftHeaderAfter = new PageHeader(leftPage);
    Assert.Equal(40, leftHeaderAfter.RightmostChildPageIndex);

    // 3. Verify rightNode has been wiped
    Assert.Equal(0, rightNode.ItemCount);
  }
}
