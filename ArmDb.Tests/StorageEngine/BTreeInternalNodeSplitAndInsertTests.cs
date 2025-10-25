using ArmDb.DataModel;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeInternalNodeTests
{
  [Fact]
  public void SplitAndInsert_InternalNode_RedistributesEntriesAndPromotesMedianKey()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var pageA = CreateTestPage(10); // The node to be split
    var pageB = CreateTestPage(11); // The new, empty sibling
    SlottedPage.Initialize(pageA, PageType.InternalNode);
    SlottedPage.Initialize(pageB, PageType.InternalNode);

    var nodeA = new BTreeInternalNode(pageA, tableDef);
    var nodeB = new BTreeInternalNode(pageB, tableDef);

    // 1. Pre-populate nodeA with 3 entries. (N=3)
    // Entries are (Key, PtrToLeftChild)
    // We'll insert in order for this setup
    nodeA.InsertEntryForTest(new Key([DataValue.CreateInteger(100)]), new PageId(1, 10));
    nodeA.InsertEntryForTest(new Key([DataValue.CreateInteger(200)]), new PageId(1, 20));
    nodeA.InsertEntryForTest(new Key([DataValue.CreateInteger(400)]), new PageId(1, 40));

    // Set the original rightmost pointer
    var headerA_before = new PageHeader(pageA);
    headerA_before.RightmostChildPageIndex = 50; // Points to page 50

    // 2. The new entry that is causing the split (N+1 = 4 entries total)
    var newKeyToInsert = new Key([DataValue.CreateInteger(300)]);
    var newChildPageId = new PageId(1, 30);

    // 3. Define the expected state after the split
    //    Temp sorted list of (N+1) entries:
    //    - (100, Ptr:10)
    //    - (200, Ptr:20)
    //    - (300, Ptr:30)  <-- This is the median (index 2 of 4, or (N+1)/2)
    //    - (400, Ptr:40)
    //    Original Rightmost Pointer: Ptr:50

    //    Expected promoted key (median key):
    var expectedSeparatorKey = new Key([DataValue.CreateInteger(300)]);

    // Act
    // Call the split method
    Key actualSeparatorKey = nodeA.SplitAndInsert(newKeyToInsert, newChildPageId, nodeB);

    // Assert
    // 1. Verify the correct separator key was promoted (returned)
    Assert.Equal(expectedSeparatorKey, actualSeparatorKey);

    // 2. Verify state of nodeA (the original, now left-hand, node)
    var headerA_after = new PageHeader(pageA);
    Assert.Equal(2, headerA_after.ItemCount); // Should contain (100, 10) and (200, 20)
                                              // The median entry's pointer (Ptr:30) becomes the new rightmost pointer for nodeA
    Assert.Equal(newChildPageId.PageIndex, headerA_after.RightmostChildPageIndex);

    // Check nodeA's entries
    var nodeA_entry0 = nodeA.GetEntryForTest(0);
    var nodeA_entry1 = nodeA.GetEntryForTest(1);
    Assert.Equal(new Key([DataValue.CreateInteger(100)]), nodeA_entry0.key);
    Assert.Equal(new Key([DataValue.CreateInteger(200)]), nodeA_entry1.key);

    // 3. Verify state of nodeB (the new, right-hand, sibling)
    var headerB = new PageHeader(pageB);
    Assert.Equal(1, headerB.ItemCount); // Should contain (400, 40)
                                        // The original rightmost pointer (Ptr:50) becomes the rightmost pointer for nodeB
    Assert.Equal(50, headerB.RightmostChildPageIndex);

    // Check nodeB's entry
    var nodeB_entry0 = nodeB.GetEntryForTest(0);
    Assert.Equal(new Key([DataValue.CreateInteger(400)]), nodeB_entry0.key);
    Assert.Equal(new PageId(1, 40), nodeB_entry0.childPageId);
  }

  [Fact]
  public void SplitAndInsert_WhenNewKeyIsNotMedian_PromotesCorrectMedianKey()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var pageA = CreateTestPage(20);
    var pageB = CreateTestPage(21);
    SlottedPage.Initialize(pageA, PageType.InternalNode);
    SlottedPage.Initialize(pageB, PageType.InternalNode);

    var nodeA = new BTreeInternalNode(pageA, tableDef);
    var nodeB = new BTreeInternalNode(pageB, tableDef);

    // 1. Pre-populate nodeA with 3 entries. (N=3)
    nodeA.InsertEntryForTest(new Key([DataValue.CreateInteger(100)]), new PageId(1, 10));
    nodeA.InsertEntryForTest(new Key([DataValue.CreateInteger(200)]), new PageId(1, 20));
    nodeA.InsertEntryForTest(new Key([DataValue.CreateInteger(400)]), new PageId(1, 40));
    var headerA_before = new PageHeader(pageA);
    headerA_before.RightmostChildPageIndex = 50;

    // 2. The new entry is the *smallest* key, not the median.
    var newKeyToInsert = new Key([DataValue.CreateInteger(50)]);
    var newChildPageId = new PageId(1, 5);

    // 3. Define the expected state after the split
    //    Temp sorted list of (N+1) entries:
    //    - ( 50, Ptr: 5)
    //    - (100, Ptr:10)
    //    - (200, Ptr:20)  <-- This is the median (index 2 of 4)
    //    - (400, Ptr:40)
    //    Original Rightmost Pointer: Ptr:50

    //    Expected promoted key (median key):
    var expectedSeparatorKey = new Key([DataValue.CreateInteger(200)]);

    // Act
    Key actualSeparatorKey = nodeA.SplitAndInsert(newKeyToInsert, newChildPageId, nodeB);

    // Assert
    // 1. Verify the correct separator key (200) was promoted
    Assert.Equal(expectedSeparatorKey, actualSeparatorKey);

    // 2. Verify state of nodeA (original node)
    var headerA_after = new PageHeader(pageA);
    Assert.Equal(2, headerA_after.ItemCount); // Should contain (50, 5) and (100, 10)
                                              // The median entry's pointer (Ptr:20) becomes the new rightmost pointer
    Assert.Equal(20, headerA_after.RightmostChildPageIndex);

    var nodeA_entry0 = nodeA.GetEntryForTest(0);
    var nodeA_entry1 = nodeA.GetEntryForTest(1);
    Assert.Equal(newKeyToInsert, nodeA_entry0.key);
    Assert.Equal(newChildPageId, nodeA_entry0.childPageId);
    Assert.Equal(new Key([DataValue.CreateInteger(100)]), nodeA_entry1.key);

    // 3. Verify state of nodeB (new sibling)
    var headerB = new PageHeader(pageB);
    Assert.Equal(1, headerB.ItemCount); // Should contain (400, 40)
                                        // The original rightmost pointer (Ptr:50) becomes the rightmost pointer for nodeB
    Assert.Equal(50, headerB.RightmostChildPageIndex);

    var nodeB_entry0 = nodeB.GetEntryForTest(0);
    Assert.Equal(new Key([DataValue.CreateInteger(400)]), nodeB_entry0.key);
  }

  [Fact]
  public void SplitAndInsert_WhenNewKeyIsLargest_PromotesCorrectMedianKey()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var pageA = CreateTestPage(30);
    var pageB = CreateTestPage(31);
    SlottedPage.Initialize(pageA, PageType.InternalNode);
    SlottedPage.Initialize(pageB, PageType.InternalNode);

    var nodeA = new BTreeInternalNode(pageA, tableDef);
    var nodeB = new BTreeInternalNode(pageB, tableDef);

    // 1. Pre-populate nodeA with 3 entries. (N=3)
    nodeA.InsertEntryForTest(new Key([DataValue.CreateInteger(100)]), new PageId(1, 10));
    nodeA.InsertEntryForTest(new Key([DataValue.CreateInteger(200)]), new PageId(1, 20));
    nodeA.InsertEntryForTest(new Key([DataValue.CreateInteger(300)]), new PageId(1, 30));
    var headerA_before = new PageHeader(pageA);
    headerA_before.RightmostChildPageIndex = 40;

    // 2. The new entry is the *largest* key.
    var newKeyToInsert = new Key([DataValue.CreateInteger(400)]);
    var newChildPageId = new PageId(1, 50);

    // 3. Define the expected state after the split
    //    Temp sorted list of (N+1) entries:
    //    - (100, Ptr:10)
    //    - (200, Ptr:20)
    //    - (300, Ptr:30)  <-- This is the median (index 2 of 4)
    //    - (400, Ptr:50)
    //    Original Rightmost Pointer: Ptr:40

    //    Expected promoted key (median key):
    var expectedSeparatorKey = new Key([DataValue.CreateInteger(300)]);

    // Act
    Key actualSeparatorKey = nodeA.SplitAndInsert(newKeyToInsert, newChildPageId, nodeB);

    // Assert
    // 1. Verify the correct separator key (300) was promoted
    Assert.Equal(expectedSeparatorKey, actualSeparatorKey);

    // 2. Verify state of nodeA (original node)
    var headerA_after = new PageHeader(pageA);
    Assert.Equal(2, headerA_after.ItemCount); // Should contain (100, 10) and (200, 20)
                                              // The median entry's pointer (Ptr:30) becomes the new rightmost pointer
    Assert.Equal(30, headerA_after.RightmostChildPageIndex);

    var nodeA_entry0 = nodeA.GetEntryForTest(0);
    var nodeA_entry1 = nodeA.GetEntryForTest(1);
    Assert.Equal(new Key([DataValue.CreateInteger(100)]), nodeA_entry0.key);
    Assert.Equal(new Key([DataValue.CreateInteger(200)]), nodeA_entry1.key);

    // 3. Verify state of nodeB (new sibling)
    var headerB = new PageHeader(pageB);
    Assert.Equal(1, headerB.ItemCount); // Should contain (400, 50)
                                        // The original rightmost pointer (Ptr:40) becomes the rightmost pointer for nodeB
    Assert.Equal(40, headerB.RightmostChildPageIndex);

    var nodeB_entry0 = nodeB.GetEntryForTest(0);
    Assert.Equal(newKeyToInsert, nodeB_entry0.key);
    Assert.Equal(newChildPageId, nodeB_entry0.childPageId);
  }
}