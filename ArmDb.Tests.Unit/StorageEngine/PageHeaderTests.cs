using System.Buffers.Binary; // Needed for direct byte verification
using ArmDb.Storage;

namespace ArmDb.Tests.Unit.Storage;

public class PageHeaderTests
{
  // Helper to create a blank test page with a writeable buffer
  private static Page CreateTestPage()
  {
    var buffer = new byte[Page.Size];
    var memory = new Memory<byte>(buffer);
    // The PageId doesn't matter for header tests, so use default
    return new Page(default, memory);
  }

  [Fact]
  public void PageHeader_SetAndGetCommonProperties_WritesAndReadsCorrectValues()
  {
    // Arrange
    var page = CreateTestPage();
    // Create the header struct as a view over the page's memory
    var header = new PageHeader(page);

    long expectedLsn = 0x1122334455667788L;
    int expectedItemCount = 123;
    int expectedDataStart = 4096;
    int expectedParentIndex = 789;
    var expectedPageType = PageType.LeafNode; // Use a specific type for the test

    // Act
    header.PageLsn = expectedLsn;
    header.ItemCount = expectedItemCount;
    header.DataStartOffset = expectedDataStart;
    header.ParentPageIndex = expectedParentIndex;
    header.PageType = expectedPageType;

    // Assert
    // 1. Read back through the properties to ensure getters work
    Assert.Equal(expectedLsn, header.PageLsn);
    Assert.Equal(expectedItemCount, header.ItemCount);
    Assert.Equal(expectedDataStart, header.DataStartOffset);
    Assert.Equal(expectedParentIndex, header.ParentPageIndex);
    Assert.Equal(expectedPageType, header.PageType);

    // 2. Verify by reading directly from the underlying buffer to ensure setters worked correctly
    var pageSpan = page.Span;
    Assert.Equal(expectedLsn, BinaryPrimitives.ReadInt64LittleEndian(pageSpan.Slice(PageHeader.PAGE_LSN_OFFSET)));
    Assert.Equal(expectedItemCount, BinaryPrimitives.ReadInt32LittleEndian(pageSpan.Slice(PageHeader.ITEM_COUNT_OFFSET)));
    Assert.Equal(expectedDataStart, BinaryPrimitives.ReadInt32LittleEndian(pageSpan.Slice(PageHeader.DATA_START_OFFSET)));
    Assert.Equal(expectedParentIndex, BinaryPrimitives.ReadInt32LittleEndian(pageSpan.Slice(PageHeader.PARENT_PAGE_INDEX_OFFSET)));
    Assert.Equal((byte)expectedPageType, pageSpan[PageHeader.PAGE_TYPE_OFFSET]);
  }

  // --- Tests for Leaf Node Specific Properties ---

  [Fact]
  public void PageHeader_WhenTypeIsLeafNode_SetAndGetSiblingPointers_Succeeds()
  {
    // Arrange
    var page = CreateTestPage();
    var header = new PageHeader(page);
    // Set the context for the page type
    header.PageType = PageType.LeafNode;

    int expectedPrev = 101;
    int expectedNext = 103;

    // Act
    header.PrevPageIndex = expectedPrev;
    header.NextPageIndex = expectedNext;

    // Assert
    Assert.Equal(expectedPrev, header.PrevPageIndex);
    Assert.Equal(expectedNext, header.NextPageIndex);

    // Verify underlying bytes
    var pageSpan = page.Span;
    Assert.Equal(expectedPrev, BinaryPrimitives.ReadInt32LittleEndian(pageSpan.Slice(PageHeader.TYPE_SPECIFIC_POINTER_1_OFFSET)));
    Assert.Equal(expectedNext, BinaryPrimitives.ReadInt32LittleEndian(pageSpan.Slice(PageHeader.TYPE_SPECIFIC_POINTER_2_OFFSET)));
  }

  [Fact]
  public void PageHeader_WhenTypeIsLeafNode_AccessingInternalNodeProperty_ThrowsInvalidOperationException()
  {
    // Arrange
    var page = CreateTestPage();
    // Initialize the page header with the correct type
    new PageHeader(page).PageType = PageType.LeafNode;

    // Act & Assert for Getter
    var exGet = Assert.Throws<InvalidOperationException>(() =>
    {
      // Create a new header view inside the lambda and access the property.
      // This avoids capturing a ref struct from an outer scope.
      _ = new PageHeader(page).RightmostChildPageIndex;
    });
    Assert.Contains("only valid for InternalNode pages", exGet.Message);

    // Act & Assert for Setter
    var exSet = Assert.Throws<InvalidOperationException>(() =>
    {
      // Do the same for the setter test.
      new PageHeader(page).RightmostChildPageIndex = 123;
    });
    Assert.Contains("only valid for InternalNode pages", exSet.Message);
  }

  // --- Tests for Internal Node Specific Properties ---

  [Fact]
  public void PageHeader_WhenTypeIsInternalNode_SetAndGetRightmostPointer_Succeeds()
  {
    // Arrange
    var page = CreateTestPage();
    var header = new PageHeader(page);
    header.PageType = PageType.InternalNode; // Set page type to InternalNode

    int expectedRightmost = 999;

    // Act
    header.RightmostChildPageIndex = expectedRightmost;

    // Assert
    Assert.Equal(expectedRightmost, header.RightmostChildPageIndex);

    // Verify underlying bytes (uses the same offset as PrevPageIndex)
    var pageSpan = page.Span;
    Assert.Equal(expectedRightmost, BinaryPrimitives.ReadInt32LittleEndian(pageSpan.Slice(PageHeader.TYPE_SPECIFIC_POINTER_1_OFFSET)));
  }

  [Fact]
  public void PageHeader_WhenTypeIsInternalNode_AccessingLeafNodeProperties_ThrowsInvalidOperationException()
  {
    // Arrange
    var page = CreateTestPage();
    new PageHeader(page).PageType = PageType.InternalNode;

    // Act & Assert for PrevPageIndex
    Assert.Throws<InvalidOperationException>(() => _ = new PageHeader(page).PrevPageIndex);
    Assert.Throws<InvalidOperationException>(() => new PageHeader(page).PrevPageIndex = 123);

    // Act & Assert for NextPageIndex
    Assert.Throws<InvalidOperationException>(() => _ = new PageHeader(page).NextPageIndex);
    Assert.Throws<InvalidOperationException>(() => new PageHeader(page).NextPageIndex = 123);
  }
}