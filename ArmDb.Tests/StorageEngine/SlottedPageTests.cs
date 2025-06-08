using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public class SlottedPageTests
{
  [Fact]
  public void Initialize_AsLeafNode_SetsDefaultHeaderCorrectly()
  {
    // Arrange
    var page = CreateTestPage();

    // Act
    SlottedPage.Initialize(page, PageType.LeafNode);

    // Assert
    var header = new PageHeader(page); // Create a view to read back the values

    Assert.Equal(0L, header.PageLsn);
    Assert.Equal(0, header.ItemCount);
    Assert.Equal(Page.Size, header.DataStartOffset);
    Assert.Equal(PageHeader.INVALID_PAGE_INDEX, header.ParentPageIndex); // Check default parent
    Assert.Equal(PageType.LeafNode, header.PageType);
    // Check leaf-specific fields are set to their "null" state
    Assert.Equal(PageHeader.INVALID_PAGE_INDEX, header.PrevPageIndex);
    Assert.Equal(PageHeader.INVALID_PAGE_INDEX, header.NextPageIndex);
  }

  [Fact]
  public void Initialize_AsLeafNodeWithParent_SetsParentPageIndexCorrectly()
  {
    // Arrange
    var page = CreateTestPage();
    int expectedParentIndex = 123;

    // Act
    SlottedPage.Initialize(page, PageType.LeafNode, expectedParentIndex);

    // Assert
    var header = new PageHeader(page);
    Assert.Equal(expectedParentIndex, header.ParentPageIndex);
    // Sanity check another value to ensure other defaults are still set
    Assert.Equal(PageType.LeafNode, header.PageType);
  }

  [Fact]
  public void Initialize_AsInternalNode_SetsDefaultHeaderCorrectly()
  {
    // Arrange
    var page = CreateTestPage();

    // Act
    SlottedPage.Initialize(page, PageType.InternalNode);

    // Assert
    var header = new PageHeader(page);

    Assert.Equal(0L, header.PageLsn);
    Assert.Equal(0, header.ItemCount);
    Assert.Equal(Page.Size, header.DataStartOffset);
    Assert.Equal(PageHeader.INVALID_PAGE_INDEX, header.ParentPageIndex);
    Assert.Equal(PageType.InternalNode, header.PageType);
    // Check internal-specific field is set to its "null" state
    Assert.Equal(PageHeader.INVALID_PAGE_INDEX, header.RightmostChildPageIndex);
  }

  [Fact]
  public void Initialize_WithInvalidPageType_ThrowsArgumentException()
  {
    // Arrange
    var page = CreateTestPage();

    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("pageType", () =>
        SlottedPage.Initialize(page, PageType.Invalid)
    );
    Assert.Contains("invalid or unsupported page type", ex.Message);
  }

  [Fact]
  public void Initialize_WithNullPage_ThrowsArgumentNullException()
  {
    // Arrange
    Page? nullPage = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("page", () =>
        // Use null-forgiving operator (!) as we are intentionally testing the null case
        SlottedPage.Initialize(nullPage!, PageType.LeafNode)
    );
  }

  // Helper to create a blank test page.
  // We fill it with a non-zero value to ensure our Initialize method is actually writing zeros.
  private static Page CreateTestPage()
  {
    var buffer = new byte[Page.Size];
    Array.Fill(buffer, (byte)0xFF); // Pre-fill with non-zero data
    var memory = new Memory<byte>(buffer);
    // PageId doesn't matter for formatting tests, so use a default value.
    return new Page(default, memory);
  }
}