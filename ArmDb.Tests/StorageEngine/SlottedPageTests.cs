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

  [Fact]
  public void GetFreeSpace_OnFreshlyInitializedPage_ReturnsCorrectSize()
  {
    // Arrange
    var page = CreateTestPage();
    // Initialize the page to a known empty state
    SlottedPage.Initialize(page, PageType.LeafNode);
    int expectedFreeSpace = Page.Size - PageHeader.HEADER_SIZE;

    // Act
    int actualFreeSpace = SlottedPage.GetFreeSpace(page);

    // Assert
    Assert.Equal(expectedFreeSpace, actualFreeSpace);
  }

  [Fact]
  public void GetFreeSpace_WithSomeSlotsAndData_ReturnsCorrectSize()
  {
    // Arrange
    var page = CreateTestPage();
    var header = new PageHeader(page); // Get a view to manipulate header state for the test

    // Simulate a page with 3 records taking up a total of 150 bytes of data
    int itemCount = 3;
    int dataOnPageBytes = 150;
    int dataStartOffset = Page.Size - dataOnPageBytes; // e.g., 8192 - 150 = 8042

    header.ItemCount = itemCount;
    header.DataStartOffset = dataStartOffset;

    int slotsSize = itemCount * Slot.Size; // 3 * 8 = 24 bytes
    int endOfSlots = PageHeader.HEADER_SIZE + slotsSize; // 32 + 24 = 56
    int expectedFreeSpace = dataStartOffset - endOfSlots; // 8042 - 56 = 7986

    // Act
    int actualFreeSpace = SlottedPage.GetFreeSpace(page);

    // Assert
    Assert.Equal(expectedFreeSpace, actualFreeSpace);
  }

  [Fact]
  public void GetFreeSpace_WhenPageIsFull_ReturnsZero()
  {
    // Arrange
    var page = CreateTestPage();
    var header = new PageHeader(page);

    // Simulate a state where the free space has been completely used up
    int itemCount = 10;
    int slotsSize = itemCount * Slot.Size; // 10 * 8 = 80 bytes
    int endOfSlots = PageHeader.HEADER_SIZE + slotsSize; // 32 + 80 = 112

    // Set the data pointer to be exactly at the end of the slot array
    header.ItemCount = itemCount;
    header.DataStartOffset = endOfSlots;

    // Act
    int actualFreeSpace = SlottedPage.GetFreeSpace(page);

    // Assert
    Assert.Equal(0, actualFreeSpace);
  }

  [Fact]
  public void GetFreeSpace_OnCorruptedPage_WherePointersCrossed_ReturnsZero()
  {
    // Arrange
    var page = CreateTestPage();
    var header = new PageHeader(page);

    // Simulate corruption where the data heap pointer has moved past the slot array
    header.ItemCount = 20; // Slots take up 32 + 20*8 = 192 bytes
    header.DataStartOffset = 100; // Data pointer is *before* end of slots

    // The raw calculation inside GetFreeSpace would be 100 - 192 = -92

    // Act
    int actualFreeSpace = SlottedPage.GetFreeSpace(page);

    // Assert
    // The Math.Max(0, ...) check in the implementation should cap the result at 0
    Assert.Equal(0, actualFreeSpace);
  }

  [Fact]
  public void GetFreeSpace_WithNullPage_ThrowsArgumentNullException()
  {
    // Arrange
    Page? nullPage = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("page", () =>
        // Use null-forgiving operator (!) as we are intentionally testing the null case
        SlottedPage.GetFreeSpace(nullPage!)
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