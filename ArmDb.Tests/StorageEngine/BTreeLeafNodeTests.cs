using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public partial class BTreeLeafNodeTests
{
  [Fact]
  public void Constructor_WithValidLeafPage_InitializesSuccessfully()
  {
    // Arrange
    var page = CreateTestPage();
    var tableDef = CreateTestTable();
    SlottedPage.Initialize(page, PageType.LeafNode); // Correctly format the page

    // Act
    var exception = Record.Exception(() => new BTreeLeafNode(page, tableDef));

    // Assert
    Assert.Null(exception); // Should not throw
  }

  [Fact]
  public void Constructor_WithIncorrectPageType_ThrowsArgumentException()
  {
    // Arrange
    var page = CreateTestPage();
    var tableDef = CreateTestTable();
    SlottedPage.Initialize(page, PageType.InternalNode); // Format as the WRONG type

    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("page", () => new BTreeLeafNode(page, tableDef));
    Assert.Contains("Expected a leaf node page", ex.Message);
  }

  [Fact]
  public void Constructor_WithUninitializedPage_ThrowsArgumentException()
  {
    // Arrange
    var page = CreateTestPage(); // Page is unformatted, PageType will be Invalid (0)
    var tableDef = CreateTestTable();

    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("page", () => new BTreeLeafNode(page, tableDef));
    Assert.Contains("Received an invalid Page", ex.Message);
  }

  [Fact]
  public void Constructor_WithNullPage_ThrowsArgumentNullException()
  {
    // Arrange
    Page? nullPage = null;
    var tableDef = CreateTestTable();

    // Act & Assert
    Assert.Throws<ArgumentNullException>("page", () => new BTreeLeafNode(nullPage!, tableDef));
  }

  [Fact]
  public void Constructor_WithNullTableDefinition_ThrowsArgumentNullException()
  {
    // Arrange
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    TableDefinition? nullTableDef = null;

    // Act & Assert
    Assert.Throws<ArgumentNullException>("tableDefinition", () => new BTreeLeafNode(page, nullTableDef!));
  }

  private static TableDefinition CreateTestTable()
  {
    var tableDef = new TableDefinition("TestUsers");
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: true));
    return tableDef;
  }

  private static Page CreateTestPage()
  {
    var buffer = new byte[Page.Size];
    return new Page(new PageId(1, 0), buffer.AsMemory());
  }
}