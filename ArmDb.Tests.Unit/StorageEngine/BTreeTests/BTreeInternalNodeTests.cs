using ArmDb.SchemaDefinition;
using ArmDb.Storage;

namespace ArmDb.Tests.Unit.Storage.BTreeTests;

public partial class BTreeInternalNodeTests
{
  [Fact]
  public void Constructor_WithValidInternalPage_InitializesSuccessfully()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    // Correctly format the page as an InternalNode
    SlottedPage.Initialize(page, PageType.InternalNode);

    // Act
    var exception = Record.Exception(() => new BTreeInternalNode(page, tableDef));

    // Assert
    Assert.Null(exception); // Should not throw
  }

  [Theory]
  [InlineData(PageType.LeafNode)]
  [InlineData(PageType.Invalid)]
  public void Constructor_WithIncorrectPageType_ThrowsArgumentException(PageType wrongType)
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    // Format the page with the WRONG type
    if (wrongType != PageType.Invalid) // Invalid is the default state of a blank page
    {
      SlottedPage.Initialize(page, wrongType);
    }

    // Act & Assert
    var ex = Assert.Throws<ArgumentException>("page", () => new BTreeInternalNode(page, tableDef));
  }

  private static TableDefinition CreateIntPKTable()
  {
    var tableDef = new TableDefinition("IntPKTable");
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("IntPKTable", ["Id"]));
    return tableDef;
  }

  // Helper to create a blank test page
  private static Page CreateTestPage(int pageIndex = 0)
  {
    var buffer = new byte[Page.Size];
    return new Page(new PageId(1, pageIndex), buffer.AsMemory());
  }

  private static TableDefinition CreateCompositePKTable()
  {
    var tableDef = new TableDefinition("CompositePKTable");
    tableDef.AddColumn(new ColumnDefinition("OrgName", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("EmployeeId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("CompositePKTable", new[] { "OrgName", "EmployeeId" }));
    return tableDef;
  }
}
