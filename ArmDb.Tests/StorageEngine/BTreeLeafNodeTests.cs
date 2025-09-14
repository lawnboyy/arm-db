using ArmDb.DataModel;
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

  [Fact]
  public void Search_WhenKeyExists_ReturnsCorrectDataRow()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);

    var row0 = new DataRow(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row1 = new DataRow(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));
    var row2 = new DataRow(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30"));

    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row0), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row1), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row2), 2);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateInteger(20)]); // Search by key

    // Act
    DataRow? actualRow = leafNode.Search(searchKey); // Use the key-based search method

    // Assert
    Assert.NotNull(actualRow);
    // The Equals method on DataRow compares the content
    Assert.Equal(row1, actualRow);
  }

  [Fact]
  public void Search_WhenKeyIsNotMidpoint_ReturnsCorrectDataRow()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);

    var row0 = new DataRow(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row1 = new DataRow(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));
    var row2 = new DataRow(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30"));
    var row3 = new DataRow(DataValue.CreateInteger(40), DataValue.CreateString("Data for 40"));
    var row4 = new DataRow(DataValue.CreateInteger(50), DataValue.CreateString("Data for 50"));

    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row0), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row1), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row2), 2);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row3), 3);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row4), 4);

    var leafNode = new BTreeLeafNode(page, tableDef);
    // Search for a key in the upper half of the data
    var searchKey = new Key([DataValue.CreateInteger(40)]);

    // Act
    DataRow? actualRow = leafNode.Search(searchKey);

    // Assert
    Assert.NotNull(actualRow);
    Assert.Equal(row3, actualRow);
  }

  [Fact]
  public void Search_WhenKeyDoesNotExist_ReturnsNull()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);

    var row0 = new DataRow(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row1 = new DataRow(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));
    var row2 = new DataRow(DataValue.CreateInteger(40), DataValue.CreateString("Data for 40"));

    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row0), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row1), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row2), 2);

    var leafNode = new BTreeLeafNode(page, tableDef);
    // Search for a key that does not exist but falls between existing keys
    var searchKey = new Key([DataValue.CreateInteger(30)]);

    // Act
    DataRow? actualRow = leafNode.Search(searchKey);

    // Assert
    Assert.Null(actualRow);
  }

  [Fact]
  public void Search_AfterDelete_ReturnsNullForDeletedRecord()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Populate with records
    leafNode.TryInsert(new DataRow(DataValue.CreateInteger(10), DataValue.CreateString("Data A")));
    leafNode.TryInsert(new DataRow(DataValue.CreateInteger(20), DataValue.CreateString("Data B")));
    leafNode.TryInsert(new DataRow(DataValue.CreateInteger(30), DataValue.CreateString("Data C")));

    var keyToDelete = new Key([DataValue.CreateInteger(20)]);

    // Act: Delete the record
    bool deleteSuccess = leafNode.Delete(keyToDelete);
    Assert.True(deleteSuccess, "The delete operation should have been successful.");

    // Act: Search for the now-deleted record
    DataRow? result = leafNode.Search(keyToDelete);

    // Assert
    Assert.Null(result);
  }

  [Fact]
  public void Search_AfterDelete_FindsRemainingRecordCorrectly()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    var row10 = new DataRow(DataValue.CreateInteger(10), DataValue.CreateString("Data A"));
    var row20 = new DataRow(DataValue.CreateInteger(20), DataValue.CreateString("Data B")); // To be deleted
    var row30 = new DataRow(DataValue.CreateInteger(30), DataValue.CreateString("Data C"));

    leafNode.TryInsert(row10);
    leafNode.TryInsert(row20);
    leafNode.TryInsert(row30);

    var keyToDelete = new Key([DataValue.CreateInteger(20)]);
    var keyToFind = new Key([DataValue.CreateInteger(30)]);

    // Act: Delete a record
    bool deleteSuccess = leafNode.Delete(keyToDelete);
    Assert.True(deleteSuccess, "The delete operation should have been successful.");

    // Act: Search for a different, remaining record
    DataRow? result = leafNode.Search(keyToFind);

    // Assert
    Assert.NotNull(result);
    Assert.Equal(row30, result);
  }

  private static TableDefinition CreateTestTable()
  {
    var tableDef = new TableDefinition("TestUsers");
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("Name", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: true));
    return tableDef;
  }

  private static Page CreateTestPage(PageId? pageId = null)
  {
    var buffer = new byte[Page.Size];
    PageId newPageId = (pageId == null) ? new PageId(1, 0) : pageId.Value;

    return new Page(newPageId, buffer.AsMemory());
  }

  private static TableDefinition CreateIntPKTable()
  {
    var tableDef = new TableDefinition("IntPKTable");
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Varchar, 100), isNullable: true));
    tableDef.AddConstraint(new PrimaryKeyConstraint("IntPKTable", ["Id"]));
    return tableDef;
  }

  private static TableDefinition CreateCompositePKTable()
  {
    var tableDef = new TableDefinition("CompositePKTable");
    tableDef.AddColumn(new ColumnDefinition("OrgName", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("EmployeeId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("CompositePKTable", new[] { "OrgName", "EmployeeId" }));
    return tableDef;
  }

  private static TableDefinition CreateCompositePKTableWithIsActive()
  {
    var tableDef = new TableDefinition("CompositePKTable");
    tableDef.AddColumn(new ColumnDefinition("OrgName", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("EmployeeId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("CompositePKTable", new[] { "OrgName", "EmployeeId" }));
    return tableDef;
  }

  private static TableDefinition CreateComplexCompositePKTable()
  {
    var tableDef = new TableDefinition("ComplexCompositePKTable");
    // Physical column order
    tableDef.AddColumn(new ColumnDefinition("IsActive", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("Department", new DataTypeInfo(PrimitiveDataType.Varchar, 50), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("EmployeeId", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("HireDate", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: false));
    // Primary Key logical order is different
    tableDef.AddConstraint(new PrimaryKeyConstraint("ComplexCompositePKTable", new[] { "Department", "HireDate", "EmployeeId" }));
    return tableDef;
  }
}