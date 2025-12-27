using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.StorageEngine.BTreeTests;

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
    var exception = Xunit.Record.Exception(() => new BTreeLeafNode(page, tableDef));

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

    var row0 = new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row1 = new ArmDb.DataModel.Record(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));
    var row2 = new ArmDb.DataModel.Record(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30"));

    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row0), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row1), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row2), 2);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateInteger(20)]); // Search by key

    // Act
    ArmDb.DataModel.Record? actualRow = leafNode.Search(searchKey); // Use the key-based search method

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

    var row0 = new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row1 = new ArmDb.DataModel.Record(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));
    var row2 = new ArmDb.DataModel.Record(DataValue.CreateInteger(30), DataValue.CreateString("Data for 30"));
    var row3 = new ArmDb.DataModel.Record(DataValue.CreateInteger(40), DataValue.CreateString("Data for 40"));
    var row4 = new ArmDb.DataModel.Record(DataValue.CreateInteger(50), DataValue.CreateString("Data for 50"));

    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row0), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row1), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row2), 2);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row3), 3);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row4), 4);

    var leafNode = new BTreeLeafNode(page, tableDef);
    // Search for a key in the upper half of the data
    var searchKey = new Key([DataValue.CreateInteger(40)]);

    // Act
    ArmDb.DataModel.Record? actualRow = leafNode.Search(searchKey);

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

    var row0 = new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data for 10"));
    var row1 = new ArmDb.DataModel.Record(DataValue.CreateInteger(20), DataValue.CreateString("Data for 20"));
    var row2 = new ArmDb.DataModel.Record(DataValue.CreateInteger(40), DataValue.CreateString("Data for 40"));

    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row0), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row1), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, row2), 2);

    var leafNode = new BTreeLeafNode(page, tableDef);
    // Search for a key that does not exist but falls between existing keys
    var searchKey = new Key([DataValue.CreateInteger(30)]);

    // Act
    ArmDb.DataModel.Record? actualRow = leafNode.Search(searchKey);

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
    leafNode.TryInsert(new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data A")));
    leafNode.TryInsert(new ArmDb.DataModel.Record(DataValue.CreateInteger(20), DataValue.CreateString("Data B")));
    leafNode.TryInsert(new ArmDb.DataModel.Record(DataValue.CreateInteger(30), DataValue.CreateString("Data C")));

    var keyToDelete = new Key([DataValue.CreateInteger(20)]);

    // Act: Delete the record
    bool deleteSuccess = leafNode.Delete(keyToDelete);
    Assert.True(deleteSuccess, "The delete operation should have been successful.");

    // Act: Search for the now-deleted record
    ArmDb.DataModel.Record? result = leafNode.Search(keyToDelete);

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

    var row10 = new ArmDb.DataModel.Record(DataValue.CreateInteger(10), DataValue.CreateString("Data A"));
    var row20 = new ArmDb.DataModel.Record(DataValue.CreateInteger(20), DataValue.CreateString("Data B")); // To be deleted
    var row30 = new ArmDb.DataModel.Record(DataValue.CreateInteger(30), DataValue.CreateString("Data C"));

    leafNode.TryInsert(row10);
    leafNode.TryInsert(row20);
    leafNode.TryInsert(row30);

    var keyToDelete = new Key([DataValue.CreateInteger(20)]);
    var keyToFind = new Key([DataValue.CreateInteger(30)]);

    // Act: Delete a record
    bool deleteSuccess = leafNode.Delete(keyToDelete);
    Assert.True(deleteSuccess, "The delete operation should have been successful.");

    // Act: Search for a different, remaining record
    ArmDb.DataModel.Record? result = leafNode.Search(keyToFind);

    // Assert
    Assert.NotNull(result);
    Assert.Equal(row30, result);
  }

  [Fact]
  public void GetAllRawRecords_ReturnsAllRawDataInOrder() // Renamed test
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    var row10 = new Record(DataValue.CreateInteger(10), DataValue.CreateString("Data 10"));
    var row20 = new Record(DataValue.CreateInteger(20), DataValue.CreateString("Data 20"));
    var row30 = new Record(DataValue.CreateInteger(30), DataValue.CreateString("Data 30"));

    var row10Bytes = RecordSerializer.Serialize(tableDef.Columns, row10);
    var row20Bytes = RecordSerializer.Serialize(tableDef.Columns, row20);
    var row30Bytes = RecordSerializer.Serialize(tableDef.Columns, row30);

    SlottedPage.TryAddRecord(page, row10Bytes, 0);
    SlottedPage.TryAddRecord(page, row20Bytes, 1);
    SlottedPage.TryAddRecord(page, row30Bytes, 2);

    // Act
    var rawRecords = leafNode.GetAllRawRecords(); // Renamed method call

    // Assert
    Assert.NotNull(rawRecords);
    Assert.Equal(3, rawRecords.Count);

    // Check raw data for each entry
    Assert.True(row10Bytes.AsSpan().SequenceEqual(rawRecords[0]));
    Assert.True(row20Bytes.AsSpan().SequenceEqual(rawRecords[1]));
    Assert.True(row30Bytes.AsSpan().SequenceEqual(rawRecords[2]));
  }

  [Fact]
  public void GetAllRawRecords_OnEmptyPage_ReturnsEmptyList()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // Act
    var rawRecords = leafNode.GetAllRawRecords(); // Renamed method call

    // Assert
    Assert.NotNull(rawRecords);
    Assert.Empty(rawRecords);
  }

  [Fact]
  public void Repopulate_WithValidData_CorrectlyWipesAndReloadsPage()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // 1. Add some "original" data to the page that must be wiped
    var originalRow = new Record(DataValue.CreateInteger(10), DataValue.CreateString("Original Data"));
    leafNode.TryInsert(originalRow);

    // 2. Prepare the new, sorted list of records to be loaded
    var newRow100 = new Record(DataValue.CreateInteger(100), DataValue.CreateString("New A"));
    var newRow200 = new Record(DataValue.CreateInteger(200), DataValue.CreateString("New B"));

    var newRecords = new List<Record> { newRow100, newRow200 };

    // The orchestrator would serialize the records to pass to Repopulate
    var newRawRecords = newRecords
        .Select(r => RecordSerializer.Serialize(tableDef.Columns, r))
        .ToList();

    // Act
    // Call the Repopulate method (which you will implement)
    leafNode.Repopulate(newRawRecords);

    // Assert
    // 1. Verify the item count is correct
    Assert.Equal(2, leafNode.ItemCount);

    // 2. Verify the old data is gone
    var oldKey = new Key([DataValue.CreateInteger(10)]);
    Assert.Null(leafNode.Search(oldKey));

    // 3. Verify the new data is present and in the correct order
    var newKey100 = new Key([DataValue.CreateInteger(100)]);
    var newKey200 = new Key([DataValue.CreateInteger(200)]);

    Assert.Equal(newRow100, leafNode.Search(newKey100));
    Assert.Equal(newRow200, leafNode.Search(newKey200));

    // 4. Verify the slots are in the correct order by checking the raw data
    var entries = leafNode.GetAllRawRecords(); // Renamed method call
    Assert.True(newRawRecords[0].AsSpan().SequenceEqual(entries[0]));
    Assert.True(newRawRecords[1].AsSpan().SequenceEqual(entries[1]));
  }

  [Fact]
  public void Repopulate_WithEmptyList_CorrectlyWipesPageAndSetsItemCountToZero()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // 1. Add some "original" data to the page that must be wiped
    var originalRow = new Record(DataValue.CreateInteger(10), DataValue.CreateString("Original Data"));
    leafNode.TryInsert(originalRow);
    Assert.Equal(1, leafNode.ItemCount); // Verify setup

    // 2. Prepare an empty list of records
    var emptyRawRecords = new List<byte[]>();
    var oldKey = new Key([DataValue.CreateInteger(10)]);

    // Act
    leafNode.Repopulate(emptyRawRecords);

    // Assert
    // 1. Verify the item count is now zero
    Assert.Equal(0, leafNode.ItemCount);

    // 2. Verify the old data is gone
    Assert.Null(leafNode.Search(oldKey));

    // 3. Verify GetAllRawRecords returns an empty list
    Assert.Empty(leafNode.GetAllRawRecords());

    // 4. Verify the header is correctly reset
    var header = new PageHeader(page);
    Assert.Equal(0, header.ItemCount);
    Assert.Equal(Page.Size, header.DataStartOffset); // Data offset reset to end of page
  }

  [Fact]
  public void Repopulate_WhenDataIsTooLargeForPage_ThrowsInvalidOperationException()
  {
    // Arrange
    var tableDef = CreateIntPKTable(); // Schema: (ID INT, Data VARCHAR)
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // 1. Create a list of records that will not fit on a single page
    // Use a string size that is definitely too large when repeated
    var largeString = new string('A', 3000);
    var newRawRecords = new List<byte[]>();

    // Create 3 large records. 3 * (~3000 bytes + overhead) will not fit in 8KB.
    for (int i = 0; i < 3; i++)
    {
      var row = new Record(DataValue.CreateInteger(i), DataValue.CreateString(largeString));
      newRawRecords.Add(RecordSerializer.Serialize(tableDef.Columns, row));
    }

    // Act & Assert
    // The Repopulate method should throw an exception when SlottedPage.TryAddRecord fails
    // due to lack of space.
    var ex = Assert.Throws<InvalidOperationException>(() =>
        leafNode.Repopulate(newRawRecords)
    );

    Assert.Contains("Data for repopulating is too large to fit on a single page.", ex.Message, StringComparison.OrdinalIgnoreCase);

    // 3. Verify the page was reset but remains empty
    // The Repopulate method should leave the page in a clean, initialized state
    // even after the failed bulk load.
    Assert.Equal(0, leafNode.ItemCount);
    var header = new PageHeader(page);
    Assert.Equal(0, header.ItemCount);
    Assert.Equal(Page.Size, header.DataStartOffset);
  }

  [Fact]
  public void Repopulate_WhenDataIsTooLarge_FailsUpFrontAndDoesNotModifyPage()
  {
    // Arrange
    var tableDef = CreateIntPKTable(); // Schema: (ID INT, Data VARCHAR)
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // 1. Add some "original" data to the page
    var originalRow = new Record(DataValue.CreateInteger(10), DataValue.CreateString("Original Data"));
    var originalRowKey = originalRow.GetPrimaryKey(tableDef);
    leafNode.TryInsert(originalRow);

    // 2. Create a list of new records that is too large to fit on an empty page
    var largeString = new string('A', 3000);
    var newRawRecords = new List<byte[]>();
    for (int i = 0; i < 3; i++) // 3 * ~3000 bytes + overhead > 8KB
    {
      var row = new Record(DataValue.CreateInteger(i), DataValue.CreateString(largeString));
      newRawRecords.Add(RecordSerializer.Serialize(tableDef.Columns, row));
    }

    // 3. Snapshot the original page state
    var pageStateBefore = page.Data.ToArray();

    // Act & Assert
    // 4. Verify the method throws an exception
    var ex = Assert.Throws<InvalidOperationException>(() =>
        leafNode.Repopulate(newRawRecords)
    );

    Assert.Contains("Data for repopulating is too large to fit on a single page.", ex.Message, StringComparison.OrdinalIgnoreCase);

    // 5. CRUCIAL: Verify the page was not modified
    var pageStateAfter = page.Data.ToArray();
    Assert.True(pageStateBefore.SequenceEqual(pageStateAfter), "Page was modified despite data being too large.");

    // 6. Verify the original data is still present
    Assert.Equal(1, leafNode.ItemCount);
    Assert.NotNull(leafNode.Search(originalRowKey));
  }

  [Fact]
  public void Repopulate_WithNullList_ThrowsArgumentNullException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var leafNode = new BTreeLeafNode(page, tableDef);
    List<byte[]>? nullRawRecords = null;

    // Act & Assert
    // Verify the method throws an ArgumentNullException when the list is null
    Assert.Throws<ArgumentNullException>("sortedRawRecords", () =>
        leafNode.Repopulate(nullRawRecords!)
    );
  }

  [Fact]
  public void Repopulate_WhenCalled_PreservesParentPageIndex()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    int expectedParentIndex = 50; // A non-default parent index

    // Initialize the page with the specific parent index
    SlottedPage.Initialize(page, PageType.LeafNode, expectedParentIndex);
    var leafNode = new BTreeLeafNode(page, tableDef);

    // 1. Add some "original" data
    var originalRow = new Record(DataValue.CreateInteger(10), DataValue.CreateString("Original Data"));
    leafNode.TryInsert(originalRow);

    // 2. Prepare a new, valid list of records for repopulation
    var newRow = new Record(DataValue.CreateInteger(100), DataValue.CreateString("New A"));
    var newRawRecords = new List<byte[]>
    {
      RecordSerializer.Serialize(tableDef.Columns, newRow)
    };

    // 3. Verify the parent index before acting
    var headerBefore = new PageHeader(page);
    Assert.Equal(expectedParentIndex, headerBefore.ParentPageIndex);

    // Act
    leafNode.Repopulate(newRawRecords);

    // Assert
    // 1. Verify the data was repopulated
    Assert.Equal(1, leafNode.ItemCount);
    Assert.NotNull(leafNode.Search(newRow.GetPrimaryKey(tableDef)));
    Assert.Null(leafNode.Search(originalRow.GetPrimaryKey(tableDef)));

    // 2. CRUCIAL: Verify the parent page index was preserved
    var headerAfter = new PageHeader(page);
    Assert.Equal(expectedParentIndex, headerAfter.ParentPageIndex);
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

  private static Page CreateTestPage(int pageIndex)
  {
    var buffer = new byte[Page.Size];
    PageId newPageId = new PageId(1, pageIndex);

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