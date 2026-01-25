using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.Storage;

namespace ArmDb.Tests.Unit.Storage.BTreeTests;

public partial class BTreeLeafNodeTests
{
  [Theory]
  // Key to Search For, Expected Slot Index
  [InlineData(40, 3)] // The initial midpoint
  [InlineData(20, 1)] // A value in the lower half
  [InlineData(60, 5)] // A value in the upper half
  [InlineData(10, 0)] // The first element
  [InlineData(70, 6)] // The last element
  public void FindSlotIndex_WhenKeyExists_IteratesAndReturnsCorrectIndex(int keyToFind, int expectedIndex)
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var nullVarchar = DataValue.CreateNull(PrimitiveDataType.Varchar);

    // Populate the page with a larger, odd number of sorted records
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(10), nullVarchar)), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(20), nullVarchar)), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(30), nullVarchar)), 2);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(40), nullVarchar)), 3);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(50), nullVarchar)), 4);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(60), nullVarchar)), 5);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(70), nullVarchar)), 6);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateInteger(keyToFind)]);

    // Act
    int foundIndex = leafNode.FindPrimaryKeySlotIndex(searchKey);

    // Assert
    Assert.Equal(expectedIndex, foundIndex);
  }

  [Theory]
  // Key to Search For, Expected Insertion Index
  [InlineData(5, 0)]  // Smaller than all existing keys
  [InlineData(25, 2)] // Fits between 20 and 30
  [InlineData(45, 4)] // Fits between 40 and 50
  [InlineData(75, 7)] // Larger than all existing keys
  public void FindSlotIndex_WhenKeyNotFound_ReturnsBitwiseComplementOfInsertionIndex(int keyToFind, int expectedInsertionIndex)
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var nullVarchar = DataValue.CreateNull(PrimitiveDataType.Varchar);

    // Populate page with gaps in the key sequence
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(10), nullVarchar)), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(20), nullVarchar)), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(30), nullVarchar)), 2);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(40), nullVarchar)), 3);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(50), nullVarchar)), 4);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(60), nullVarchar)), 5);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(70), nullVarchar)), 6);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateInteger(keyToFind)]);

    // Act
    int result = leafNode.FindPrimaryKeySlotIndex(searchKey);

    // Assert
    // 1. Verify the result is negative, indicating "not found"
    Assert.True(result < 0, "Result should be negative when key is not found.");

    // 2. Apply the bitwise complement to get the insertion index
    int insertionIndex = ~result;

    // 3. Verify the insertion index is correct
    Assert.Equal(expectedInsertionIndex, insertionIndex);
  }

  [Theory]
  [InlineData("Sales", 50, 2)]     // Initial midpoint
  [InlineData("Engineering", 101, 0)] // First element
  [InlineData("Support", 80, 4)]       // Last element
  [InlineData("HR", 20, 1)]            // Lower half
  [InlineData("Sales", 52, 3)]         // Upper half
  public void FindSlotIndex_WithCompositeKey_WhenKeyExists_ReturnsCorrectIndex(string orgName, int employeeId, int expectedIndex)
  {
    // Arrange
    var tableDef = CreateCompositePKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);

    // Populate with a larger set of sorted composite keys
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("Engineering"), DataValue.CreateInteger(101))), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("HR"), DataValue.CreateInteger(20))), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("Sales"), DataValue.CreateInteger(50))), 2);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("Sales"), DataValue.CreateInteger(52))), 3);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("Support"), DataValue.CreateInteger(80))), 4);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateString(orgName), DataValue.CreateInteger(employeeId)]);

    // Act
    int foundIndex = leafNode.FindPrimaryKeySlotIndex(searchKey);

    // Assert
    Assert.Equal(expectedIndex, foundIndex);
  }

  [Theory]
  [InlineData("Dev", 1, 0)]         // Before all
  [InlineData("Engineering", 102, 1)] // After ("Engineering", 101)
  [InlineData("Sales", 51, 3)]      // Between ("Sales", 50) and ("Sales", 52)
  [InlineData("Support", 90, 5)]    // After all
  public void FindSlotIndex_WithCompositeKey_WhenKeyNotFound_ReturnsBitwiseComplementOfInsertionIndex(string orgName, int employeeId, int expectedInsertionIndex)
  {
    // Arrange
    var tableDef = CreateCompositePKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);

    // Populate with the same larger set of sorted composite keys
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("Engineering"), DataValue.CreateInteger(101))), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("HR"), DataValue.CreateInteger(20))), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("Sales"), DataValue.CreateInteger(50))), 2);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("Sales"), DataValue.CreateInteger(52))), 3);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateString("Support"), DataValue.CreateInteger(80))), 4);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateString(orgName), DataValue.CreateInteger(employeeId)]);

    // Act
    int result = leafNode.FindPrimaryKeySlotIndex(searchKey);

    // Assert
    Assert.True(result < 0);
    int insertionIndex = ~result;
    Assert.Equal(expectedInsertionIndex, insertionIndex);
  }

  [Theory]
  // Search Key: (Department, HireDate, EmployeeId), Expected Result (Index or ~InsertionIndex)
  [InlineData("Sales", "2024-02-10", 52, 2)] // Exact match
  [InlineData("HR", "2023-01-15", 101, 0)]   // Exact match (first element)
  [InlineData("Sales", "2024-02-10", 55, 3)] // Exact match (last element)
  [InlineData("Admin", "2025-01-01", 1, ~0)] // Not Found: Before all
  [InlineData("Sales", "2024-02-10", 53, ~3)] // Not Found: Between two keys
  [InlineData("Tech", "2025-01-01", 1, ~4)]   // Not Found: After all
  public void FindSlotIndex_WithComplexReorderedCompositeKey_ReturnsCorrectResult(string dept, string hireDateStr, int empId, int expectedResult)
  {
    // Arrange
    var tableDef = CreateComplexCompositePKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var hireDate = DateTime.Parse(hireDateStr).ToUniversalTime();

    // Populate with sorted composite keys (Department, HireDate, EmployeeId)
    // Physical row order: (IsActive, Department, EmployeeId, HireDate)
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateBoolean(true), DataValue.CreateString("HR"), DataValue.CreateInteger(101), DataValue.CreateDateTime(DateTime.Parse("2023-01-15").ToUniversalTime()))), 0);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateBoolean(true), DataValue.CreateString("Sales"), DataValue.CreateInteger(50), DataValue.CreateDateTime(DateTime.Parse("2022-05-20").ToUniversalTime()))), 1);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateBoolean(false), DataValue.CreateString("Sales"), DataValue.CreateInteger(52), DataValue.CreateDateTime(DateTime.Parse("2024-02-10").ToUniversalTime()))), 2);
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateBoolean(true), DataValue.CreateString("Sales"), DataValue.CreateInteger(55), DataValue.CreateDateTime(DateTime.Parse("2024-02-10").ToUniversalTime()))), 3);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateString(dept), DataValue.CreateDateTime(hireDate), DataValue.CreateInteger(empId)]);

    // Act
    int actualResult = leafNode.FindPrimaryKeySlotIndex(searchKey);

    // Assert
    Assert.Equal(expectedResult, actualResult);
  }

  [Fact]
  public void FindSlotIndex_OnEmptyPage_ReturnsCorrectInsertionIndex()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode); // Page is empty, ItemCount is 0

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateInteger(100)]); // Any key
    int expectedResult = ~0; // Bitwise complement of insertion index 0

    // Act
    int actualResult = leafNode.FindPrimaryKeySlotIndex(searchKey);

    // Assert
    Assert.Equal(expectedResult, actualResult);
  }

  [Theory]
  // Key to search for, Expected result (0 for match, ~0 for insert before, ~1 for insert after)
  [InlineData(25, ~0)]
  [InlineData(50, 0)]
  [InlineData(75, ~1)]
  public void FindSlotIndex_OnSingleItemPage_ReturnsCorrectResults(int keyToFind, int expectedResult)
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var nullVarchar = DataValue.CreateNull(PrimitiveDataType.Varchar);

    // Populate with a single record
    SlottedPage.TryAddRecord(page, RecordSerializer.Serialize(tableDef.Columns, new ArmDb.DataModel.Record(DataValue.CreateInteger(50), nullVarchar)), 0);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateInteger(keyToFind)]);

    // Act
    int actualResult = leafNode.FindPrimaryKeySlotIndex(searchKey);

    // Assert
    Assert.Equal(expectedResult, actualResult);
  }

  [Fact]
  public void FindSlotIndex_WhenRecordOnPageHasNullKey_ThrowsInvalidDataException()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);

    // Manually construct a corrupted record where the PK (Id) is marked as NULL
    var corruptedRow = new ArmDb.DataModel.Record(DataValue.CreateNull(PrimitiveDataType.Int), DataValue.CreateString("Corrupted"));
    var corruptedBytes = RecordSerializer.Serialize(tableDef.Columns, corruptedRow);
    SlottedPage.TryAddRecord(page, corruptedBytes, 0);

    var leafNode = new BTreeLeafNode(page, tableDef);
    var searchKey = new Key([DataValue.CreateInteger(10)]); // Any search key

    // Act & Assert
    // The exception should come from RecordSerializer.DeserializePrimaryKey when it finds the null
    var ex = Assert.Throws<InvalidDataException>(() => leafNode.FindPrimaryKeySlotIndex(searchKey));
    Assert.Contains("Primary key column 'Id' cannot be null", ex.Message);
  }
}