using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.Storage;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.Storage.BTreeTests;

public partial class BTreeNodeTests
{
  [Fact]
  public void GetBytesUsed_OnEmptyPage_ReturnsHeaderSize()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode); // Format the page

    // We test the base class method via a concrete instance
    var node = new BTreeLeafNode(page, tableDef);

    int expectedUsedSpace = PageHeader.HEADER_SIZE;

    // Act
    int actualUsedSpace = node.GetBytesUsed();

    // Assert
    Assert.Equal(expectedUsedSpace, actualUsedSpace);
  }

  [Fact]
  public void GetBytesUsed_OnPageWithData_ReturnsHeaderPlusDataAndSlots()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.LeafNode);
    var node = new BTreeLeafNode(page, tableDef);

    var row1 = new Record(DataValue.CreateInteger(10), DataValue.CreateString("Data A"));
    var row1Bytes = RecordSerializer.Serialize(tableDef.Columns, row1);

    var row2 = new Record(DataValue.CreateInteger(20), DataValue.CreateString("More Data B"));
    var row2Bytes = RecordSerializer.Serialize(tableDef.Columns, row2);

    // Act: Insert records
    node.TryInsert(row1);
    node.TryInsert(row2);

    // Assert: Calculate the exact expected size
    int expectedUsedSpace = PageHeader.HEADER_SIZE;
    expectedUsedSpace += row1Bytes.Length + Slot.Size; // Space for row 1 + its slot
    expectedUsedSpace += row2Bytes.Length + Slot.Size; // Space for row 2 + its slot

    int actualUsedSpace = node.GetBytesUsed();

    Assert.Equal(expectedUsedSpace, actualUsedSpace);
  }

  [Fact]
  public void GetBytesUsed_OnEmptyInternalNode_ReturnsHeaderSize()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode); // Format as InternalNode

    var node = new BTreeInternalNode(page, tableDef);

    int expectedUsedSpace = PageHeader.HEADER_SIZE;

    // Act
    int actualUsedSpace = node.GetBytesUsed();

    // Assert
    Assert.Equal(expectedUsedSpace, actualUsedSpace);
  }

  [Fact]
  public void GetBytesUsed_OnInternalNodeWithData_ReturnsHeaderPlusDataAndSlots()
  {
    // Arrange
    var tableDef = CreateIntPKTable();
    var page = CreateTestPage();
    SlottedPage.Initialize(page, PageType.InternalNode);
    var node = new BTreeInternalNode(page, tableDef);

    // Create and serialize some internal node entries
    var entry1Bytes = BTreeInternalNode.SerializeRecord(new Key([DataValue.CreateInteger(100)]), new PageId(1, 10), tableDef);
    var entry2Bytes = BTreeInternalNode.SerializeRecord(new Key([DataValue.CreateInteger(200)]), new PageId(1, 20), tableDef);

    // Act: Insert records using the low-level slotted page helper
    SlottedPage.TryAddRecord(page, entry1Bytes, 0);
    SlottedPage.TryAddRecord(page, entry2Bytes, 1);

    // Assert: Calculate the exact expected size
    int expectedUsedSpace = PageHeader.HEADER_SIZE;
    expectedUsedSpace += entry1Bytes.Length + Slot.Size; // Space for entry 1 + its slot
    expectedUsedSpace += entry2Bytes.Length + Slot.Size; // Space for entry 2 + its slot

    int actualUsedSpace = node.GetBytesUsed();

    Assert.Equal(expectedUsedSpace, actualUsedSpace);
  }

  private static TableDefinition CreateIntPKTable()
  {
    var tableDef = new TableDefinition("IntPKTable");
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false));
    tableDef.AddColumn(new ColumnDefinition("Data", new DataTypeInfo(PrimitiveDataType.Varchar, 100), isNullable: true));
    tableDef.AddConstraint(new PrimaryKeyConstraint("IntPKTable", new[] { "Id" }));
    return tableDef;
  }

  private static Page CreateTestPage(int pageIndex = 0)
  {
    var buffer = new byte[Page.Size];
    // Pre-fill with 0xFF so we can see changes
    Array.Fill(buffer, (byte)0xFF);
    return new Page(new PageId(1, pageIndex), buffer.AsMemory());
  }
}
