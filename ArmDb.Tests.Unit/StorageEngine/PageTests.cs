using ArmDb.Storage; // Target namespace

namespace ArmDb.Tests.Unit.Storage; // Test namespace

public partial class PageTests
{
  // Helper to create a test page with a writeable buffer
  private static (Page page, byte[] buffer) CreateTestPage(int pageIndex = 0)
  {
    var buffer = new byte[Page.Size]; // Standard size
    var memory = new Memory<byte>(buffer);
    var pageId = new PageId(1, pageIndex); // TableId is 1 for this test
    var page = new Page(pageId, memory);
    return (page, buffer);
  }
}