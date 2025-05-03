using ArmDb.StorageEngine; // Target namespace

namespace ArmDb.UnitTests.StorageEngine; // Test namespace

public partial class PageTests
{
  // Helper to create a test page with a writeable buffer
  private static (Page page, byte[] buffer) CreateTestPage(long pageId = 0)
  {
    var buffer = new byte[Page.Size]; // Standard size
    var memory = new Memory<byte>(buffer);
    var page = new Page(pageId, memory);
    return (page, buffer);
  }
}