using ArmDb.Storage;

namespace ArmDb.Tests.Unit.Storage;

public static class StorageEngineTestHelper
{
  public static byte[] CreateTestBuffer(byte fillValue, int size = Page.Size)
  {
    var buffer = new byte[size];
    Array.Fill(buffer, fillValue);
    return buffer;
  }
}