using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine;

public static class StorageEngineTestHelper
{
  public static byte[] CreateTestBuffer(byte fillValue, int size = Page.Size)
  {
    var buffer = new byte[size];
    Array.Fill(buffer, fillValue);
    return buffer;
  }
}