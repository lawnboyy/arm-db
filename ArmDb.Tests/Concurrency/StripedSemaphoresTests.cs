using ArmDb.Concurrency;

namespace ArmDb.UnitTests.Concurrency;

public class StripedSemaphoresTests
{
  [Fact]
  public async Task Indexer_SameKey_ReturnsSameSemaphoreEnforcingMutualExclusion()
  {
    // Arrange
    var locks = new StripedSemaphoreMap<string>(16);
    string key = "resource-A";
    int sharedCounter = 0;
    int taskCount = 10;

    // Act
    var tasks = Enumerable.Range(0, taskCount).Select(async _ =>
    {
      // API Change: Use the clean indexer syntax
      SemaphoreSlim sem = locks[key];

      await sem.WaitAsync();
      try
      {
        int temp = sharedCounter;
        await Task.Delay(5);
        sharedCounter = temp + 1;
      }
      finally
      {
        sem.Release();
      }
    });

    await Task.WhenAll(tasks);

    // Assert
    Assert.Equal(taskCount, sharedCounter);
  }
}