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
    var expectedSemaphore = locks[key];

    // Act
    var tasks = Enumerable.Range(0, taskCount).Select(async _ =>
    {
      // API Change: Use the clean indexer syntax
      SemaphoreSlim sem = locks[key];
      Assert.Same(expectedSemaphore, sem);

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

  [Fact]
  public async Task Indexer_DifferentKeys_DifferentStripes_RunConcurrently()
  {
    // Arrange
    // Use a large stripe count (1024) to effectively guarantee different keys (1 and 2) hit different buckets
    var locks = new StripedSemaphoreMap<int>(stripeCount: 1024);
    var tcs1 = new TaskCompletionSource<bool>();
    var tcs2 = new TaskCompletionSource<bool>();

    // Act
    // Task 1: Grabs Lock 1, signals tcs1, then waits for tcs2
    var task1 = Task.Run(async () =>
    {
      var sem = locks[1];
      await sem.WaitAsync();
      try
      {
        tcs1.SetResult(true); // Signal we are inside Critical Section 1
        await tcs2.Task;      // Wait for Task 2 to enter Critical Section 2
      }
      finally
      {
        sem.Release();
      }
    });

    // Task 2: Waits for tcs1, Grabs Lock 2, signals tcs2
    var task2 = Task.Run(async () =>
    {
      await tcs1.Task;      // Wait until Task 1 is inside Critical Section 1
      var sem = locks[2];
      await sem.WaitAsync(); // Should not block if striping is working correctly
      try
      {
        tcs2.SetResult(true); // Signal we are inside Critical Section 2
      }
      finally
      {
        sem.Release();
      }
    });

    var allTasks = Task.WhenAll(task1, task2);

    // If concurrency fails (e.g., if the implementation accidentally used a global lock), 
    // this will timeout because Task 1 would hold the lock and wait for Task 2, 
    // but Task 2 would be waiting for the lock held by Task 1.
    var completedTask = await Task.WhenAny(allTasks, Task.Delay(2000));

    // Assert
    Assert.Equal(allTasks, completedTask); // If Delay won, we deadlocked or timed out
    Assert.True(task1.IsCompletedSuccessfully);
    Assert.True(task2.IsCompletedSuccessfully);
  }

  [Fact]
  public async Task Indexer_HashCollision_DifferentKeysSameStripe_EnforcesMutualExclusion()
  {
    // Arrange
    // Force collisions by using a single stripe.
    // Key "A" and Key "B" are different, but MUST share the same semaphore.
    var locks = new StripedSemaphoreMap<string>(stripeCount: 1);
    int activeCount = 0;
    bool exclusionFailed = false;

    // Act
    var task1 = Task.Run(async () =>
    {
      var sem = locks["KeyA"];
      await sem.WaitAsync();
      try
      {
        activeCount++;
        await Task.Delay(50); // Hold lock to widen race window
        if (activeCount > 1) exclusionFailed = true;
        activeCount--;
      }
      finally
      {
        sem.Release();
      }
    });

    var task2 = Task.Run(async () =>
    {
      var sem = locks["KeyB"];
      await sem.WaitAsync();
      try
      {
        activeCount++;
        await Task.Delay(50);
        if (activeCount > 1) exclusionFailed = true;
        activeCount--;
      }
      finally
      {
        sem.Release();
      }
    });

    await Task.WhenAll(task1, task2);

    // Assert
    Assert.False(exclusionFailed, "Two threads entered critical section simultaneously despite hash collision.");
  }

  [Fact]
  public void Indexer_NegativeHashCode_DoesNotThrowIndexOutOfRangeException()
  {
    // Arrange
    var locks = new StripedSemaphoreMap<int>(stripeCount: 16);

    // Act & Assert
    // int.MinValue is -2147483648. 
    // Math.Abs(int.MinValue) throws OverflowException.
    // Simple modulo (%) returns negative index in C#.
    // The implementation must handle this (e.g. bit masking).
    var exception = Record.Exception(() =>
    {
      var sem = locks[int.MinValue];
    });

    Assert.Null(exception);
    Assert.NotNull(locks[int.MinValue]);
  }

  [Fact]
  public void Constructor_InvalidStripeCount_ThrowsArgumentOutOfRangeException()
  {
    Assert.Throws<ArgumentOutOfRangeException>(() => new StripedSemaphoreMap<string>(0));
    Assert.Throws<ArgumentOutOfRangeException>(() => new StripedSemaphoreMap<string>(-1));
  }
}