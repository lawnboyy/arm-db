namespace ArmDb.Concurrency;

public class StripedSemaphores<TKey>
{
  private readonly int _stripeCount;
  private readonly SemaphoreSlim[] _semaphores;

  public StripedSemaphores(int stripeCount)
  {
    _stripeCount = stripeCount;
    _semaphores = new SemaphoreSlim[stripeCount];

    // Initialize our semaphores...
    for (int i = 0; i < _semaphores.Length; i++)
      _semaphores[i] = new SemaphoreSlim(1, 1);
  }

  public SemaphoreSlim this[TKey key]
  {
    get
    {
      if (key == null)
        throw new ArgumentNullException(nameof(key));

      // Ensure that we have a positive hash that is within the bounds of the semaphore array.
      // 0x7FFFFFFF is the binary mask 0111...1111 (all 1s except the sign bit)
      int index = (key.GetHashCode() & 0x7FFFFFFF) % _stripeCount;
      return _semaphores[index];
    }
  }
}
