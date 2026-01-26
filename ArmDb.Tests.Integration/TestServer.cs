using System.Net;
using System.Net.Sockets;
using ArmDb.Server;
using ArmDb.Common.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;

namespace ArmDb.Tests.Integration;

public class TestServer : IAsyncDisposable
{
  private readonly CancellationTokenSource _cts = new();
  private readonly int _port;
  private readonly IFileSystem _fileSystem;
  private readonly string _baseDataPath; // Store the temp path
  private Task? _serverTask;
  private ServerListener? _listener;

  public int Port => _port;

  public TestServer(IFileSystem fileSystem)
  {
    _port = GetFreePort();
    _fileSystem = fileSystem;

    // Create a safe, isolated temporary directory for this server instance
    _baseDataPath = Path.Combine(Path.GetTempPath(), $"ArmDb_TestServer_{Guid.NewGuid()}");
  }

  public async Task StartAsync()
  {
    // Use the factory method to create the listener
    _listener = await ServerListener.CreateServerListener(
        _port,
        _fileSystem,
        _baseDataPath,
        NullLoggerFactory.Instance
    );

    _serverTask = _listener.StartAsync(_cts.Token);

    // Give it a moment to bind/start listening
    await Task.Delay(100);
  }

  public async ValueTask DisposeAsync()
  {
    _cts.Cancel();
    if (_serverTask != null)
    {
      try
      {
        await _serverTask;
      }
      catch (OperationCanceledException)
      {
        // Expected shutdown
      }
    }
    _cts.Dispose();

    // Note: ServerListener doesn't currently expose a Dispose method to clean up 
    // the StorageEngine, but in a real implementation we might want that.
    // For tests, the process teardown or file system cleanup usually suffices.
  }

  private static int GetFreePort()
  {
    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    int port = ((IPEndPoint)listener.LocalEndpoint).Port;
    return port;
  }
}