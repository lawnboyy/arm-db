using System.Net;
using System.Net.Sockets;
using ArmDb.Common.Abstractions;
using ArmDb.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArmDb.Server;

internal class ServerListener
{
  private readonly int _port;
  private readonly ILoggerFactory _loggerFactory;
  private readonly TcpListener _tcpListener;

  /// <summary>
  /// Singleton instance of our storage engine.
  /// </summary>
  private readonly IStorageEngine _storageEngine;


  private ServerListener(int port, IStorageEngine storageEngine, ILoggerFactory loggerFactory)
  {
    _port = port;
    _loggerFactory = loggerFactory;
    _storageEngine = storageEngine;
    _tcpListener = new TcpListener(IPAddress.Loopback, port);
  }

  internal static async Task<ServerListener> CreateServerListener(int port, IFileSystem fileSystem, string filePath, ILoggerFactory loggerFactory)
  {
    // Create the disk manager...
    var diskLogger = loggerFactory.CreateLogger<DiskManager>();
    var diskManager = new DiskManager(fileSystem, diskLogger, filePath);

    var bpmLogger = loggerFactory.CreateLogger<BufferPoolManager>();
    var bpmOptions = new BufferPoolManagerOptions { PoolSizeInPages = 100000 };
    var bpm = new BufferPoolManager(Options.Create(bpmOptions), diskManager, bpmLogger);

    var storageLogger = loggerFactory.CreateLogger<StorageEngine>();
    var storageEngine = await StorageEngine.CreateStorageEngineAsync(bpm, storageLogger);

    return new ServerListener(port, storageEngine, loggerFactory);
  }

  internal async Task StartAsync(CancellationToken ct = default)
  {
    _tcpListener.Start();

    try
    {
      while (!ct.IsCancellationRequested)
      {
        var client = await _tcpListener.AcceptTcpClientAsync(ct);

        // Fire off a task to handle the connection and continue. Note that we do not await the task
        // so we can accept concurrent connection requests.
        _ = Task.Run(() => HandleClientAsync(client, ct));
      }
    }
    catch (OperationCanceledException)
    {
      // TODO: Do something here...
    }
  }

  private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
  {
    using (client)
    {
      var handler = new ConnectionHandler(client, _storageEngine);
      await handler.RunAsync(ct);
    }
  }
}