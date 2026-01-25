// using System.Net;
// using System.Net.Sockets;
// using ArmDb.Server;
// using Microsoft.Extensions.Logging.Abstractions;

// namespace ArmDb.IntegrationTests;

// public class TestServer : IAsyncDisposable
// {
//   private readonly CancellationTokenSource _cts = new();
//   private readonly int _port;
//   private Task? _serverTask;
//   private readonly ServerListener _listener; // The actual server component

//   public int Port => _port;

//   public TestServer()
//   {
//     _port = GetFreePort();
//     // We'll need to implement ServerListener in the main project soon.
//     // For now, assume it takes a port and a logger.
//     _listener = new ServerListener(_port, NullLogger<ServerListener>.Instance);
//   }

//   public async Task StartAsync()
//   {
//     _serverTask = _listener.StartAsync(_cts.Token);
//     // Give it a moment to bind
//     await Task.Delay(100);
//   }

//   public async ValueTask DisposeAsync()
//   {
//     _cts.Cancel();
//     if (_serverTask != null)
//     {
//       try
//       {
//         await _serverTask;
//       }
//       catch (OperationCanceledException)
//       {
//         // Expected shutdown
//       }
//     }
//     _cts.Dispose();
//   }

//   private static int GetFreePort()
//   {
//     using var listener = new TcpListener(IPAddress.Loopback, 0);
//     listener.Start();
//     int port = ((IPEndPoint)listener.LocalEndpoint).Port;
//     return port;
//   }
// }