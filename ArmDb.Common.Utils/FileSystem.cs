﻿using ArmDb.Common.Abstractions;
using Microsoft.Win32.SafeHandles;

namespace ArmDb.Common.Utils;

/// <summary>
/// Concrete implementation of IFileSystem using standard System.IO methods.
/// Provides access to the physical file system.
/// </summary>
public sealed class FileSystem : IFileSystem
{
  /// <summary>
  /// Determines whether the specified directory exists.
  /// </summary>
  /// <param name="path">The path to test.</param>
  /// <returns>true if path refers to an existing directory; false if the directory does not exist or an error occurs.</returns>
  public bool DirectoryExists(string path)
  {
    return Directory.Exists(path);
  }

  /// <summary>
  /// Determines whether the specified file exists.
  /// </summary>
  /// <param name="path">The file to check.</param>
  /// <returns>true if the caller has the required permissions and path contains the name of an existing file; otherwise, false.</returns>
  public bool FileExists(string path)
  {
    return File.Exists(path);
  }

  /// <summary>
  /// Combines two strings into a path.
  /// </summary>
  /// <param name="path1">The first path to combine.</param>
  /// <param name="path2">The second path to combine.</param>
  /// <returns>The combined paths.</returns>
  public string CombinePath(string path1, string path2)
  {
    return Path.Combine(path1, path2);
  }

  public void EnsureDirectoryExists(string path)
  {
    Directory.CreateDirectory(path);
  }

  public Task<long> GetFileLengthAsync(string path)
  {
    return Task.Run(() =>
    {
      // Each call executes this lambda on a ThreadPool thread
      using SafeFileHandle handle = File.OpenHandle(
          path,
          FileMode.Open,      // Opens existing file
          FileAccess.Read,    // Read-only access needed
          FileShare.Read,     // <-- Allows other readers
          FileOptions.None);
      // GetLength is sync once handle is acquired
      return RandomAccess.GetLength(handle);
    });
  }

  public Task SetFileLengthAsync(string path, long length)
  {
    // Validate arguments before offloading
    ArgumentOutOfRangeException.ThrowIfNegative(length);
    ArgumentException.ThrowIfNullOrWhiteSpace(path); // Basic path check

    return Task.Run(() =>
    {
      // Open the stream, set length, and ensure disposal
      // Use FileShare.None for safety during resize operation, although
      // this increases potential for IOException if file is in use elsewhere.
      using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None))
      {
        fs.SetLength(length);
        // Flush might be needed on some OSes? Generally SetLength handles it.
        // fs.Flush(flushToDisk: true); // Consider if needed
      }
      // Exceptions (FileNotFound, IO, UnauthorizedAccess, etc.) from FileStream/SetLength
      // will propagate via the Task returned by Task.Run.
    });
  }

  public Task<string> ReadAllTextAsync(string path)
  {
    return File.ReadAllTextAsync(path);
  }

  public async Task<int> ReadFileAsync(string path, long fileOffset, Memory<byte> destination)
  {
    // Validate arguments
    ArgumentException.ThrowIfNullOrWhiteSpace(path);
    ArgumentOutOfRangeException.ThrowIfNegative(fileOffset);
    // Reading 0 bytes into an empty buffer is valid, ReadAsync handles it.

    // This ensures the handle is properly disposed even if exceptions occur.
    using (SafeFileHandle handle = File.OpenHandle(
        path,
        FileMode.Open,           // File must exist
        FileAccess.Read,         // Only need to read
        FileShare.Read,          // Allow other concurrent readers
        FileOptions.Asynchronous // Enable async I/O for the handle
                                 // Note: Buffer size option is not relevant for OpenHandle directly
    ))
    {
      // Perform the asynchronous read operation using RandomAccess static methods
      int bytesRead = await RandomAccess.ReadAsync(handle, destination, fileOffset);
      return bytesRead;
    }
    // Exceptions from OpenHandle or ReadAsync (FileNotFound, UnauthorizedAccess, IO, etc.)
    // will propagate up the call stack naturally.
  }

  public async Task WriteFileAsync(string path, long fileOffset, ReadOnlyMemory<byte> source)
  {
    ArgumentException.ThrowIfNullOrWhiteSpace(path);
    ArgumentOutOfRangeException.ThrowIfNegative(fileOffset);

    using (SafeFileHandle handle = File.OpenHandle(
        path,
        FileMode.OpenOrCreate, // Create if not exists
        FileAccess.Write,      // Only need to write
        FileShare.None,        // No sharing during write
        FileOptions.Asynchronous // Enable async I/O for the handle
    ))
    {
      await RandomAccess.WriteAsync(handle, source, fileOffset);
    }
  }

  public Task DeleteFileAsync(string path)
  {
    // Validate arguments before offloading to Task.Run
    ArgumentException.ThrowIfNullOrWhiteSpace(path);

    return Task.Run(() =>
    {
      // File.Delete is synchronous and idempotent regarding non-existence.
      // It will throw for other errors (permissions, path is directory, etc.)
      File.Delete(path);

      // Exceptions are propagated via the Task.
    });
  }
}