using ArmDb.Common.Abstractions;
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

  public Task<int> ReadFileAsync(string path, long fileOffset, Memory<byte> destination)
  {
    throw new NotImplementedException();
  }

  public Task WriteFileAsync(string path, long fileOffset, ReadOnlyMemory<byte> source)
  {
    throw new NotImplementedException();
  }

  public Task DeleteFileAsync(string path)
  {
    throw new NotImplementedException();
  }
}