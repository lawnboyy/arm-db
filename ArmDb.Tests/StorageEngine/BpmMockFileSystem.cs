// In your Test Project (e.g., ArmDb.UnitTests.Common.Utils or a new TestHelpers file)
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using ArmDb.Common.Abstractions;

namespace ArmDb.UnitTests.TestUtils; // Example namespace

public class BpmMockFileSystem : IFileSystem
{
  public Dictionary<string, byte[]> Files { get; } = new(StringComparer.OrdinalIgnoreCase);
  public HashSet<string> Directories { get; } = new(StringComparer.OrdinalIgnoreCase);

  // Paths for which ReadFileAsync should throw an IOException
  public HashSet<string> ReadFailurePaths { get; } = new(StringComparer.OrdinalIgnoreCase);
  // Paths for which WriteFileAsync should throw an IOException
  public HashSet<string> WriteFailurePaths { get; } = new(StringComparer.OrdinalIgnoreCase);

  public Dictionary<string, int> ReadFileAsyncCallCounts { get; } = new(StringComparer.OrdinalIgnoreCase);

  public string CombinePath(string path1, string path2) => Path.Combine(path1, path2); // Use actual Path.Combine
  public bool DirectoryExists(string path) => Directories.Contains(path);
  public void EnsureDirectoryExists(string path) => Directories.Add(path); // Simplified: just tracks it
  public bool FileExists(string path) => Files.ContainsKey(path);

  public void AddFileContent(string path, byte[] content)
  {
    var dirName = Path.GetDirectoryName(path);
    if (!string.IsNullOrEmpty(dirName))
    {
      EnsureDirectoryExists(dirName); // Ensure directory path is tracked
    }
    Files[path] = content;
  }

  public Task<long> GetFileLengthAsync(string path)
  {
    if (Files.TryGetValue(path, out var content))
      return Task.FromResult((long)content.Length);
    throw new FileNotFoundException("Mock: File not found.", path);
  }

  public Task SetFileLengthAsync(string path, long length)
  {
    ArgumentOutOfRangeException.ThrowIfNegative(length);
    if (Files.TryGetValue(path, out var content))
    {
      Array.Resize(ref content, (int)length); // Simple resize for mock
      Files[path] = content;
    }
    else
    {
      Files[path] = new byte[length]; // Create new with specified length
    }
    return Task.CompletedTask;
  }

  public int GetReadFileCallCount(string path)
  {
    ReadFileAsyncCallCounts.TryGetValue(path, out int count);
    return count;
  }

  public Task<int> ReadFileAsync(string path, long fileOffset, Memory<byte> destination)
  {
    if (ReadFailurePaths.Contains(path))
      throw new IOException($"Simulated read failure for {path}");

    ReadFileAsyncCallCounts.TryGetValue(path, out int currentCount);
    ReadFileAsyncCallCounts[path] = currentCount + 1;

    if (Files.TryGetValue(path, out var content))
    {
      if (fileOffset < 0) throw new ArgumentOutOfRangeException(nameof(fileOffset));
      if (fileOffset >= content.Length) return Task.FromResult(0); // Read at or past EOF

      int bytesToRead = Math.Min(destination.Length, content.Length - (int)fileOffset);
      content.AsSpan((int)fileOffset, bytesToRead).CopyTo(destination.Span);
      return Task.FromResult(bytesToRead);
    }
    throw new FileNotFoundException("Mock: File not found for read.", path);
  }

  public Task WriteFileAsync(string path, long fileOffset, ReadOnlyMemory<byte> source)
  {
    if (WriteFailurePaths.Contains(path))
      throw new IOException($"Simulated write failure for {path}");
    ArgumentOutOfRangeException.ThrowIfNegative(fileOffset);

    if (!Files.TryGetValue(path, out var content) || content.Length < fileOffset + source.Length)
    {
      // Ensure directory for path exists for simplicity in mock, real EnsureDirectoryExists would be called by DiskManager init
      var dir = Path.GetDirectoryName(path);
      if (!string.IsNullOrEmpty(dir) && !Directories.Contains(dir)) Directories.Add(dir);

      // Auto-extend or create
      var requiredLength = (int)(fileOffset + source.Length);
      var newContent = new byte[requiredLength];
      content?.CopyTo(newContent, 0); // Copy existing if any
      Files[path] = newContent;
      content = newContent;
    }
    source.Span.CopyTo(content.AsSpan((int)fileOffset));
    return Task.CompletedTask;
  }

  public Task DeleteFileAsync(string path)
  {
    Files.Remove(path);
    return Task.CompletedTask;
  }

  public Task<string> ReadAllTextAsync(string path)
  {
    throw new NotImplementedException();
  }
}