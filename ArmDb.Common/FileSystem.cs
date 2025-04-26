namespace ArmDb.Common;

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
  /// Asynchronously opens a text file, reads all the text in the file, and then closes the file.
  /// </summary>
  /// <param name="path">The file to open for reading.</param>
  /// <returns>A task that represents the asynchronous read operation, which wraps the string containing all text in the file.</returns>
  /// <exception cref="System.IO.IOException">An I/O error occurred while opening the file.</exception>
  /// <exception cref="System.UnauthorizedAccessException">The caller does not have the required permission.</exception>
  /// <exception cref="System.IO.FileNotFoundException">The file specified in path was not found.</exception>
  /// <exception cref="System.Security.SecurityException">The caller does not have the required permission.</exception>
  /// <exception cref="System.IO.DirectoryNotFoundException">The specified path is invalid (for example, it is on an unmapped drive).</exception>
  public Task<string> ReadAllTextAsync(string path)
  {
    return File.ReadAllTextAsync(path);
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
}