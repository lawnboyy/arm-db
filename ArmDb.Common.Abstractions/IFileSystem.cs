namespace ArmDb.Common.Abstractions;
/// <summary>
/// Provides an abstraction over file system operations, allowing for
/// testability and potential platform-specific implementations.
/// Focuses on operations needed for page-based storage management.
/// </summary>
public interface IFileSystem
{
  /// <summary>
  /// Combines two strings into a path using the platform-specific separator.
  /// </summary>
  /// <param name="path1">The first path to combine.</param>
  /// <param name="path2">The second path to combine.</param>
  /// <returns>The combined paths.</returns>
  string CombinePath(string path1, string path2);

  /// <summary>
  /// Determines whether the specified directory exists.
  /// </summary>
  /// <param name="path">The path to test.</param>
  /// <returns>true if path refers to an existing directory; false otherwise.</returns>
  bool DirectoryExists(string path);

  /// <summary>
  /// Ensures that the specified directory exists. If the directory structure does not exist, it is created recursively.
  /// </summary>
  /// <param name="path">The absolute path of the directory to ensure exists.</param>
  /// <returns>Void</returns>
  /// <exception cref="System.IO.IOException">Thrown on I/O errors during directory creation.</exception>
  /// <exception cref="System.UnauthorizedAccessException">Thrown if permissions are insufficient.</exception>
  void EnsureDirectoryExists(string path);

  /// <summary>
  /// Determines whether the specified file exists.
  /// </summary>
  /// <param name="path">The file path to check.</param>
  /// <returns>Returns true if the file exists; false otherwise.</returns>
  bool FileExists(string path);

  /// <summary>
  /// Asynchronously gets the length of the specified file in bytes.
  /// </summary>
  /// <param name="path">The path to the file.</param>
  /// <returns>A task that represents the asynchronous operation. The task result contains the length of the file in bytes.</returns>
  /// <exception cref="System.IO.FileNotFoundException">Thrown if the file does not exist.</exception>
  /// <exception cref="System.IO.IOException">Thrown on I/O errors.</exception>
  /// <exception cref="System.UnauthorizedAccessException">Thrown if permissions are insufficient.</exception>
  Task<long> GetFileLengthAsync(string path);

  /// <summary>
  /// Asynchronously sets the length of the specified file. Can be used to pre-allocate or truncate a file.
  /// </summary>
  /// <param name="path">The path to the file. The file must exist.</param>
  /// <param name="length">The desired length of the file in bytes (must be non-negative).</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  /// <exception cref="System.ArgumentOutOfRangeException">Thrown if length is negative.</exception>
  /// <exception cref="System.IO.FileNotFoundException">Thrown if the file does not exist.</exception>
  /// <exception cref="System.IO.IOException">Thrown on I/O errors.</exception>
  /// <exception cref="System.UnauthorizedAccessException">Thrown if permissions are insufficient.</exception>
  Task SetFileLengthAsync(string path, long length);

  /// <summary>
  /// Asynchronously reads a sequence of bytes from the specified file, starting at a given offset,
  /// into the destination memory buffer.
  /// </summary>
  /// <param name="path">The path to the file.</param>
  /// <param name="fileOffset">The zero-based byte offset in the file from which to begin reading.</param>
  /// <param name="destination">The memory buffer to write the data into.</param>
  /// <returns>
  /// A task that represents the asynchronous read operation. The task result contains the total
  /// number of bytes read into the buffer. This might be less than the length of the destination buffer if
  /// the end of the file was reached before filling the buffer. Returns 0 if the offset is at or beyond the end of the file.
  /// </returns>
  /// <exception cref="System.ArgumentOutOfRangeException">Thrown if fileOffset is negative.</exception>
  /// <exception cref="System.IO.FileNotFoundException">Thrown if the file does not exist.</exception>
  /// <exception cref="System.IO.IOException">Thrown on I/O errors.</exception>
  /// <exception cref="System.UnauthorizedAccessException">Thrown if permissions are insufficient.</exception>
  Task<int> ReadFileAsync(string path, long fileOffset, Memory<byte> destination);

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
  Task<string> ReadAllTextAsync(string path);

  /// <summary>
  /// Asynchronously writes a sequence of bytes from a read-only memory buffer to the specified file,
  /// starting at a given offset. Creates the file if it does not exist; otherwise, overwrites existing data at the specified location.
  /// May extend the file if the write goes beyond the current end of the file.
  /// </summary>
  /// <param name="path">The path to the file.</param>
  /// <param name="fileOffset">The zero-based byte offset in the file at which to begin writing.</param>
  /// <param name="source">The read-only memory buffer containing the data to write.</param>
  /// <returns>A task representing the asynchronous write operation.</returns>
  /// <exception cref="System.ArgumentOutOfRangeException">Thrown if fileOffset is negative.</exception>
  /// <exception cref="System.IO.IOException">Thrown on I/O errors.</exception>
  /// <exception cref="System.UnauthorizedAccessException">Thrown if permissions are insufficient.</exception>
  /// <exception cref="System.NotSupportedException">Thrown if the file is opened in a way that does not support writing.</exception>
  Task WriteFileAsync(string path, long fileOffset, ReadOnlyMemory<byte> source);

  /// <summary>
  /// Asynchronously deletes the specified file. If the file does not exist, the operation completes successfully without throwing an exception.
  /// </summary>
  /// <param name="path">The path of the file to be deleted.</param>
  /// <returns>A task representing the asynchronous delete operation.</returns>
  /// <exception cref="System.IO.IOException">Thrown on I/O errors (e.g., file in use, path is a directory).</exception>
  /// <exception cref="System.UnauthorizedAccessException">Thrown if permissions are insufficient.</exception>
  Task DeleteFileAsync(string path);
}