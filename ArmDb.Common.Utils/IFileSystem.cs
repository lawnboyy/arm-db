namespace ArmDb.Common.Utils;

public interface IFileSystem
{
  bool DirectoryExists(string path);
  bool FileExists(string path);
  Task<string> ReadAllTextAsync(string path);
  string CombinePath(string path1, string path2);
  // Add other methods as needed (e.g., GetFiles, WriteFile)
}