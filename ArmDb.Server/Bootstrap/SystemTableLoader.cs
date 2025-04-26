using System.Text.Json;
using System.Text.Json.Serialization;
using ArmDb.Common.Utils;
using ArmDb.SchemaDefinition;

namespace ArmDb.Server.Bootstrap;

/// <summary>
/// Helper class responsible for loading the table definitions for system catalog tables
/// from their JSON definition files during the bootstrap process.
/// </summary>
// Renamed class from CatalogDefinitionLoader
public static class SystemTableLoader
{
  // List of expected definition files for the core system catalog
  private static readonly string[] CatalogDefinitionFiles = {
      "sys_databases.json",
      "sys_tables.json",
      "sys_columns.json",
      "sys_constraints.json"
      // Add more as needed (e.g., sys_types.json)
  };

  // Configure JSON deserialization options
  private static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions
  {
    PropertyNameCaseInsensitive = true,
    ReadCommentHandling = JsonCommentHandling.Skip,
    AllowTrailingCommas = true,
    Converters = { new JsonStringEnumConverter() }
    // Polymorphism is handled by attributes on the Constraint class (.NET 7+)
  };

  /// <summary>
  /// Loads table definitions for the system catalog from JSON files in the specified directory.
  /// </summary>
  /// <param name="definitionDirectoryPath">The path to the directory containing the catalog JSON definition files.</param>
  /// <returns>A list of TableDefinition objects for the system catalog.</returns>
  /// <exception cref="DirectoryNotFoundException">Thrown if the definition directory does not exist.</exception>
  /// <exception cref="FileNotFoundException">Thrown if a required catalog definition file is missing.</exception>
  /// <exception cref="JsonException">Thrown if a JSON file is malformed or cannot be deserialized.</exception>
  /// <exception cref="Exception">Other potential exceptions during file access.</exception>
  // Method name kept the same, belongs to the renamed class now
  public static async Task<List<TableDefinition>> LoadCatalogDefinitionsAsync(string definitionDirectoryPath, IFileSystem fileSystem)
  {
    if (!fileSystem.DirectoryExists(definitionDirectoryPath))
    {
      throw new DirectoryNotFoundException($"Catalog definition directory not found: '{definitionDirectoryPath}'");
    }

    var catalogTables = new List<TableDefinition>();

    Console.WriteLine($"Loading catalog definitions from: {definitionDirectoryPath}"); // Replace with proper logging

    foreach (var fileName in CatalogDefinitionFiles)
    {
      string filePath = fileSystem.CombinePath(definitionDirectoryPath, fileName);
      Console.WriteLine($"  Attempting to load: {fileName}..."); // Replace with proper logging

      if (!fileSystem.FileExists(filePath))
      {
        // Critical error if a required system table definition is missing
        throw new FileNotFoundException($"Required catalog definition file not found: '{filePath}'");
      }

      try
      {
        string jsonContent = await fileSystem.ReadAllTextAsync(filePath);

        // Deserialize the JSON content into a TableDefinition object
        var tableDefinitionSurrogate = JsonSerializer.Deserialize<TableDefinitionSerializable>(jsonContent, SerializerOptions);
        var tableDefinition = tableDefinitionSurrogate?.ToTableDefinition();

        if (tableDefinition != null)
        {
          catalogTables.Add(tableDefinition);
          Console.WriteLine($"    Successfully loaded and deserialized '{tableDefinition.Name}'."); // Replace with proper logging
        }
        else
        {
          // Should not happen if JSON is valid unless content is just "null"
          Console.WriteLine($"    Warning: Deserialization resulted in null for file '{fileName}'. Skipping."); // Replace with proper logging
        }
      }
      catch (JsonException jsonEx)
      {
        // Provide context for JSON parsing errors
        throw new JsonException($"Error deserializing catalog definition file '{filePath}'. Details: {jsonEx.Message}", jsonEx);
      }
      catch (Exception ex)
      {
        // Catch other potential errors (file access, etc.)
        throw new Exception($"Failed to load or parse catalog definition file '{filePath}'.", ex);
      }
    }

    Console.WriteLine($"Successfully loaded {catalogTables.Count} catalog table definitions."); // Replace with proper logging
    return catalogTables;
  }
}