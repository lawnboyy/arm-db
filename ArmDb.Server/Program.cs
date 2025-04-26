using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ArmDb.Common.Utils;
using ArmDb.SchemaDefinition;
using ArmDb.Server.Bootstrap;
// Add using for ArmDb.StorageEngine interfaces when defined
// Add using for ArmDb.DataModel when needed for row generation

// Use default builder which sets up logging, config, DI
var builder = Host.CreateApplicationBuilder(args);

// --- Configuration ---
// Reads appsettings.json, environment variables, command line args by default
var bootstrapConfig = builder.Configuration.GetSection("Bootstrap");
// Get path to definition files, default to "./definitions" relative to app execution
string definitionDir = Path.GetFullPath( // Ensure we have an absolute path
    bootstrapConfig["DefinitionDirectory"] ?? Path.Combine(AppContext.BaseDirectory, "Definitions")
);

// --- Dependency Injection Setup ---
// Register the concrete file system implementation for the IFileSystem interface
builder.Services.AddSingleton<IFileSystem, FileSystem>();

// TODO: Register other services when they are created
// builder.Services.AddSingleton<IStorageEngine, StorageEngine>();
// builder.Services.AddSingleton<ISchemaManager, SchemaManager>();
// ... etc ...

// --- Build Host ---
// This creates the service provider container
var host = builder.Build();

// --- Bootstrap Logic (Run once before starting host services) ---
// Get necessary services from the DI container
var logger = host.Services.GetRequiredService<ILogger<Program>>(); // Logger for Program class
var fileSystem = host.Services.GetRequiredService<IFileSystem>();
// var storageEngine = host.Services.GetRequiredService<IStorageEngine>(); // Get later

List<TableDefinition>? systemTableDefinitions = null;
bool bootstrapSuccess = true; // Assume success unless error occurs

try
{
  logger.LogInformation("ArmDb Server starting...");
  logger.LogInformation("Looking for catalog definitions in: {Path}", definitionDir);

  // ====================================================================
  // TODO: CHECK IF BOOTSTRAP IS NEEDED
  // This logic will interact with the StorageEngine to see if the
  // database files and/or specific catalog tables/markers already exist.
  // For now, we assume it's always needed for this code path.
  bool isBootstrapNeeded = true;
  // ====================================================================

  if (isBootstrapNeeded)
  {
    logger.LogInformation("Bootstrap required. Loading system table definitions...");

    // --- Step 1: Load Definitions from JSON ---
    // Call the static loader method, passing the configured path and IFileSystem
    systemTableDefinitions = await SystemTableLoader.LoadCatalogDefinitionsAsync(definitionDir, fileSystem);
    logger.LogInformation("Successfully deserialized {Count} system table definitions.", systemTableDefinitions.Count);

    if (systemTableDefinitions.Count == 0)
    {
      logger.LogWarning("No system table definitions were loaded. Bootstrap cannot proceed effectively.");
      // Depending on requirements, this might be a fatal error.
      // bootstrapSuccess = false;
    }
    else
    {
      // --- Step 2: Create Physical Tables (Placeholder) ---
      // TODO: Iterate through systemTableDefinitions and call
      //       storageEngine.CreateTableAsync(tableDef.Name, tableDef)
      //       Requires IStorageEngine service and its implementation.
      logger.LogWarning("[TODO] Physical creation of system tables via StorageEngine not yet implemented.");

      // --- Step 3: Generate Bootstrap Data Rows (Placeholder) ---
      // TODO: Implement logic to create List<DataRow> for each system table,
      //       populating them with rows describing the system tables themselves.
      //       e.g., List<DataRow> sysTableRows = GenerateRowsForSysTables(systemTableDefinitions);
      logger.LogWarning("[TODO] Generation of bootstrap data rows not yet implemented.");

      // --- Step 4: Insert Bootstrap Data (Placeholder) ---
      // TODO: Iterate through generated DataRow lists and call
      //       storageEngine.InsertRowAsync(tableName, row)
      //       Requires IStorageEngine service and its implementation.
      logger.LogWarning("[TODO] Insertion of bootstrap data via StorageEngine not yet implemented.");

      // --- Step 5: Mark Bootstrap Complete (Placeholder) ---
      // TODO: Create a version file or marker via StorageEngine/FileSystem
      //       to prevent bootstrap from running again.
      logger.LogInformation("Bootstrap sequence tentatively complete (pending StorageEngine implementation).");
    }
  }
  else
  {
    logger.LogInformation("Database already initialized. Skipping bootstrap sequence.");
    // TODO: Implement loading schema FROM existing catalog tables via StorageEngine.
  }
}
catch (Exception ex)
{
  logger.LogCritical(ex, "A critical error occurred during server startup or bootstrap phase.");
  bootstrapSuccess = false; // Mark bootstrap as failed
                            // Depending on severity, you might want to exit the application
                            // Environment.Exit(1); // Or allow host to terminate gracefully
}

// --- Run the Server ---
if (bootstrapSuccess)
{
  logger.LogInformation("Proceeding to run the server host.");
  // This starts any IHostedService instances (like the network listener)
  // and blocks until the host is shut down (e.g., by Ctrl+C or SIGTERM).
  await host.RunAsync();
  logger.LogInformation("Server host shut down gracefully.");
}
else
{
  logger.LogError("Server startup aborted due to critical errors during initialization/bootstrap.");
  // Exit code could be non-zero here
}