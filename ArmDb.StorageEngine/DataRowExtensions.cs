using System.Reflection.Metadata.Ecma335;
using ArmDb.DataModel;
using ArmDb.SchemaDefinition;

namespace ArmDb.StorageEngine;

public static class DataRowExtensions
{
  public static Key GetPrimaryKey(this DataRow row, TableDefinition tableDefinition)
  {
    var primaryKey = tableDefinition.GetPrimaryKeyConstraint();
    var allColumns = tableDefinition.Columns;

    if (primaryKey == null)
      throw new ArgumentNullException("No primary key was found!");

    var keyValues = new DataValue[primaryKey.ColumnNames.Count];

    var primaryKeyColumns = primaryKey.ColumnNames;
    var primaryKeyReverseIndexLookup = primaryKey.ColumnNames
      .Select((pk, i) => new { Name = pk, Index = i })
      .ToDictionary(kvp => kvp.Name);

    for (var i = 0; i < allColumns.Count; i++)
    {
      // If this column is part of the primary key...
      if (primaryKeyColumns.Contains(allColumns[i].Name))
      {
        // Grab the value and add it to our Key...
        var keyIndex = primaryKeyReverseIndexLookup[allColumns[i].Name].Index;
        keyValues[keyIndex] = row[i];
      }
    }

    return new Key(keyValues);
  }
}