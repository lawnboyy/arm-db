namespace ArmDb.SchemaDefinition; // File-scoped namespace

/// <summary>
/// Defines the actions that can occur when a referenced key (in a primary/unique constraint)
/// is updated or deleted, affecting rows referencing it via a foreign key.
/// </summary>
public enum ReferentialAction
{
  /// <summary>
  /// No action is taken automatically. The operation (UPDATE/DELETE) on the referenced table
  /// will fail if there are dependent rows in the referencing table. This is often the default.
  /// </summary>
  NoAction = 0, // Or Restrict, depending on exact SQL standard interpretation (often similar in effect)

  /// <summary>
  /// The corresponding change (UPDATE/DELETE) is cascaded to the referencing rows.
  /// If a referenced row is deleted, referencing rows are deleted.
  /// If a referenced key value is updated, the referencing foreign key values are updated.
  /// </summary>
  Cascade,

  /// <summary>
  /// The foreign key columns in the referencing rows are set to NULL.
  /// This requires the foreign key columns to be nullable.
  /// </summary>
  SetNull,

  /// <summary>
  /// The foreign key columns in the referencing rows are set to their defined default value.
  /// This requires the foreign key columns to have a default value defined.
  /// </summary>
  SetDefault
}