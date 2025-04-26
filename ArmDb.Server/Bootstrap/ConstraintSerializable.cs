using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using ArmDb.SchemaDefinition; // Needs reference to domain model

namespace ArmDb.Server.Bootstrap; // Updated namespace

/// <summary>
/// Base serializable surrogate class for Constraints. Internal to bootstrap process.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "ConstraintType")]
[JsonDerivedType(typeof(PrimaryKeyConstraintSerializable), typeDiscriminator: "PrimaryKey")]
[JsonDerivedType(typeof(ForeignKeyConstraintSerializable), typeDiscriminator: "ForeignKey")]
[JsonDerivedType(typeof(UniqueConstraintSerializable), typeDiscriminator: "Unique")]
internal abstract class ConstraintSerializableBase
{
  public string? Name { get; init; }
  public abstract Constraint ToConstraint(string tableName);
}

/// <summary>
/// Serializable surrogate for PrimaryKeyConstraint.
/// </summary>
internal sealed class PrimaryKeyConstraintSerializable : ConstraintSerializableBase
{
  public List<string>? ColumnNames { get; init; }

  public override Constraint ToConstraint(string tableName)
  {
    if (ColumnNames == null || !ColumnNames.Any())
      throw new InvalidOperationException($"Deserialized primary key constraint serializable object '{Name ?? "(unnamed)"}' is missing column names.");
    return new PrimaryKeyConstraint(tableName, ColumnNames, Name);
  }
}

/// <summary>
/// Serializable surrogate for UniqueConstraint.
/// </summary>
internal sealed class UniqueConstraintSerializable : ConstraintSerializableBase
{
  public List<string>? ColumnNames { get; init; }

  public override Constraint ToConstraint(string tableName)
  {
    if (ColumnNames == null || !ColumnNames.Any())
      throw new InvalidOperationException($"Deserialized unique constraint serializable object '{Name ?? "(unnamed)"}' is missing column names.");
    return new UniqueConstraint(tableName, ColumnNames, Name);
  }
}

/// <summary>
/// Serializable surrogate for ForeignKeyConstraint.
/// </summary>
internal sealed class ForeignKeyConstraintSerializable : ConstraintSerializableBase
{
  public List<string>? ReferencingColumnNames { get; init; }
  public string? ReferencedTableName { get; init; }
  public List<string>? ReferencedColumnNames { get; init; }

  [JsonConverter(typeof(JsonStringEnumConverter))]
  public ReferentialAction OnUpdateAction { get; init; } = ReferentialAction.NoAction;

  [JsonConverter(typeof(JsonStringEnumConverter))]
  public ReferentialAction OnDeleteAction { get; init; } = ReferentialAction.NoAction;

  public override Constraint ToConstraint(string tableName)
  {
    if (ReferencingColumnNames == null || !ReferencingColumnNames.Any())
      throw new InvalidOperationException($"Deserialized foreign key constraint serializable object '{Name ?? "(unnamed)"}' is missing referencing column names.");
    if (string.IsNullOrWhiteSpace(ReferencedTableName))
      throw new InvalidOperationException($"Deserialized foreign key constraint serializable object '{Name ?? "(unnamed)"}' is missing referenced table name.");
    if (ReferencedColumnNames == null || !ReferencedColumnNames.Any())
      throw new InvalidOperationException($"Deserialized foreign key constraint serializable object '{Name ?? "(unnamed)"}' is missing referenced column names.");

    return new ForeignKeyConstraint(
        tableName,
        ReferencingColumnNames,
        ReferencedTableName,
        ReferencedColumnNames,
        Name,
        OnUpdateAction,
        OnDeleteAction
    );
  }
}