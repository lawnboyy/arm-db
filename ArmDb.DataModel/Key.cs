using System.Text;

namespace ArmDb.DataModel;

public sealed record class Key : IEquatable<Key>
{
  public IReadOnlyList<DataValue> Values { get; init; }

  public Key(IReadOnlyList<DataValue> values)
  {
    // Add validation to ensure values are not null
    ArgumentNullException.ThrowIfNull(values);
    Values = values;
  }

  // Override Equals to use sequence equality for the list
  public bool Equals(Key? other)
  {
    if (other is null) return false;
    if (ReferenceEquals(this, other)) return true;

    return Values.SequenceEqual(other.Values);
  }

  // When you override Equals, you MUST override GetHashCode
  public override int GetHashCode()
  {
    unchecked // Allow overflow, which is fine for hash codes
    {
      int hash = 17;
      foreach (var value in Values)
      {
        // Combine hash codes of each DataValue in the list
        hash = hash * 23 + (value?.GetHashCode() ?? 0);
      }
      return hash;
    }
  }

  public override string ToString()
  {
    var stringBuilder = new StringBuilder();
    foreach (var value in Values)
    {
      stringBuilder.Append($"{value.ToString()}+");
    }

    stringBuilder.Remove(stringBuilder.Length - 1, 1);

    return stringBuilder.ToString();
  }
}