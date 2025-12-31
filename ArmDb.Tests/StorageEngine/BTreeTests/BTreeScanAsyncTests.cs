using ArmDb.DataModel;
using Record = ArmDb.DataModel.Record;
using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;

namespace ArmDb.UnitTests.StorageEngine.BTreeTests;

public partial class BTreeTests
{
  [Fact]
  public async Task ScanAsync_MinExclusive_ReturnsCorrectRange()
  {
    // Query: Username > "Aaron" (Exclusive)
    // Should skip "Aaron" and start at "Bob"

    var tree = await CreatePopulatedTree();
    var min = CreateKey("Aaron");
    Key? max = null;

    // Act: minInclusive = false
    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, false, max, false))
    {
      result.Add(item);
    }

    // Assert
    Assert.Equal(14, result.Count); // 15 total - 1 (Aaron)
    Assert.Equal("Bob", result.First().Values[0].ToString());
    Assert.Equal("Kevin", result.Last().Values[0].ToString());
    Assert.DoesNotContain(result, r => r.Values[0].ToString() == "Aaron");
  }

  [Fact]
  public async Task ScanAsync_MaxInclusive_ReturnsCorrectRange()
  {
    // Query: Username <= "Bob" (Inclusive)
    // Should include "Aaron" and "Bob"

    var tree = await CreatePopulatedTree();
    Key? min = null;
    var max = CreateKey("Bob");

    // Act: maxInclusive = true
    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, false, max, true))
    {
      result.Add(item);
    }

    // Assert
    Assert.Equal(2, result.Count);
    Assert.Equal("Aaron", result[0].Values[0].ToString());
    Assert.Equal("Bob", result[1].Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_MinExclusive_MaxInclusive_ReturnsCorrectRange()
  {
    // Query: Username > "Aaron" AND Username <= "Fabio"
    // Should Start at "Bob" and End at "Fabio" (Including Fabio)

    var tree = await CreatePopulatedTree();
    var min = CreateKey("Aaron");
    var max = CreateKey("Fabio");

    // Act: minInclusive = false, maxInclusive = true
    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, false, max, true))
    {
      result.Add(item);
    }

    // Assert
    // Expected: Bob, Cabral, Cadence, Cyril, Dabney, Delta, Eagle, Ezra, Fabio (9 items)
    Assert.Equal(9, result.Count);
    Assert.Equal("Bob", result.First().Values[0].ToString());
    Assert.Equal("Fabio", result.Last().Values[0].ToString());

    Assert.DoesNotContain(result, r => r.Values[0].ToString() == "Aaron");
    Assert.DoesNotContain(result, r => r.Values[0].ToString() == "George");
  }

  [Fact]
  public async Task ScanAsync_MinInclusive_MaxExclusive_ReturnsCorrectRange()
  {
    // Query: Username >= "Cabral" AND Username < "Fabio"
    // Should Start at "Cabral" and End at "Ezra" (Exclude Fabio)

    var tree = await CreatePopulatedTree();
    var min = CreateKey("Cabral");
    var max = CreateKey("Fabio");

    // Act: minInclusive = true, maxInclusive = false
    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, true, max, false))
    {
      result.Add(item);
    }

    // Assert
    Assert.Equal("Cabral", result.First().Values[0].ToString());
    Assert.Equal("Ezra", result.Last().Values[0].ToString());
    Assert.DoesNotContain(result, r => r.Values[0].ToString() == "Fabio");
  }

  [Fact]
  public async Task ScanAsync_NoConstraints_ReturnsAll()
  {
    // Case: Full Table Scan
    var tree = await CreatePopulatedTree();

    // Act
    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(null, false, null, false))
    {
      result.Add(item);
    }

    // Assert
    Assert.Equal(15, result.Count);
  }

  [Fact]
  public async Task ScanAsync_EmptyTree_ReturnsEmpty()
  {
    var tableDef = new TableDefinition("EmptySchema");
    tableDef.AddColumn(new ColumnDefinition("Username", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_User", ["Username"]));

    var tree = await BTree.CreateAsync(_bpm, tableDef);

    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(null, false, null, false))
    {
      result.Add(item);
    }

    Assert.Empty(result);
  }

  [Fact]
  public async Task ScanAsync_MinGreaterThanMax_ReturnsEmpty()
  {
    var tree = await CreatePopulatedTree();
    var min = CreateKey("Z");
    var max = CreateKey("A");

    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, true, max, true))
    {
      result.Add(item);
    }

    Assert.Empty(result);
  }

  [Fact]
  public async Task ScanAsync_NonExistentStartKey_SeeksToNextAvailable()
  {
    // Scan starting at "Al" (Inclusive). "Al" does not exist.
    // Should land on "Bob".
    var tree = await CreatePopulatedTree();
    var min = CreateKey("Al");
    Key? max = null;

    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, true, max, false))
    {
      result.Add(item);
    }

    Assert.NotEmpty(result);
    Assert.Equal("Bob", result.First().Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_NonExistentMin_Inclusive_SeeksToNextValue()
  {
    // Case: User asks for >= "Al" (Between "Aaron" and "Bob")
    // Since "Al" doesn't exist, it should land on "Bob".

    var tree = await CreatePopulatedTree();
    var min = CreateKey("Al");
    Key? max = null;

    var result = new List<Record>();
    // minInclusive = true
    await foreach (var item in tree.ScanAsync(min, true, max, false))
    {
      result.Add(item);
    }

    Assert.NotEmpty(result);
    Assert.Equal("Bob", result.First().Values[0].ToString());
    // Verify it didn't accidentally wrap around or start at Aaron
    Assert.DoesNotContain(result, r => r.Values[0].ToString() == "Aaron");
  }

  [Fact]
  public async Task ScanAsync_NonExistentMin_Exclusive_SeeksToNextValue()
  {
    // Case: User asks for > "Al" (Between "Aaron" and "Bob")
    // "Al" doesn't exist. > "Al" is effectively the same as >= "Al" 
    // in a sparse domain. It should also land on "Bob".

    var tree = await CreatePopulatedTree();
    var min = CreateKey("Al");
    Key? max = null;

    var result = new List<Record>();
    // minInclusive = false
    await foreach (var item in tree.ScanAsync(min, false, max, false))
    {
      result.Add(item);
    }

    Assert.NotEmpty(result);
    Assert.Equal("Bob", result.First().Values[0].ToString());
    Assert.DoesNotContain(result, r => r.Values[0].ToString() == "Aaron");
  }

  [Fact]
  public async Task ScanAsync_MinSmallerThanAllKeys_StartsAtFirstRecord()
  {
    // Case: User asks for >= "000" (Smaller than "Aaron")
    // Should start at "Aaron".

    var tree = await CreatePopulatedTree();
    var min = CreateKey("000");
    Key? max = null;

    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, true, max, false))
    {
      result.Add(item);
    }

    Assert.Equal(15, result.Count); // Should get everything
    Assert.Equal("Aaron", result.First().Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_MinLargerThanAllKeys_ReturnsEmpty()
  {
    // Case: User asks for >= "Zzz" (Larger than "Kevin")
    // Should return nothing.

    var tree = await CreatePopulatedTree();
    var min = CreateKey("Zzz");
    Key? max = null;

    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, true, max, false))
    {
      result.Add(item);
    }

    Assert.Empty(result);
  }

  private Key CreateKey(string val)
  {
    return new Key(new[] { DataValue.CreateString(val) });
  }

  private async Task<BTree> CreatePopulatedTree()
  {
    // Schema definition
    var tableDef = new TableDefinition("ScanTreeSchema");
    tableDef.AddColumn(new ColumnDefinition("Username", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddColumn(new ColumnDefinition("Email", new DataTypeInfo(PrimitiveDataType.Varchar, 250), false));
    tableDef.AddColumn(new ColumnDefinition("DoB", new DataTypeInfo(PrimitiveDataType.DateTime), false));
    // Bio column with padding to force page splits (sibling traversal)
    tableDef.AddColumn(new ColumnDefinition("Bio", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_User", ["Username"]));

    var btree = await BTree.CreateAsync(_bpm, tableDef);

    // Padding string (approx 2KB)
    var largeString = new string('x', 2000);

    foreach (var user in GetUsers())
    {
      var record = new Record(new List<DataValue>
            {
                DataValue.CreateString(user.Username),
                DataValue.CreateString(user.Email),
                DataValue.CreateDateTime(user.DoB),
                DataValue.CreateString(largeString)
            });
      await btree.InsertAsync(record);
    }
    return btree;
  }

  private List<User> GetUsers()
  {
    return new List<User>
    {
      new User { Username = "Aaron", DoB = new DateTime(1977, 1, 25), Email = "aaron@email.com" },
      new User { Username = "Bob", DoB = new DateTime(1993, 2, 15), Email = "bob@email.com" },
      new User { Username = "Cabral", DoB = new DateTime(1988, 8, 22), Email = "cabral@email.com" },
      new User { Username = "Cadence", DoB = new DateTime(1995, 2, 14), Email = "cadence@email.com" },
      new User { Username = "Cyril", DoB = new DateTime(1982, 6, 30), Email = "cyril@email.com" },
      new User { Username = "Dabney", DoB = new DateTime(1991, 9, 10), Email = "dabney@email.com" },
      new User { Username = "Delta", DoB = new DateTime(1993, 12, 1), Email = "delta@email.com" },
      new User { Username = "Eagle", DoB = new DateTime(1987, 5, 18), Email = "eagle@email.com" },
      new User { Username = "Ezra", DoB = new DateTime(1994, 3, 25), Email = "ezra@email.com" },
      new User { Username = "Fabio", DoB = new DateTime(1989, 7, 7), Email = "fabio@email.com" },
      new User { Username = "George", DoB = new DateTime(1992, 1, 15), Email = "george@email.com" },
      new User { Username = "Hannah", DoB = new DateTime(1996, 10, 20), Email = "hannah@email.com" },
      new User { Username = "Ian", DoB = new DateTime(1984, 12, 8), Email = "ian@email.com" },
      new User { Username = "Julia", DoB = new DateTime(1991, 5, 3), Email = "julia@email.com" },
      new User { Username = "Kevin", DoB = new DateTime(1986, 9, 29), Email = "kevin@email.com" }
    };
  }
}

public class User
{
  public string Username { get; set; } = string.Empty;
  public DateTime DoB { get; set; }
  public string Email { get; set; } = string.Empty;
}