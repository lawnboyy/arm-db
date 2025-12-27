using ArmDb.DataModel;
using ArmDb.SchemaDefinition;
using ArmDb.StorageEngine;
using Record = ArmDb.DataModel.Record;

namespace ArmDb.UnitTests.StorageEngine.BTreeTests;

public partial class BTreeTests
{
  // [Fact]
  // public async Task ScanAsync_MinProvided_NoMax_ReturnsCorrectRange()
  // {
  //   // Case 1: Minimum value provided, but no max
  //   // Goal: "Start at 'C' and go to the end"

  //   // Arrange
  //   var tree = CreatePopulatedTree();
  //   string min = "C";
  //   string? max = null;

  //   // Act
  //   var result = await tree.ScanAsync(min, max);

  //   // Assert
  //   Assert.NotNull(result);
  //   Assert.Equal("Cabral", result.First()); // Should start with first 'C'
  //   Assert.Equal("George", result.Last());  // Should go to the very end
  //   Assert.DoesNotContain("Bob", result);   // Should not have items before 'C'
  //   Assert.Equal(9, result.Count);          // Cabral through George
  // }

  // [Fact]
  // public async Task ScanAsync_NoMin_MaxProvided_ReturnsCorrectRange()
  // {
  //   // Case 2: A min is not provided, but a max is
  //   // Goal: "Start at beginning and stop before 'C'" (Half-open interval)

  //   // Arrange
  //   var tree = CreatePopulatedTree();
  //   string? min = null;
  //   string max = "C"; // Exclusive upper bound

  //   // Act
  //   var result = await tree.ScanAsync(min, max);

  //   // Assert
  //   Assert.Equal(2, result.Count);
  //   Assert.Equal("Aaron", result[0]);
  //   Assert.Equal("Bob", result[1]);
  //   Assert.DoesNotContain("Cabral", result); // Should stop before 'C'
  // }

  // [Fact]
  // public async Task ScanAsync_MinAndMaxProvided_ReturnsCorrectRange()
  // {
  //   // Case 3: Both min and max values are provided
  //   // Goal: "Find usernames starting with 'C', 'D', or 'E'" (From 'C' up to 'F')

  //   // Arrange
  //   var tree = CreatePopulatedTree();
  //   string min = "C";
  //   string max = "F"; // Exclusive upper bound

  //   // Act
  //   var result = await tree.ScanAsync(min, max);

  //   // Assert
  //   // Expected: Cabral, Cadence, Cyril, Dabney, Delta, Eagle, Ezra
  //   Assert.Equal(7, result.Count);
  //   Assert.Equal("Cabral", result.First());
  //   Assert.Equal("Ezra", result.Last());

  //   // Verify boundaries
  //   Assert.DoesNotContain("Bob", result);   // Before Min
  //   Assert.DoesNotContain("Fabio", result); // On/After Max
  // }

  [Fact]
  public async Task ScanAsync_NoConstraints_ReturnsAll()
  {
    // Case 4: Neither min nor max values are provided
    // Goal: Full Table Scan
    // Arrange
    var tree = await CreatePopulatedTree();

    // Act
    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync())
    {
      result.Add(item);
    }

    // Assert
    Assert.Equal(15, result.Count); // All 15 users
    Assert.Equal("Aaron", result.First().Values[0].ToString());
    Assert.Equal("Kevin", result.Last().Values[0].ToString());
  }

  private async Task<BTree> CreatePopulatedTree()
  {
    // Arrange
    // 1. Define schema with large column to force small fan-out (max 2 items per page)
    var tableDef = new TableDefinition("ScanTreeSchema");
    tableDef.AddColumn(new ColumnDefinition("Username", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddColumn(new ColumnDefinition("Email", new DataTypeInfo(PrimitiveDataType.Varchar, 250), false));
    tableDef.AddColumn(new ColumnDefinition("DoB", new DataTypeInfo(PrimitiveDataType.DateTime), false));
    tableDef.AddColumn(new ColumnDefinition("Bio", new DataTypeInfo(PrimitiveDataType.Varchar, 3000), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_User", ["Username"]));

    var btree = await BTree.CreateAsync(_bpm, tableDef);

    // specific large string to pad the record size (approx 2KB)
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

  internal class User
  {
    public string Username { get; set; } = "";
    public string Email { get; set; } = "";
    public DateTime DoB { get; set; } = DateTime.Now;
  }
}