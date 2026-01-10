using ArmDb.DataModel;
using Record = ArmDb.DataModel.Record;
using ArmDb.SchemaDefinition;
using ArmDb.Storage;

namespace ArmDb.UnitTests.Storage.BTreeTests;

public partial class BTreeTests
{
  [Fact]
  public async Task ScanAsync_SameValueDifferentColumns_ChecksCorrectColumn()
  {
    // Arrange
    var tableDef = new TableDefinition("ColumnConfusionSchema");
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddColumn(new ColumnDefinition("ColA", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddColumn(new ColumnDefinition("ColB", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Id", ["Id"]));

    var tree = await BTree.CreateAsync(_bpm, tableDef);

    // Insert records with overlapping values in different columns
    // Record 1: Matches on ColA only
    await tree.InsertAsync(new Record(DataValue.CreateInteger(1), DataValue.CreateString("Target"), DataValue.CreateString("Other")));
    // Record 2: Matches on ColB only
    await tree.InsertAsync(new Record(DataValue.CreateInteger(2), DataValue.CreateString("Other"), DataValue.CreateString("Target")));
    // Record 3: Matches on both
    await tree.InsertAsync(new Record(DataValue.CreateInteger(3), DataValue.CreateString("Target"), DataValue.CreateString("Target")));

    // Act 1: Scan ColA for "Target"
    var resultsA = new List<Record>();
    await foreach (var row in tree.ScanAsync("ColA", DataValue.CreateString("Target")))
    {
      resultsA.Add(row);
    }

    // Assert 1
    Assert.Equal(2, resultsA.Count);
    Assert.Contains(resultsA, r => r.Values[0].GetAs<int>() == 1);
    Assert.Contains(resultsA, r => r.Values[0].GetAs<int>() == 3);
    Assert.DoesNotContain(resultsA, r => r.Values[0].GetAs<int>() == 2);

    // Act 2: Scan ColB for "Target"
    var resultsB = new List<Record>();
    await foreach (var row in tree.ScanAsync("ColB", DataValue.CreateString("Target")))
    {
      resultsB.Add(row);
    }

    // Assert 2
    Assert.Equal(2, resultsB.Count);
    Assert.Contains(resultsB, r => r.Values[0].GetAs<int>() == 2);
    Assert.Contains(resultsB, r => r.Values[0].GetAs<int>() == 3);
    Assert.DoesNotContain(resultsB, r => r.Values[0].GetAs<int>() == 1);
  }

  [Fact]
  public async Task ScanAsync_PredicateOnLargeColumn_MatchesCorrectly()
  {
    // Arrange
    var tree = await CreatePopulatedTree();
    var uniqueLargeBio = new string('y', 2000); // 2KB string

    // Insert a new record with a unique large value in the Bio column
    // (Existing records have 2000 'x's)
    await tree.InsertAsync(new Record(new List<DataValue>
    {
        DataValue.CreateString("Zack"),
        DataValue.CreateString("zack@email.com"),
        DataValue.CreateDateTime(DateTime.UtcNow),
        DataValue.CreateString(uniqueLargeBio)
    }));

    // Act
    // Scan for Bio = uniqueLargeBio
    var results = new List<Record>();
    await foreach (var row in tree.ScanAsync("Bio", DataValue.CreateString(uniqueLargeBio)))
    {
      results.Add(row);
    }

    // Assert
    Assert.Single(results);
    Assert.Equal("Zack", results.First().Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_SearchingForNull_ReturnsRecordsWithNulls()
  {
    // Arrange
    var tableDef = new TableDefinition("NullSchema");
    tableDef.AddColumn(new ColumnDefinition("Id", new DataTypeInfo(PrimitiveDataType.Int), false));
    tableDef.AddColumn(new ColumnDefinition("NullableCol", new DataTypeInfo(PrimitiveDataType.Varchar, 50), true)); // Nullable
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_Id", ["Id"]));

    var tree = await BTree.CreateAsync(_bpm, tableDef);

    // Insert records: IDs 2 and 3 have NULL in NullableCol
    await tree.InsertAsync(new Record(DataValue.CreateInteger(1), DataValue.CreateString("NotNull")));
    await tree.InsertAsync(new Record(DataValue.CreateInteger(2), DataValue.CreateNull(PrimitiveDataType.Varchar)));
    await tree.InsertAsync(new Record(DataValue.CreateInteger(3), DataValue.CreateNull(PrimitiveDataType.Varchar)));
    await tree.InsertAsync(new Record(DataValue.CreateInteger(4), DataValue.CreateString("AlsoNotNull")));

    // Act
    // Scan for NullableCol = NULL
    var results = new List<Record>();
    await foreach (var row in tree.ScanAsync("NullableCol", DataValue.CreateNull(PrimitiveDataType.Varchar)))
    {
      results.Add(row);
    }

    // Assert
    Assert.Equal(2, results.Count);
    Assert.Contains(results, r => r.Values[0].GetAs<int>() == 2);
    Assert.Contains(results, r => r.Values[0].GetAs<int>() == 3);
    Assert.DoesNotContain(results, r => r.Values[0].GetAs<int>() == 1);
  }

  [Fact]
  public async Task ScanAsync_OnEmptyTree_WithPredicate_ReturnsEmpty()
  {
    // Arrange
    var tableDef = new TableDefinition("EmptySchema");
    tableDef.AddColumn(new ColumnDefinition("Username", new DataTypeInfo(PrimitiveDataType.Varchar, 50), false));
    tableDef.AddConstraint(new PrimaryKeyConstraint("PK_User", ["Username"]));

    // Create an empty BTree
    var tree = await BTree.CreateAsync(_bpm, tableDef);

    // Act
    var results = new List<Record>();
    // Scan using the column name overload on an empty tree
    await foreach (var row in tree.ScanAsync("Username", DataValue.CreateString("NonExistent")))
    {
      results.Add(row);
    }

    // Assert
    Assert.Empty(results);
  }

  [Fact]
  public async Task ScanAsync_TargetingFirstAndLastRecords_ReturnsMatches()
  {
    // Arrange
    var tree = await CreatePopulatedTree();

    // Act 1: Scan for the very first record ("Aaron") by Username (PK)
    var firstResults = new List<Record>();
    await foreach (var row in tree.ScanAsync("Username", DataValue.CreateString("Aaron")))
    {
      firstResults.Add(row);
    }

    // Act 2: Scan for the very last record ("Kevin") by Username (PK)
    var lastResults = new List<Record>();
    await foreach (var row in tree.ScanAsync("Username", DataValue.CreateString("Kevin")))
    {
      lastResults.Add(row);
    }

    // Assert
    Assert.Single(firstResults);
    Assert.Equal("Aaron", firstResults.First().Values[0].ToString());

    Assert.Single(lastResults);
    Assert.Equal("Kevin", lastResults.First().Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_WithColumnValuePredicate_NoMatches_ReturnsEmpty()
  {
    // Arrange
    var tree = await CreatePopulatedTree();

    // Act
    // Scan for Email = "ghost@email.com" which does not exist in the populated tree
    var results = new List<Record>();
    await foreach (var row in tree.ScanAsync("Email", DataValue.CreateString("ghost@email.com")))
    {
      results.Add(row);
    }

    // Assert
    Assert.Empty(results);
  }

  [Fact]
  public async Task ScanAsync_WithInvalidColumnName_ThrowsException()
  {
    // Arrange
    var tree = await CreatePopulatedTree();

    // Act & Assert
    await Assert.ThrowsAsync<ArgumentException>(async () =>
    {
      await foreach (var row in tree.ScanAsync("GhostColumn", DataValue.CreateString("value")))
      {
        // Should trigger exception before yielding
      }
    });
  }

  [Fact]
  public async Task ScanAsync_WithDataTypeMismatch_ThrowsException()
  {
    // Arrange
    var tree = await CreatePopulatedTree();

    // Act & Assert
    // "DoB" is defined as DateTime, but we are passing an Integer value.
    await Assert.ThrowsAsync<ArgumentException>(async () =>
    {
      await foreach (var row in tree.ScanAsync("DoB", DataValue.CreateInteger(12345)))
      {
        // Should trigger exception validation before scanning
      }
    });
  }

  [Fact]
  public async Task ScanAsync_WithDateTimePredicate_ReturnsMatchingRecords()
  {
    // Arrange
    var tree = await CreatePopulatedTree();
    // Bob's DoB from GetUsers() is new DateTime(1993, 2, 15)
    var targetDate = new DateTime(1993, 2, 15);

    // Act
    var results = new List<Record>();
    await foreach (var row in tree.ScanAsync("DoB", DataValue.CreateDateTime(targetDate)))
    {
      results.Add(row);
    }

    // Assert
    Assert.Single(results);
    Assert.Equal("Bob", results.First().Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_WithPrimaryKeyPredicate_ReturnsSingleRecord()
  {
    // Arrange
    var tree = await CreatePopulatedTree();

    // Act
    // "Username" is the PK. ScanAsync should find the single record via full scan.
    var results = new List<Record>();
    await foreach (var row in tree.ScanAsync("Username", DataValue.CreateString("Fabio")))
    {
      results.Add(row);
    }

    // Assert
    Assert.Single(results);
    Assert.Equal("Fabio", results.First().Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_WithColumnValuePredicate_ReturnsMatchingRecords()
  {
    // Arrange
    var tree = await CreatePopulatedTree();

    // Insert a record with a shared value in a non-PK column to verify filtering.
    // Existing data: Bob has email "bob@email.com".
    // Let's insert "Bob2" with the same email to ensure we get both matches.
    // Schema: Username(0), Email(1), DoB(2), Bio(3)
    var largeString = new string('x', 2000);
    var bob2 = new Record(new List<DataValue>
    {
        DataValue.CreateString("Bob2"),
        DataValue.CreateString("bob@email.com"),
        DataValue.CreateDateTime(DateTime.UtcNow),
        DataValue.CreateString(largeString)
    });
    await tree.InsertAsync(bob2);

    // Act
    // Scan for Email = "bob@email.com" (Column Index 1)
    var results = new List<Record>();
    // This assumes the BTree has an overload ScanAsync(int columnIndex, DataValue value)
    await foreach (var row in tree.ScanAsync("Email", DataValue.CreateString("bob@email.com")))
    {
      results.Add(row);
    }

    // Assert
    Assert.Equal(2, results.Count);
    Assert.Contains(results, r => r.Values[0].ToString() == "Bob");
    Assert.Contains(results, r => r.Values[0].ToString() == "Bob2");
    Assert.DoesNotContain(results, r => r.Values[0].ToString() == "Aaron");
  }

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

  [Fact]
  public async Task ScanAsync_NonExistentMax_Inclusive_EndsAtPreviousValue()
  {
    // Case: User asks for <= "Al" (Between "Aaron" and "Bob")
    // "Al" doesn't exist. Should return "Aaron" and stop before "Bob".

    var tree = await CreatePopulatedTree();
    Key? min = null;
    var max = CreateKey("Al");

    var result = new List<Record>();
    // maxInclusive = true
    await foreach (var item in tree.ScanAsync(min, false, max, true))
    {
      result.Add(item);
    }

    Assert.Single(result);
    Assert.Equal("Aaron", result.First().Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_NonExistentMax_Exclusive_EndsAtPreviousValue()
  {
    // Case: User asks for < "Al" (Between "Aaron" and "Bob")
    // Should return "Aaron" and stop before "Bob".

    var tree = await CreatePopulatedTree();
    Key? min = null;
    var max = CreateKey("Al");

    var result = new List<Record>();
    // maxInclusive = false
    await foreach (var item in tree.ScanAsync(min, false, max, false))
    {
      result.Add(item);
    }

    Assert.Single(result);
    Assert.Equal("Aaron", result.First().Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_MaxSmallerThanAllKeys_ReturnsEmpty()
  {
    // Case: User asks for <= "000" (Smaller than "Aaron")
    // Should return empty.

    var tree = await CreatePopulatedTree();
    Key? min = null;
    var max = CreateKey("000");

    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, false, max, true))
    {
      result.Add(item);
    }

    Assert.Empty(result);
  }

  [Fact]
  public async Task ScanAsync_MaxLargerThanAllKeys_ReturnsAll()
  {
    // Case: User asks for <= "Zzz" (Larger than "Kevin")
    // Should return all records.

    var tree = await CreatePopulatedTree();
    Key? min = null;
    var max = CreateKey("Zzz");

    var result = new List<Record>();
    await foreach (var item in tree.ScanAsync(min, false, max, true))
    {
      result.Add(item);
    }

    Assert.Equal(15, result.Count);
    Assert.Equal("Kevin", result.Last().Values[0].ToString());
  }

  [Fact]
  public async Task ScanAsync_MinExists_Exclusive_TraversesToNextPageIfNeeded()
  {
    // Edge Case: Min exists and is Exclusive.
    // Critical Scenario: The Min key is the *last* key on a leaf page.
    // The scanner must follow the sibling pointer to the next page to find the first result.
    // We iterate through adjacent pairs to guarantee hitting the page boundary condition.

    var tree = await CreatePopulatedTree();
    var users = GetUsers();

    for (int i = 0; i < users.Count - 1; i++)
    {
      var currentUser = users[i];
      var nextUser = users[i + 1];

      // Request: > currentUser
      var min = CreateKey(currentUser.Username);
      Key? max = null;

      // Act: minInclusive = false
      var result = new List<Record>();
      await foreach (var item in tree.ScanAsync(min, false, max, false))
      {
        result.Add(item);
      }

      Assert.NotEmpty(result);
      // The first record returned must be the immediate next user
      Assert.Equal(nextUser.Username, result.First().Values[0].ToString());
    }
  }

  [Fact]
  public async Task ScanAsync_MaxExists_Exclusive_StopsCorrectlyAtPageBoundaries()
  {
    // Edge Case: Max exists and is Exclusive.
    // Critical Scenario: The Max key is the *first* key on a new leaf page.
    // The scanner must stop at the last key of the previous page and NOT include the Max key.
    // We iterate through adjacent pairs to guarantee hitting the page boundary condition.

    var tree = await CreatePopulatedTree();
    var users = GetUsers();

    for (int i = 1; i < users.Count; i++)
    {
      var prevUser = users[i - 1];
      var currentUser = users[i];

      // Request: < currentUser
      // Should stop exactly at prevUser
      Key? min = null;
      var max = CreateKey(currentUser.Username);

      var result = new List<Record>();
      // maxInclusive = false
      await foreach (var item in tree.ScanAsync(min, false, max, false))
      {
        result.Add(item);
      }

      Assert.Equal(prevUser.Username, result.Last().Values[0].ToString());
      Assert.DoesNotContain(result, r => r.Values[0].ToString() == currentUser.Username);
    }
  }

  [Fact]
  public async Task ScanAsync_MinEqualsMax_EdgeCases()
  {
    // Edge Case: Min and Max are identical.
    // We verify that flags correctly produce empty sets vs single-point lookups.

    var tree = await CreatePopulatedTree();
    var target = CreateKey("Bob");

    // 1. Min Exclusive, Max Inclusive -> Empty ( Bob < x <= Bob is impossible )
    var res1 = new List<Record>();
    await foreach (var item in tree.ScanAsync(target, false, target, true)) res1.Add(item);
    Assert.Empty(res1);

    // 2. Min Inclusive, Max Exclusive -> Empty ( Bob <= x < Bob is impossible )
    var res2 = new List<Record>();
    await foreach (var item in tree.ScanAsync(target, true, target, false)) res2.Add(item);
    Assert.Empty(res2);

    // 3. Min Exclusive, Max Exclusive -> Empty
    var res3 = new List<Record>();
    await foreach (var item in tree.ScanAsync(target, false, target, false)) res3.Add(item);
    Assert.Empty(res3);

    // 4. Min Inclusive, Max Inclusive -> Single Record (Point Lookup via Scan)
    var res4 = new List<Record>();
    await foreach (var item in tree.ScanAsync(target, true, target, true)) res4.Add(item);
    Assert.Single(res4);
    Assert.Equal("Bob", res4.First().Values[0].ToString());
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