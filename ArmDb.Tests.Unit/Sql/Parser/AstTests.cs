using ArmDb.SchemaDefinition;
using ArmDb.Sql.Ast;

namespace ArmDb.Tests.Unit.Sql.Parser;

public partial class AstTests
{
  [Fact]
  public void CreateDatabaseStatement_ConstructsCorrectly()
  {
    // SQL: CREATE DATABASE "mydb";

    // Arrange & Act
    string dbName = "mydb";
    var createDbStmt = new CreateDatabaseStatement(dbName);

    // Assert
    Assert.NotNull(createDbStmt);
    Assert.Equal(dbName, createDbStmt.DatabaseName);
  }

  [Fact]
  public void CreateTableStatement_SimpleStructure_ConstructsCorrectly()
  {
    // SQL: CREATE TABLE simple_users (id INT, username VARCHAR(255));

    // Arrange & Act
    // We are manually constructing the AST to verify the structure matches our design contract.
    // Later, the Parser tests will verify that "parsing string -> this structure" works.

    var tableName = new ObjectIdentifier("simple_users", "mydb");

    var columns = new List<ColumnDefinition>
        {
            new ColumnDefinition(
                "id",
                new DataTypeInfo(PrimitiveDataType.Int),
                isNullable: true // Default SQL nullable behavior if not specified
            ),
            new ColumnDefinition(
                "username",
                new DataTypeInfo(PrimitiveDataType.Varchar, 255),
                isNullable: true
            )
        };

    // No constraints for this simple example
    // Updated type from TableConstraint to Constraint based on user feedback
    var constraints = new List<Constraint>();

    var createStmt = new CreateTableStatement(tableName, columns, constraints);

    // Assert
    Assert.NotNull(createStmt);

    // Verify Table Name
    Assert.Equal("simple_users", createStmt.Table.Name);
    Assert.Equal("mydb", createStmt.Table.DatabaseName);

    // Verify Columns
    Assert.Equal(2, createStmt.Columns.Count);

    // Column 1: id
    Assert.Equal("id", createStmt.Columns[0].Name);
    Assert.Equal(PrimitiveDataType.Int, createStmt.Columns[0].DataType.PrimitiveType);

    // Column 2: username
    Assert.Equal("username", createStmt.Columns[1].Name);
    Assert.Equal(PrimitiveDataType.Varchar, createStmt.Columns[1].DataType.PrimitiveType);
    Assert.Equal(255, createStmt.Columns[1].DataType.MaxLength);
  }

  [Fact]
  public void CreateTableStatement_ComplexStructure_ConstructsCorrectly()
  {
    /*
    CREATE TABLE mydb.users
    (
        id int NOT NULL, -- Ignored IDENTITY for now
        email character VARCHAR(255) NOT NULL,    
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        is_onboarded boolean NOT NULL, -- Ignored DEFAULT false
        last_updated DATETIME,
        CONSTRAINT users_pkey PRIMARY KEY (id),
        CONSTRAINT users_email_unique UNIQUE (email)
    );
    */

    // Arrange & Act
    var tableName = new ObjectIdentifier("users", "mydb");

    // Construct Columns
    var columns = new List<ColumnDefinition>
        {
            // id int NOT NULL
            new ColumnDefinition(
                "id",
                new DataTypeInfo(PrimitiveDataType.Int),
                isNullable: false
            ),
            
            // email VARCHAR(255) NOT NULL
            new ColumnDefinition("email", new DataTypeInfo(PrimitiveDataType.Varchar, 255), isNullable: false),

            // first_name VARCHAR(255)
            new ColumnDefinition("first_name", new DataTypeInfo(PrimitiveDataType.Varchar, 255), isNullable: true),

            // last_name VARCHAR(255)
            new ColumnDefinition("last_name", new DataTypeInfo(PrimitiveDataType.Varchar, 255), isNullable: true),

            // is_onboarded boolean NOT NULL
            new ColumnDefinition("is_onboarded", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: false),

            // last_updated DATETIME
            new ColumnDefinition("last_updated", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: true)
        };

    // Construct Constraints
    var constraints = new List<Constraint>
        {
            new PrimaryKeyConstraint("users", new[] { "id" }, "users_pkey"),
            new UniqueKeyConstraint("users", new[] { "email" }, "users_email_unique")
        };

    var createStmt = new CreateTableStatement(tableName, columns, constraints);

    // Assert
    Assert.NotNull(createStmt);

    // Verify Identifier
    Assert.Equal("users", createStmt.Table.Name);
    Assert.Equal("mydb", createStmt.Table.DatabaseName);

    // Verify Columns
    Assert.Equal(6, createStmt.Columns.Count);

    // Spot check specific complex columns
    var idCol = createStmt.Columns[0];
    Assert.Equal("id", idCol.Name);
    // DefaultValue check removed

    var emailCol = createStmt.Columns[1];
    Assert.Equal("email", emailCol.Name);
    Assert.Equal(PrimitiveDataType.Varchar, emailCol.DataType.PrimitiveType);
    Assert.Equal(255, emailCol.DataType.MaxLength);
    Assert.False(emailCol.IsNullable);

    var boolCol = createStmt.Columns[4];
    Assert.Equal("is_onboarded", boolCol.Name);
    Assert.Equal(PrimitiveDataType.Boolean, boolCol.DataType.PrimitiveType);
    Assert.False(boolCol.IsNullable);

    // Verify Constraints
    Assert.Equal(2, createStmt.Constraints.Count);

    var pk = Assert.IsType<PrimaryKeyConstraint>(createStmt.Constraints[0]);
    Assert.Equal("users_pkey", pk.Name);
    Assert.Contains("id", pk.ColumnNames);

    var unique = Assert.IsType<UniqueKeyConstraint>(createStmt.Constraints[1]);
    Assert.Equal("users_email_unique", unique.Name);
    Assert.Contains("email", unique.ColumnNames);
  }
}