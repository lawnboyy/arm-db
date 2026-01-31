using System.Collections.Generic;
using ArmDb.SchemaDefinition;
using ArmDb.Sql.Ast; // Assuming this namespace based on project structure
using Xunit;

namespace ArmDb.Tests.Unit.Sql.Parser;

public partial class AstTests
{
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
      new PrimaryKeyConstraint("users", ["id"], "users_pkey"),
      new UniqueKeyConstraint("users", ["email"], "users_email_unique")
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

  [Fact]
  public void CreateTableStatement_WithForeignKeysAndCompositePK_ConstructsCorrectly()
  {
    /*
    CREATE TABLE mydb.order_items
    (
        order_id int NOT NULL,
        product_id int NOT NULL,
        quantity int NOT NULL,
        CONSTRAINT pk_order_items PRIMARY KEY (order_id, product_id),
        CONSTRAINT fk_order_items_orders FOREIGN KEY (order_id) REFERENCES mydb.orders(id)
    );
    */

    var tableName = new ObjectIdentifier("order_items", "mydb");

    var columns = new List<ColumnDefinition>
        {
            new ColumnDefinition("order_id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
            new ColumnDefinition("product_id", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
            new ColumnDefinition("quantity", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false)
        };

    var constraints = new List<Constraint>
    {
      // Composite Primary Key
      new PrimaryKeyConstraint("order_items", ["order_id", "product_id"], "pk_order_items"),
      
      // Foreign Key
      // Updated Constructor Signature based on provided code:
      // (referencingTable, referencingColumns, referencedTable, referencedColumns, name, ...)
      new ForeignKeyConstraint(
          "order_items",              // Referencing Table
          ["order_id"],       // Referencing Columns
          "orders",                   // Referenced Table
          ["id"],             // Referenced Columns
          "fk_order_items_orders"     // Constraint Name
      )
    };

    var createStmt = new CreateTableStatement(tableName, columns, constraints);

    Assert.NotNull(createStmt);
    Assert.Equal(3, createStmt.Columns.Count);
    Assert.Equal(2, createStmt.Constraints.Count);

    // Verify Composite PK
    var pk = Assert.IsType<PrimaryKeyConstraint>(createStmt.Constraints[0]);
    Assert.Equal("pk_order_items", pk.Name);
    Assert.Equal(2, pk.ColumnNames.Count);
    Assert.Equal("order_id", pk.ColumnNames[0]);
    Assert.Equal("product_id", pk.ColumnNames[1]);

    // Verify FK
    var fk = Assert.IsType<ForeignKeyConstraint>(createStmt.Constraints[1]);
    Assert.Equal("fk_order_items_orders", fk.Name);
    Assert.Equal("orders", fk.ReferencedTableName);
    Assert.Single(fk.ReferencingColumnNames);
    Assert.Equal("order_id", fk.ReferencingColumnNames[0]);
    Assert.Single(fk.ReferencedColumnNames);
    Assert.Equal("id", fk.ReferencedColumnNames[0]);
  }

  [Fact]
  public void CreateTableStatement_KitchenSink_AllTypesAndConstraints_ConstructsCorrectly()
  {
    /*
    CREATE TABLE mydb.kitchen_sink
    (
        col_int INT NOT NULL,
        col_bigint BIGINT,
        col_varchar VARCHAR(100),
        col_bool BOOLEAN,
        col_decimal DECIMAL,
        col_datetime DATETIME,
        col_float FLOAT,
        col_blob BLOB,

        CONSTRAINT pk_sink PRIMARY KEY (col_int),
        CONSTRAINT uq_sink_varchar UNIQUE (col_varchar),
        CONSTRAINT fk_sink_other FOREIGN KEY (col_bigint) REFERENCES mydb.other_table(id)
    );
    */

    var tableName = new ObjectIdentifier("kitchen_sink", "mydb");

    var columns = new List<ColumnDefinition>
    {
      new ColumnDefinition("col_int", new DataTypeInfo(PrimitiveDataType.Int), isNullable: false),
      new ColumnDefinition("col_bigint", new DataTypeInfo(PrimitiveDataType.BigInt), isNullable: true),
      new ColumnDefinition("col_varchar", new DataTypeInfo(PrimitiveDataType.Varchar, 100), isNullable: true),
      new ColumnDefinition("col_bool", new DataTypeInfo(PrimitiveDataType.Boolean), isNullable: true),
      new ColumnDefinition("col_decimal", new DataTypeInfo(PrimitiveDataType.Decimal), isNullable: true),
      new ColumnDefinition("col_datetime", new DataTypeInfo(PrimitiveDataType.DateTime), isNullable: true),
      new ColumnDefinition("col_float", new DataTypeInfo(PrimitiveDataType.Float), isNullable: true),
      new ColumnDefinition("col_blob", new DataTypeInfo(PrimitiveDataType.Blob, 255), isNullable: true)
    };

    var constraints = new List<Constraint>
    {
      new PrimaryKeyConstraint("kitchen_sink", ["col_int"], "pk_sink"),
      new UniqueKeyConstraint("kitchen_sink", ["col_varchar"], "uq_sink_varchar"),
      // Corrected FK constructor
      new ForeignKeyConstraint(
          "kitchen_sink",
          ["col_bigint"],
          "other_table",
          ["id"],
          "fk_sink_other"
      )
    };

    var createStmt = new CreateTableStatement(tableName, columns, constraints);

    Assert.NotNull(createStmt);
    Assert.Equal(8, createStmt.Columns.Count);
    Assert.Equal(3, createStmt.Constraints.Count);

    // Verify Types
    Assert.Equal(PrimitiveDataType.Int, createStmt.Columns[0].DataType.PrimitiveType);
    Assert.Equal(PrimitiveDataType.BigInt, createStmt.Columns[1].DataType.PrimitiveType);
    Assert.Equal(PrimitiveDataType.Varchar, createStmt.Columns[2].DataType.PrimitiveType);
    Assert.Equal(100, createStmt.Columns[2].DataType.MaxLength);
    Assert.Equal(PrimitiveDataType.Boolean, createStmt.Columns[3].DataType.PrimitiveType);
    Assert.Equal(PrimitiveDataType.Decimal, createStmt.Columns[4].DataType.PrimitiveType);
    Assert.Equal(PrimitiveDataType.DateTime, createStmt.Columns[5].DataType.PrimitiveType);
    Assert.Equal(PrimitiveDataType.Float, createStmt.Columns[6].DataType.PrimitiveType);
    Assert.Equal(PrimitiveDataType.Blob, createStmt.Columns[7].DataType.PrimitiveType);

    // Verify Constraints
    Assert.IsType<PrimaryKeyConstraint>(createStmt.Constraints[0]);
    Assert.IsType<UniqueKeyConstraint>(createStmt.Constraints[1]);
    Assert.IsType<ForeignKeyConstraint>(createStmt.Constraints[2]);
  }

  [Fact]
  public void InsertStatement_ConstructsCorrectly()
  {
    /*
    INSERT INTO mydb.users(
        email, first_name, last_name, is_onboarded, last_updated)
        VALUES ('bob@mail.com', 'Bob', 'Whiley', true, '11-18-2025');
    */

    // Arrange & Act
    var tableName = new ObjectIdentifier("users", "mydb");

    var columns = new List<string>
        {
            "email", "first_name", "last_name", "is_onboarded", "last_updated"
        };

    var values = new List<SqlExpression>
        {
            new LiteralExpression("bob@mail.com", PrimitiveDataType.Varchar),
            new LiteralExpression("Bob", PrimitiveDataType.Varchar),
            new LiteralExpression("Whiley", PrimitiveDataType.Varchar),
            new LiteralExpression(true, PrimitiveDataType.Boolean),
            // Dates are typically parsed as strings first, then cast during execution/binding
            new LiteralExpression("11-18-2025", PrimitiveDataType.Varchar)
        };

    var insertStmt = new InsertStatement(tableName, columns, values);

    // Assert
    Assert.NotNull(insertStmt);

    // Verify Table
    Assert.Equal("users", insertStmt.Table.Name);
    Assert.Equal("mydb", insertStmt.Table.DatabaseName);

    // Verify Columns
    Assert.Equal(5, insertStmt.Columns.Count);
    Assert.Equal("email", insertStmt.Columns[0]);
    Assert.Equal("last_updated", insertStmt.Columns[4]);

    // Verify Values
    Assert.Equal(5, insertStmt.Values.Count);

    var val1 = Assert.IsType<LiteralExpression>(insertStmt.Values[0]);
    Assert.Equal("bob@mail.com", val1.Value);
    Assert.Equal(PrimitiveDataType.Varchar, val1.Type);

    var val4 = Assert.IsType<LiteralExpression>(insertStmt.Values[3]);
    Assert.Equal(true, val4.Value);
    Assert.Equal(PrimitiveDataType.Boolean, val4.Type);
  }

  [Fact]
  public void SelectStatement_Simple_ConstructsCorrectly()
  {
    // SQL: SELECT * FROM users;

    // Arrange & Act
    var fromTable = new ObjectIdentifier("users", "mydb");

    var columns = new List<SelectColumn>
        {
            // SELECT *
            new SelectColumn(new ColumnExpression("*"), null)
        };

    var selectStmt = new SelectStatement(columns, fromTable, null);

    // Assert
    Assert.NotNull(selectStmt);

    // Verify From
    Assert.Equal("users", selectStmt.FromTable.Name);
    Assert.Equal("mydb", selectStmt.FromTable.DatabaseName);

    // Verify Columns
    Assert.Single(selectStmt.Columns);
    var col1 = selectStmt.Columns[0];
    Assert.Null(col1.Alias);

    var expr = Assert.IsType<ColumnExpression>(col1.Expression);
    Assert.Equal("*", expr.Name);

    // Verify Where
    Assert.Null(selectStmt.WhereClause);
  }

  [Fact]
  public void SelectStatement_WithProjectionsAndWhere_ConstructsCorrectly()
  {
    /*
    SELECT id, username AS u, is_onboarded 
    FROM mydb.users 
    WHERE id > 10;
    */

    // Arrange & Act
    var fromTable = new ObjectIdentifier("users", "mydb");

    var columns = new List<SelectColumn>
    {
      new SelectColumn(new ColumnExpression("id"), null),
      new SelectColumn(new ColumnExpression("username"), "u"),
      new SelectColumn(new ColumnExpression("is_onboarded"), null)
    };

    var whereClause = new BinaryExpression(
      new ColumnExpression("id"),
      BinaryOperator.GreaterThan,
      new LiteralExpression(10, PrimitiveDataType.Int)
    );

    var selectStmt = new SelectStatement(columns, fromTable, whereClause);

    // Assert
    Assert.NotNull(selectStmt);

    // Verify From
    Assert.Equal("users", selectStmt.FromTable.Name);
    Assert.Equal("mydb", selectStmt.FromTable.DatabaseName);

    // Verify Columns
    Assert.Equal(3, selectStmt.Columns.Count);

    // Col 1: id
    var expr1 = Assert.IsType<ColumnExpression>(selectStmt.Columns[0].Expression);
    Assert.Equal("id", expr1.Name);
    Assert.Null(selectStmt.Columns[0].Alias);

    // Col 2: username AS u
    var expr2 = Assert.IsType<ColumnExpression>(selectStmt.Columns[1].Expression);
    Assert.Equal("username", expr2.Name);
    Assert.Equal("u", selectStmt.Columns[1].Alias);

    // Verify Where
    Assert.NotNull(selectStmt.WhereClause);
    var bin = Assert.IsType<BinaryExpression>(selectStmt.WhereClause);
    Assert.Equal(BinaryOperator.GreaterThan, bin.Operator);

    var left = Assert.IsType<ColumnExpression>(bin.Left);
    Assert.Equal("id", left.Name);

    var right = Assert.IsType<LiteralExpression>(bin.Right);
    Assert.Equal(10, right.Value);
  }

  [Fact]
  public void UpdateStatement_ConstructsCorrectly()
  {
    /*
    UPDATE mydb.users
        SET email='bob.whiley@mail.com'
        WHERE id = 1;
    */

    // Arrange & Act
    var table = new ObjectIdentifier("users", "mydb");

    var assignments = new List<UpdateAssignment>
        {
            new UpdateAssignment(
                "email",
                new LiteralExpression("bob.whiley@mail.com", PrimitiveDataType.Varchar)
            )
        };

    var whereClause = new BinaryExpression(
        new ColumnExpression("id"),
        BinaryOperator.Equal,
        new LiteralExpression(1, PrimitiveDataType.Int)
    );

    var updateStmt = new UpdateStatement(table, assignments, whereClause);

    // Assert
    Assert.NotNull(updateStmt);
    Assert.Equal("users", updateStmt.Table.Name);
    Assert.Equal("mydb", updateStmt.Table.DatabaseName);

    Assert.Single(updateStmt.Assignments);
    Assert.Equal("email", updateStmt.Assignments[0].ColumnName);

    var val = Assert.IsType<LiteralExpression>(updateStmt.Assignments[0].Value);
    Assert.Equal("bob.whiley@mail.com", val.Value);

    Assert.NotNull(updateStmt.WhereClause);
    var binary = Assert.IsType<BinaryExpression>(updateStmt.WhereClause);
    Assert.Equal(BinaryOperator.Equal, binary.Operator);

    var left = Assert.IsType<ColumnExpression>(binary.Left);
    Assert.Equal("id", left.Name);

    var right = Assert.IsType<LiteralExpression>(binary.Right);
    Assert.Equal(1, right.Value);
  }

  [Fact]
  public void DeleteStatement_ConstructsCorrectly()
  {
    /*
    DELETE FROM mydb.users
        WHERE id = 1;
    */

    // Arrange & Act
    var table = new ObjectIdentifier("users", "mydb");

    var whereClause = new BinaryExpression(
        new ColumnExpression("id"),
        BinaryOperator.Equal,
        new LiteralExpression(1, PrimitiveDataType.Int)
    );

    var deleteStmt = new DeleteStatement(table, whereClause);

    // Assert
    Assert.NotNull(deleteStmt);
    Assert.Equal("users", deleteStmt.FromTable.Name);
    Assert.Equal("mydb", deleteStmt.FromTable.DatabaseName);

    Assert.NotNull(deleteStmt.WhereClause);
    var binary = Assert.IsType<BinaryExpression>(deleteStmt.WhereClause);
    Assert.Equal(BinaryOperator.Equal, binary.Operator);

    var left = Assert.IsType<ColumnExpression>(binary.Left);
    Assert.Equal("id", left.Name);

    var right = Assert.IsType<LiteralExpression>(binary.Right);
    Assert.Equal(1, right.Value);
  }

  [Fact]
  public void DropTableStatement_ConstructsCorrectly()
  {
    /*
    DROP TABLE mydb.users;
    */

    // Arrange & Act
    var table = new ObjectIdentifier("users", "mydb");
    var dropStmt = new DropTableStatement(table);

    // Assert
    Assert.NotNull(dropStmt);
    Assert.Equal("users", dropStmt.Table.Name);
    Assert.Equal("mydb", dropStmt.Table.DatabaseName);
  }
}