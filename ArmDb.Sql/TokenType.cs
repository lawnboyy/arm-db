namespace ArmDb.Sql;

/// <summary>
/// Defines the types of tokens identified by the tokenizer.
/// </summary>
public enum TokenType
{
  // --- End of File ---
  EndOfFile,

  // --- Literals ---
  Identifier,         // Users, "My Table", email
  StringLiteral,      // 'Hello World'
  NumericLiteral,     // 123, 45.67
  BooleanLiteral,     // TRUE, FALSE (Often keywords, but treated as literals in parser)

  // --- Keywords (DML) ---
  Select,
  Insert,
  Update,
  Delete,
  From,
  Where,
  Into,
  Values,
  Set,

  // --- Keywords (DDL) ---
  Create,
  Drop,
  Table,
  Database,
  Constraint,
  Primary,
  Key,
  Foreign,
  References,
  Unique,
  Index,
  Default,
  Null,
  Not,

  // --- Keywords (General) ---
  And,
  Or,
  As,
  On,
  In,
  Is,

  // --- Data Types (Keywords) ---
  Int,
  Integer,
  BigInt,
  Varchar,
  Char,
  Character,
  Boolean,
  Decimal,
  Float,
  Double,
  DateTime,
  Blob,
  Text,

  // --- Symbols & Operators ---
  Star,               // *
  Comma,              // ,
  Semicolon,          // ;
  OpenParen,          // (
  CloseParen,         // )
  Dot,                // .
  Equal,              // =
  NotEqual,           // != or <>
  GreaterThan,        // >
  LessThan,           // <
  GreaterThanOrEqual, // >=
  LessThanOrEqual,    // <=
  Plus,               // +
  Minus,              // -
  Slash               // /
}