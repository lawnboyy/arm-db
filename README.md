# ArmDb: A Relational Database from Scratch

## Introduction

**ArmDb** is a relational database management system (RDBMS) built from the ground up in C# using .NET 9. This project was undertaken as a learning exercise to explore and implement the core concepts of database internals.

The goal is not to create a production-ready database but to gain a deep, practical understanding of how database systems work, from low-level page management on disk to high-level query processing.

## Project Goals

The primary goal is to implement the fundamental components of a modern RDBMS, including:

* **Schema Definition:** The ability to define tables, columns, data types, and constraints (Primary Keys, Foreign Keys).
* **Disk-Based Storage Engine:**
    * Management of data on disk using fixed-size pages.
    * An in-memory **Buffer Pool Manager** to cache pages and minimize disk I/O.
    * A **Slotted Page** structure to efficiently store variable-length records.
* **B+Tree Clustered Indexes:** Storing all table data within a B+Tree structure, ordered by the table's primary key, to ensure efficient lookups and range scans.
* **SQL Query Processing:** Eventually, the ability to parse and execute standard SQL queries for data manipulation (DML) and definition (DDL).
* **ACID Compliance (Future Goal):** Implementing transaction management, concurrency control (locking/latching), and crash recovery (via a Write-Ahead Log).

## Current Architecture

The project is being built with a layered, fine-grained architecture to ensure a clear separation of concerns:

* `ArmDb.SchemaDefinition`: Defines the logical structure of the database (metadata).
* `ArmDb.DataModel`: Defines the in-memory representation of runtime data (`DataRow`, `DataValue`).
* `ArmDb.Common.Abstractions`: Contains shared interfaces like `IFileSystem`.
* `ArmDb.Common.Utils`: Contains concrete utility implementations.
* `ArmDb.StorageEngine`: The core storage layer, responsible for managing pages on disk and in memory. Contains components like the `DiskManager` and `BufferPoolManager`.
* `ArmDb.Server`: The main server process responsible for handling connections and orchestrating database operations.
* `ArmDb.Tests`: A suite of unit and integration tests to drive development and ensure correctness.

## Technology Stack

* **Language:** C# 13
* **Framework:** .NET 9
* **Testing:** xUnit

## How to Build & Run

*(Instructions to be added later)*
