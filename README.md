# MiniDB

MiniDB is a small database project written in Java from scratch.

The goal of this project is to learn how databases work internally by building the main parts step by step: parsing SQL, analyzing queries, executing them, indexing rows, caching metadata, and storing data persistently.

---

## Current Features

MiniDB currently supports a small SQL-like language with:

- `CREATE TABLE`
- `INSERT INTO`
- `SELECT ... WHERE`
- `DELETE ... WHERE`
- Basic type checking
- A REPL-style command interface
- Persistent page-based file storage
- Binary row serialization
- Schema metadata persistence
- Schema caching during runtime
- Persistent equality indexes
- Index catalog persistence
- Lazy loading of index data from disk
- Runtime index caching
- Direct row lookup using `RecordId(pageId, slotId)`
- Page-local deletion with slot reuse

Supported types:

```text
INT
TEXT
BOOL
```

Example:

```sql
CREATE TABLE students (id INT, name TEXT, active BOOL);

INSERT INTO students (id, name, active)
VALUES (1, "Rishi", true);

SELECT id, name FROM students WHERE active = true;

DELETE FROM students WHERE id = 1;
```

---

## Architecture

The project is split into several layers:

```text
SQL input
  ↓
Lexer
  ↓
Parser
  ↓
Analyzer
  ↓
Executor
  ↓
Storage / Indexing layer
```

The parser checks syntax and creates raw query objects.

The analyzer checks database-specific meaning, such as whether tables and columns exist and whether types match.

The executor runs the resolved query. For supported equality-based `WHERE` conditions, it can use the index manager when an index exists. If no usable index exists, it falls back to a normal table scan.

The storage layer handles persistent table files, schemas, pages, rows, and physical `RecordId` access.

The index layer handles index catalogs, persistent index files, runtime index caching, and direct lookup from indexed values to physical row locations.

---

## Runtime Lifecycle

MiniDB now uses a higher-level database runner/facade to wire the system together.

At startup:

```text
DatabaseRunner.start()
  ↓
Load schemas into SchemaManager cache
  ↓
Load index catalog metadata into IndexManager cache
  ↓
Actual index files are loaded lazily when first needed
```

During execution:

```text
INSERT
  ↓
Write row into page storage
  ↓
Update any indexes that exist for the table

DELETE
  ↓
Find matching rows
  ↓
Mark/delete rows from page storage
  ↓
Remove deleted RecordIds from all affected indexes
  ↓
Make freed slots reusable

SELECT ... WHERE indexed_column = value
  ↓
Use index lookup to get RecordIds
  ↓
Fetch rows directly from page storage
```

At shutdown:

```text
DatabaseRunner.shutdown()
  ↓
Flush dirty index data back to disk
```

---

## Storage

The project started with in-memory storage, moved to simple binary file storage, and now supports persistent page-based storage.

Schemas are stored separately from table data and are loaded into a schema cache at startup.

Example table directory:

```text
data/
  students/
    schema.meta
    tables.dat
    indexes.meta
    indexes/
      students_id.idx
```

The schema is stored as text:

```text
id:INT
name:TEXT
active:BOOL
```

`tables.dat` stores fixed-size 4096-byte pages.
Each page contains a header, slot directory, free space region, and row bytes.
Rows are serialized using `BinaryRowSerializer` before being inserted into pages.

Rows can be addressed physically using:

```text
RecordId(pageId, slotId)
```

This enables direct row lookup and makes indexes useful because an index can point straight to physical row locations.

---

## Page Deletion and Slot Reuse

MiniDB supports page-local deletion and deleted-slot reuse.

When a row is deleted, the storage layer can make the deleted slot available for future inserts instead of always appending new row data. This reduces unnecessary page growth and prevents the table file from expanding for every insert/delete cycle.

Slot reuse also interacts with indexing. When a row is deleted, all indexes on that table must remove the deleted row's `RecordId`. If that slot is later reused by another row, old index entries must not point to the new row incorrectly.

This is an important consistency rule:

```text
An index entry must only point to a live row whose indexed column value matches the index key.
```

---

## Indexing

MiniDB now supports persistent equality indexes.

An index maps typed values to physical row locations:

```text
Value(type, value) -> Set<RecordId>
```

Example:

```text
students.id index

1 -> [(page 0, slot 0), (page 1, slot 2)]
2 -> [(page 0, slot 1)]
```

Index metadata is stored in a table-level index catalog. The catalog records which columns are indexed and where the index files are stored.

Index data itself is stored separately in persistent index files. At startup, MiniDB loads index catalog metadata, but it does not eagerly load every full index into memory. Actual index contents are loaded lazily when a query or modification first needs them.

Current indexing behavior:

- Indexes support equality-based lookups.
- Index catalog metadata is persisted.
- Index contents are persisted.
- Index metadata is cached at startup.
- Full index data is loaded lazily.
- Loaded indexes are cached in memory.
- Inserts update all existing indexes for the affected table.
- Deletes remove deleted `RecordId`s from all existing indexes for the affected table.
- Dirty indexes are flushed back to disk.

---

## Caching

MiniDB now separates metadata caching from data loading.

Schema caching:

```text
SchemaManager
  - loads table schemas from disk
  - caches schemas by table name
  - is used by analyzer, executor, storage, and indexing code
```

Index catalog caching:

```text
IndexManager
  - loads index catalog metadata from table directories
  - knows which table columns have persisted indexes
  - does not need to load full index contents immediately
```

Index data caching:

```text
IndexManager
  - loads full index files lazily
  - keeps loaded indexes in memory
  - marks modified indexes dirty
  - rewrites dirty indexes when needed
```

This avoids repeatedly reading schema and index metadata from disk during query execution.

---

## Testing

The project has tests for:

- Creating tables
- Inserting rows
- Selecting rows
- Deleting rows
- Restart persistence
- Bad inputs
- Ensuring failed queries do not corrupt stored data
- Page-based row storage and scanning
- Direct row lookup using `RecordId`
- Page-local deletion
- Deleted-slot reuse
- Persistent index serialization
- Index catalog serialization and loading
- Building and querying indexes
- Verifying indexed `WHERE` queries use the index path
- Updating indexes after inserts
- Removing deleted `RecordId`s from indexes after deletes
- Loading index catalogs after restart
- Lazy loading index contents after restart
- Multi-table workflows with indexes
- Full workflow tests across storage, schema caching, indexing, deletion, and restart behavior

Temporary test directories are used so tests do not modify real database files.

---

## Current Limitations

MiniDB is still a learning project and does not yet support:

- Joins
- B-tree or B+ tree indexes
- Transactions
- Concurrency
- Crash recovery
- `UPDATE`
- `DROP TABLE`
- `NULL`
- Primary keys or constraints
- Query planning beyond simple equality-index selection

---

## Next Steps

Planned improvements:

- Add B-tree or B+ tree based index structures
- Add support for `UPDATE`
- Strengthen workflow and failure-safety tests
- Add more detailed execution statistics for indexed vs scanned queries
- Improve query planning beyond basic indexed equality lookup
- Add more SQL features gradually
- Explore transaction logging and crash recovery later

---

## Updates

- Moved from simple binary row storage to page-based storage where slot access is O(1) and row lookup by `RecordId` is O(1) after the page is loaded.
- Added equality-based indexing support for `WHERE` queries.
- Added persistent index files and table-level index catalogs.
- Added schema caching through a schema manager.
- Added index catalog caching and lazy index data loading.
- Added insert/delete maintenance for persisted indexes.
- Added page-local deletion and deleted-slot reuse.
- Added tests for index serialization, catalog persistence, lazy loading, indexed query execution, slot reuse, and multi-table workflows.

---

## Notes

This project is mainly for learning database internals. The focus is on understanding the design and implementation of a database system step by step, not on building a production-ready database.
