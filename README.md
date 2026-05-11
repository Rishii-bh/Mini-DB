# MiniDB

MiniDB is a small database project written in Java from scratch.

The goal of this project is to learn how databases work internally by parsing SQL, analyzing queries, executing them, indexing rows, and storing data persistently.

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
- In-memory indexing for equality-based `WHERE` lookups
- Direct row lookup using `RecordId(pageId, slotId)`

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
StorageEngine
```

The parser only checks syntax and creates raw query objects.

The analyzer checks database-specific meaning, such as whether tables and columns exist and whether types match.

The executor runs the resolved query. For supported equality-based `WHERE` conditions, it can use the in-memory index manager when an index exists; otherwise, it falls back to a normal table scan.

The storage engine handles where the data is stored.

---

## Storage

The project started with in-memory storage, moved to simple binary file storage, and now supports persistent page-based storage.

Schemas are stored separately from table data.

Example table directory:

```text
data/
  students/
    schema.meta
    tables.dat
```

The schema is stored as text:

```text
id:INT
name:TEXT
active:BOOL
```

`tables.dat` stores fixed-size 4096-byte pages.
Each page contains a header, slot directory, free space region, and row bytes.
Rows are still serialized using `BinaryRowSerializer` before being inserted into pages.

Rows can be addressed physically using:

```text
RecordId(pageId, slotId)
```

This allows direct row lookup and enables indexing.

---

## Indexing

MiniDB now supports in-memory indexes for equality lookups.

An index maps typed values to physical row locations:

```text
Value(type, value) -> List<RecordId>
```

Example:

```text
students.id index

1 -> [(page 0, slot 0), (page 1, slot 2)]
2 -> [(page 0, slot 1)]
```

The index manager stores indexes by table and column. During indexed `SELECT ... WHERE` execution, matching `RecordId`s are used to fetch rows directly from page-based storage.

Current indexing behavior:

- Indexes are in-memory only.
- Indexes support equality conditions only.
- Inserts update existing in-memory indexes.

---

- ## Page-Local Deletion and Slot Reuse

The storage layer now supports page-local deletion using stable `RecordId`s. A `RecordId` is represented as a `(pageId, slotId)` pair, so deleting a row no longer requires rewriting the entire table file or shifting other rows.

Instead of physically removing rows immediately, deleted rows are marked as tombstoned inside the page slot directory. Query scans and direct record lookups ignore tombstoned slots, so deleted rows are no longer visible to the query layer.

To avoid unbounded wasted space, pages can now reuse deleted slots during insertion. When a new row fits into the space previously occupied by a deleted row, the storage engine reuses that slot and returns the same `RecordId` for the newly inserted row. This keeps row locations stable while reducing unnecessary page growth.

The page header also tracks the largest deleted slot length, allowing insert operations to quickly determine whether a page may contain a reusable deleted slot before scanning the slot directory.

Index maintenance has been updated to work correctly with tombstone deletion and slot reuse. When rows are deleted, their `RecordId`s are removed from all relevant in-memory indexes before the slot is reused. This prevents stale index entries from pointing to newly inserted rows that occupy the same physical slot.

Additional tests now cover:
- tombstone-based deletion
- direct lookup of deleted records
- slot reuse after deletion
- preventing reuse when the new row is too large
- index correctness after delete and reuse
- persistence of reused slots after reload

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
- Building and querying in-memory indexes
- Verifying indexed `WHERE` queries use the index path
- Updating indexes after inserts
- Rebuilding affected indexes after deletes

Temporary test directories are used so tests do not modify real database files.

---

## Current Limitations

MiniDB is still a learning project and does not yet support:

- Joins
- Persistent indexes
- B-tree or B+ tree indexes
- Transactions
- Concurrency
- Crash recovery
- `UPDATE`
- `DROP TABLE`
- `NULL`
- Primary keys or constraints

---

## Next Steps

Planned improvements:
- Replace or extend in-memory indexes with persistent index structures
- B+Trees Implementation
- Add support for `UPDATE`
- Add more SQL features gradually

---

## Updates

- Moved from simple binary row storage to page-based storage where slot access is O(1) and row lookup by `RecordId` is O(1) after the page is loaded.
- Added in-memory indexing support for equality-based `WHERE` queries.
- Added tests for page-based storage, direct `RecordId` lookup, indexed query execution, and index maintenance after inserts/deletes.


## Notes

This project is mainly for learning database internals. The focus is on understanding the design and implementation of a database system step by step, not on building a production-ready database.
