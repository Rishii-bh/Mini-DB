[MINIDB-README.md](https://github.com/user-attachments/files/27455900/MINIDB-README.md)

# MiniDB

MiniDB is a small database project written in Java from scratch.

The goal of this project is to learn how databases work internally by building the main parts myself: parsing SQL, analyzing queries, executing them, and storing data persistently.

---

## Current Features

MiniDB currently supports a small SQL-like language with:

- `CREATE TABLE`
- `INSERT INTO`
- `SELECT ... WHERE`
- `DELETE ... WHERE`
- Basic type checking
- A REPL-style command interface
- Persistent file-based storage
- Binary row serialization

Supported types:

INT
TEXT
BOOL


Example:

sql
CREATE TABLE students (id INT, name TEXT, active BOOL);

INSERT INTO students (id, name, active)
VALUES (1, "Rishi", true);

SELECT id, name FROM students WHERE active = true;

DELETE FROM students WHERE id = 1;


---

## Architecture

The project is split into several layers:


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


The parser only checks syntax and creates raw query objects.

The analyzer checks database-specific meaning, such as whether tables and columns exist and whether types match.

The executor runs the resolved query.

The storage engine handles where the data is stored.

---

## Storage

The project started with in-memory storage and now supports persistent file-based storage.

The current binary storage layout stores schemas separately from row data.

Example table directory:


data/
  students/
    schema.meta
    rows.bin

The schema is stored as text:


id:INT
name:TEXT
active:BOOL

Rows are stored in binary format. Each row is written as:


[row length][row bytes]


This makes it possible to store variable-length values such as `TEXT`.

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

Temporary test directories are used so tests do not modify real database files.

---

## Current Limitations

MiniDB is still a learning project and does not yet support:

- Joins
- Indexes
- Transactions
- Concurrency
- Crash recovery
- Page-based storage
- `UPDATE`
- `DROP TABLE`
- `NULL`
- Primary keys or constraints

---

## Next Steps

Planned improvements:

- Add tombstone-based deletion instead of rewriting the whole row file
- Add a compaction step to clean up deleted rows
- Move from simple binary row files to page-based storage
- Add page headers and slot directories
- Add support for `UPDATE`
- Add more SQL features gradually

---

## Notes

This project is mainly for learning database internals. The focus is on understanding the design and implementation of a database system step by step, not on building a production-ready database.

