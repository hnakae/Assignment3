# CS 451/551: Database Processing, Fall 2025

Acknowledgments and Thanks to Prof. Mohammad Sadoghi (UC Davis)

## Author: Hiro Nakae <hnakae@uoregon.edu> & Harry

## Reminder
to run tests.py, run:
```rm -rf CS451```
```python3 m2_tester_part_1.py```
```python3 m2_tester_part_2.py```
or use Makefile:
```make m2_tester```

# assignment2 — TODO with PHASE labels

## PHASE 1 — Durability Skeleton (open/close + metadata)

- [x] open/close + metadata (storage.py)
- [x] open/close hooks (db.py)
- [x] table metadata (table.py)
- [x] page serialization + page load/save (page.py)

## PHASE 2 — Core L-Store Record Logic (base/tail lineage)

- [x] tail records update/select/sum (query.py / table.py)
- [x] maintain correct indirection chain, BaseRID, schema encoding, etc.
- [x] update page_directory correctly during insert/update
- [x] delete logic consistent with assignment

## PHASE 3 — BufferPool + Disk Page Integration

- [ ] bufferpool + disk pages (bufferpool.py)
- [ ] give table bufferpool instance (table.py)
- [ ] replace direct page access with bufferpool access (query.py / table.py)

## PHASE 4 — Versioning API (exam tester part 2)

- [x] select_version and sum_version (query.py)
- [x] version traversal using tail lineage

---

## info

1. Vocabulary and Key Terms

   * Record: A single row of data in a
     table, like a row in a
     spreadsheet. Each record has a
     unique ID and a set of column
     values.
   * RID (Record ID): A unique,
     table-wide identifier for a
     record. It's used to find the
     physical location of the base
     record on disk.
   * TID (Tail Record ID): A unique ID
     for a tail record. Tail records
     are created when a base record is
     updated.
   * Base Record: The very first
     version of a record, stored in a
     "base page."
   * Tail Record: A record that stores
     only the updated values for a
     base record. When an update
     occurs, a new tail record is
     created, and the base record is
     modified to point to it.
   * Page: A fixed-size block of data
     (4KB in this project) that is the
     smallest unit of data transferred
     between the bufferpool (memory)
     and disk.
   * Page Range: A collection of pages
     that stores a large number of
     records. Each page range contains
     a set of base pages and a set of
     tail pages.
   * Bufferpool: An in-memory cache
     for pages. It keeps frequently
     accessed pages in memory to
     minimize slow disk I/O
     operations.
   * Index: A data structure that
     enables fast lookups of records
     based on column values. This
     project uses a primary key index
     and secondary indexes.
   * Primary Key: A column that
     contains a unique value for each
     record, used for direct lookups.
   * MVCC (Multi-Version Concurrency
     Control): A technique that allows
     multiple transactions to read and
     write to the database at the same
     time without locking. It works by
     creating a new version of a
     record every time it's updated.
   * Transaction: A sequence of
     database operations (e.g., reads,
     writes) that are executed as a
     single, atomic unit of work.
   * Merge: A background process that
     consolidates the update history
     of records by combining base
     records with their tail records
     to create new, updated base
     records. This improves read
     performance over time.

  2. How lstore Works: The Big Picture

  lstore is a simplified relational
  database engine. Here’s a breakdown
  of its architecture:

   1. Data Storage:
       * When a record is inserted, it
         is stored as a base record in
         a base page.
       * When a record is updated, the
         original base record is not
         modified. Instead, a new tail
         record is created with the
         updated values and stored in
         a tail page.
       * The base record's
         "indirection column" is then
         updated to point to this new
         tail record, forming a linked
         list of updates. The head of
         this list is always the most
         recent version.

   2. Querying:
       * Select: To find a record, the
         system uses an index to get
         the record's rid. It then
         reads the base record. If the
         record has been updated, it
         follows the chain of tail
         records to reconstruct the
         most recent version.
       * `select_version`: This is a
         special version of select
         that allows you to retrieve
         older versions of a record by
         specifying a
         relative_version.

   3. Performance:
       * The bufferpool acts as a
         cache to keep frequently used
         pages in memory, which is
         much faster than reading from
         disk every time.
       * The merge process is crucial
         for long-term performance. It
         runs in the background to
         clean up the update history,
         preventing the tail record
         chains from becoming too long
         and slowing down reads.

  3. Code Organization

   * lstore/: This is the core
     directory for the database
     engine.
       * db.py: Defines the Database
         class, which manages tables.
       * table.py: Defines the Table
         class, which handles the
         storage of records.
       * query.py: Implements the
         logic for insert, select,
         update, and delete.
       * index.py: Implements the
         indexing data structures.
       * page.py: Represents a single
         page of data.
       * bufferpool.py: Implements the
         in-memory page cache.
       * transaction.py: Manages
         transactions and concurrency.
       * config.py: Contains global
         settings like page size.
   * exam_tester_*.py: These are the
     scripts you will use to test your
     implementation for each
     milestone.