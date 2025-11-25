# Assignment 3 Implementation Plan (Transactions & Concurrency)

## Prep
annotate files with labels to show exactly where record locks and mutexes should be taken during insert, update, and delete. (table.py, query.py, )

## Goals
- Add transaction semantics (begin/commit/abort with undo) and strict 2PL with no-wait behavior.
- Make core data paths thread-safe while keeping existing storage/versioning logic intact.
- Use a fixed worker-thread model to drive concurrent transactions and pass m3 testers.

## Milestone 1 — Survey & Thread-Safety Targets
- Trace current read/write paths in `query.py`, `table.py`, `index.py`, and bufferpool usage to list every shared structure mutated during insert/update/delete.
- Decide lock granularity: per-record (RID) for user data, plus coarse locks for shared maps (`key_to_rid`, `page_directory`, index trees).
- Note rollback needs for each query type: insert (delete inserted row), update (rewind base indirection + index changes), delete (restore tombstone/key map).

## Milestone 2 — Lock Manager (Strict 2PL, No-Wait)
- Implement a hash-table lock manager (new module or inside `table.py`) that supports shared/exclusive locks keyed by RID (or primary key before RID exists).
- Track lock owners and provide upgrade (S→X) when the same transaction escalates.
- Enforce no-wait: if a lock conflicts, immediately return failure so the caller can abort.
- Provide `release_all(txn_id)` to unlock everything held by a transaction.

## Milestone 3 — Transaction State & Undo
- Extend `Transaction` to own a unique id, a handle to the lock manager, a list of held locks, and an undo log capturing pre-images.
- Define undo records:
  - insert: mark for removal (drop key/rid and index entries).
  - update: store previous indirection head, previous index values, and tail rid count added.
  - delete: store original tombstone/key mapping.
- Implement `commit` to flush/return success and release locks; implement `abort` to walk the undo log, restore structures, and release locks.

## Milestone 4 — Instrument Queries with 2PL
- For each Query op, request locks via the transaction before touching data:
  - select/select_version/sum/sum_version: shared locks on target RIDs; bail out on conflict.
  - insert: exclusive lock on the new key (or provisional id) before allocation.
  - update/delete/increment: exclusive locks on target RIDs; record undo info before mutating.
- Ensure index maintenance and metadata updates run under appropriate locks; reuse table-level mutexes where contention on dictionaries is possible.
- On lock acquisition failure, trigger transaction abort path (no partial state left).

## Milestone 5 — Worker Threads
- Implement `TransactionWorker.run` to spin up a single long-lived `threading.Thread` that calls a private `__run` loop; keep the thread list size fixed.
- Implement `join` to wait on the worker; ensure stats/result reflect committed count.
- Guard against reentrancy (don’t re-run an already running worker).

## Milestone 6 — Validation
- Add lightweight unit-style checks for lock conflicts and abort/undo behavior.
- Run provided testers: `python3 m3_tester_part_1.py` then `python3 m3_tester_part_2.py` (and exam variants) to confirm concurrency correctness and versioned reads still pass.

## Notes / Assumptions
- Stay within Python `threading` (GIL-aware) and avoid spawning transient threads per txn; rely on the worker pool pattern.
- Maintain existing bufferpool/page I/O behavior; locking should wrap, not rewrite, storage logic.
- Keep code ASCII and minimal; add comments only where lock/undo flow would be non-obvious.
