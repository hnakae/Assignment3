from lstore.table import Table, Record, INDIRECTION_COLUMN, RID_COLUMN
from lstore.index import Index
import threading


class Transaction:
    transaction_counter = 0
    counter_lock = threading.Lock()

    """
    Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        with Transaction.counter_lock:
            self.id = Transaction.transaction_counter
            Transaction.transaction_counter += 1

        self.undo_log = []      # list of (action, payload)
        self._running = False

    """
    Adds the given query to this transaction
    Example:
    q = Query(grades_table)
    t = Transaction()
    t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args, table))

    
    def run(self):
        """
        Execute all queued queries with strict 2PL/no-wait. If any step
        fails, abort. Retries are attempted only for lock conflicts.
        """
        if self._running:
            return False
        self._running = True
        try:
            while True:
                self.undo_log.clear()
                ok, retryable = self._execute_once()
                if ok:
                    return self.commit()
                self.abort()
                if not retryable:
                    return False
        finally:
            self._running = False

    def _execute_once(self):
        for query, args, table in self.queries:
            # determine target rid/primary key for locking
            primary_key = None
            name = getattr(query, "__name__", "")
            if name == "insert":
                primary_key = args[table.key]
                if not table.lock_manager.acquire_exclusive(primary_key, self.id):
                    return False, True
            elif name in ("update", "delete"):
                primary_key = args[0]
                if not table.lock_manager.acquire_exclusive(primary_key, self.id):
                    return False, True
            elif name in ("select", "select_version"):
                primary_key = args[0]
                if not table.lock_manager.acquire_shared(primary_key, self.id):
                    return False, True

            # execute query; pass transaction if supported
            try:
                result = query(*args, transaction=self)
            except TypeError:
                result = query(*args)

            if result is False:
                return False, False
        return True, False

    
    def log_action(self, action, **payload):
        """
        Record an undo action. action is a string: insert/update/delete.
        Payload fields are action-specific.
        """
        self.undo_log.append((action, payload))

    
    def abort(self):
        # Roll back in reverse order
        for action, payload in reversed(self.undo_log):
            self._apply_undo(action, payload)
        self.undo_log.clear()
        # Release all locks held by this transaction
        self._release_all_locks()
        return False

    def commit(self):
        self.undo_log.clear()
        self._release_all_locks()
        return True

    
    def _release_all_locks(self):
        # Every table uses its own lock_manager; call release_all on each unique one.
        seen = set()
        for _, _, table in self.queries:
            lm = table.lock_manager
            if lm in seen:
                continue
            lm.release_all(self.id)
            seen.add(lm)

    def _apply_undo(self, action, payload):
        """
        Undo handler. Query operations are responsible for logging the
        necessary metadata in payload so rollback can restore state.
        """
        table = payload.get("table")
        if table is None:
            return

        if action == "insert":
            rid = payload.get("rid")
            primary_key = payload.get("primary_key")
            if primary_key is not None:
                with table.key_to_rid_lock:
                    table.key_to_rid.pop(primary_key, None)

            if rid is not None:
                with table.page_directory_lock:
                    record = table.page_directory.get(rid)
                    if record:
                        # mark tombstone
                        record[RID_COLUMN] = 0

            # remove from index
            user_values = payload.get("values")
            if user_values:
                for c, val in enumerate(user_values):
                    if table.index.indices[c] is not None:
                        table.index._remove(c, val, rid)

        elif action == "delete":
            rid = payload.get("rid")
            primary_key = payload.get("primary_key")
            old_values = payload.get("values") or []

            if rid is None or primary_key is None:
                return

            with table.page_directory_lock:
                record = table.page_directory.get(rid)
                if record:
                    record[RID_COLUMN] = rid

            with table.key_to_rid_lock:
                table.key_to_rid[primary_key] = rid

            for c, val in enumerate(old_values):
                if table.index.indices[c] is not None and val is not None:
                    table.index._add(c, val, rid)

        elif action == "update":
            rid = payload.get("rid")
            prior_tail = payload.get("prev_tail")
            new_tail = payload.get("new_tail")
            old_values = payload.get("old_values") or []
            new_values = payload.get("new_values") or []

            if rid is None:
                return

            # restore base indirection
            with table.page_directory_lock:
                base = table.page_directory.get(rid)
                if base:
                    base[INDIRECTION_COLUMN] = prior_tail if prior_tail is not None else 0

                # tombstone newly appended tail record if any
                if new_tail is not None and new_tail in table.page_directory:
                    table.page_directory[new_tail][RID_COLUMN] = 0

            # revert index changes
            for c in range(len(old_values)):
                old_val = old_values[c]
                new_val = new_values[c] if c < len(new_values) else None
                if table.index.indices[c] is None:
                    continue
                if new_val is not None:
                    table.index._remove(c, new_val, rid)
                if old_val is not None:
                    table.index._add(c, old_val, rid)
