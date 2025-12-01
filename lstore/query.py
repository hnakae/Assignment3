from lstore.table import Table, Record
from lstore.index import Index
from time import time


class Query:
    """
    Query executor for a single table.
    """

    def __init__(self, table):
        self.table = table



    def delete(self, primary_key, transaction=None):
        """
        Deletes (logically) the record with the given primary key.

        HOT: touches shared metadata and index
        Required locking:
            - RECORD LOCK on base_rid
            - MUTEX on table.key_to_rid
            - MUTEX on table.page_directory
            - MUTEX on index structure
        """
        try:
            txn = transaction
            if txn is not None:
                if not self.table.lock_manager.acquire_exclusive(primary_key, txn.id):
                    return False

            # HOT: lookup in shared dict
            base_rid = self.table.key_to_rid.get(primary_key)
            if base_rid is None:
                return False

            # RECORD LOCK should be acquired on base_rid here

            # HOT: index + snapshot read
            full = self._build_record_from_data(base_rid, [1] * self.table.num_columns)

            if txn is not None:
                txn.log_action(
                    "delete",
                    table=self.table,
                    rid=base_rid,
                    primary_key=primary_key,
                    values=list(full.columns),
                )

            # HOT: index mutation
            # MUTEX: protect index structure
            for c in range(self.table.num_columns):
                if self.table.index.indices[c] is not None:
                    self.table.index._remove(c, full.columns[c], base_rid)

            # HOT: page_directory mutation
            # RECORD LOCK: protects this base record’s metadata
            base_record = self.table.page_directory[base_rid]
            base_record[1] = 0  # tombstone

            # HOT: key_to_rid mutation
            # MUTEX required
            del self.table.key_to_rid[primary_key]

            return True

        except Exception:
            return False

    """
    =============================
    INSERT
    =============================
    """

    def insert(self, *columns, transaction=None):
        """
        Insert a new base record.

        HOT: updates global metadata, pages, and index
        Required locking:
            - MUTEX on table.key_to_rid
            - MUTEX on table.page_directory
            - MUTEX or atomic on table.next_rid
            - MUTEX on page allocation metadata
            - MUTEX on index structure
        """

        try:
            if len(columns) != self.table.num_columns:
                return False

            primary_key = columns[self.table.key]
            txn = transaction

            if txn is not None:
                if not self.table.lock_manager.acquire_exclusive(primary_key, txn.id):
                    return False

            # HOT: shared dict lookup
            with self.table.key_to_rid_lock:
                if primary_key in self.table.key_to_rid:
                    # overwrite existing record to make reruns idempotent
                    existing_rid = self.table.key_to_rid[primary_key]
                    old_full = self._build_record_from_data(existing_rid, [1] * self.table.num_columns)

                    schema_encoding = '0' * self.table.num_columns
                    record_data = [
                        0,
                        existing_rid,
                        int(time()),
                        schema_encoding,
                        *columns,
                    ]

                    with self.table.page_directory_lock:
                        self.table.page_directory[existing_rid] = record_data

                    # reset positional metadata for this base record
                    with self.table.page_metadata_lock:
                        if getattr(self.table, "base_positions", None) is not None:
                            self.table.base_positions[existing_rid] = None

                    # refresh index entries
                    for c in range(self.table.num_columns):
                        if self.table.index.indices[c] is not None:
                            self.table.index._remove(c, old_full.columns[c], existing_rid)
                            self.table.index._add(c, columns[c], existing_rid)

                    if txn is not None:
                        txn.log_action(
                            "update",
                            table=self.table,
                            rid=existing_rid,
                            prev_tail=record_data[0],
                            new_tail=None,
                            old_values=list(old_full.columns),
                            new_values=list(columns),
                        )
                    return True

            # HOT: global RID assignment
            # MUTEX or atomic increment required
            with self.table.next_rid_lock:
                rid = self.table.next_rid
                self.table.next_rid += 1

            schema_encoding = '0' * self.table.num_columns
            record_data = [
                0,
                rid,
                int(time()),
                schema_encoding,
                *columns,
            ]

            # HOT: page_directory mutation
            # MUTEX required
            with self.table.page_directory_lock:
                self.table.page_directory[rid] = record_data

            # HOT: key_to_rid mutation
            # MUTEX required
            with self.table.key_to_rid_lock:
                self.table.key_to_rid[primary_key] = rid

            # HOT: page writes and page metadata mutation
            # MUTEX required via table._append_base_record
            with self.table.page_metadata_lock:
                if getattr(self.table, "_append_base_record", None):
                    positions = self.table._append_base_record(columns)
                    if positions is not None:
                        # HOT: base_positions update
                        # MUTEX required
                        self.table.base_positions[rid] = positions

            # HOT: index mutation
            # MUTEX on index
            for c in range(self.table.num_columns):
                if self.table.index.indices[c] is not None:
                    self.table.index._add(c, columns[c], rid)

            if txn is not None:
                txn.log_action(
                    "insert",
                    table=self.table,
                    rid=rid,
                    primary_key=primary_key,
                    values=list(columns),
                )

            return True

        except Exception:
            return False

    """
    =============================
    SELECT
    =============================
    """

    def select(self, search_key, search_key_index, projected_columns_index, transaction=None):
        """
        Read operation.

        HOT: reads shared metadata and index
        Required locking:
            - RECORD LOCK for each target rid (depending on isolation level)
            - MUTEX for safe traversal of key_to_rid and index dicts
        """

        try:
            txn = transaction
            if txn is not None and search_key_index == self.table.key:
                if not self.table.lock_manager.acquire_shared(search_key, txn.id):
                    return False
            # Primary key fast path
            if search_key_index == self.table.key:

                # HOT: shared dict lookup
                rid = self.table.key_to_rid.get(search_key)
                if rid is None:
                    return []

                # RECORD LOCK should be held while building record
                return [self._build_record_from_data(rid, projected_columns_index)]

            # Secondary index path
            if self.table.index.indices[search_key_index] is not None:

                # HOT: index structure read
                # MUTEX required
                rids = self.table.index.locate(search_key_index, search_key) or []

                return [self._build_record_from_data(rid, projected_columns_index) for rid in rids]

            # Fallback: linear scan
            results = []

            # HOT: iterating shared dictionary
            # MUTEX required
            for rid in self.table.key_to_rid.values():

                full = self._build_record_from_data(rid, [1] * self.table.num_columns)

                if full.columns[search_key_index] == search_key:
                    projected = [
                        (full.columns[i] if include == 1 else None)
                        for i, include in enumerate(projected_columns_index)
                    ]
                    results.append(Record(full.rid, full.key, projected))

            return results

        except Exception:
            return False

    """
    =============================
    RECORD MATERIALIZATION
    =============================
    """

    def _build_record_from_data(self, base_rid, projected_columns_index):
        """
        Reconstruct latest version of a record.

        HOT: reads page_directory, base_positions, tail_positions
        Required locking:
            - RECORD LOCK on base_rid
            - RECORD LOCK on each traversed tail_rid
            - MUTEX on page_directory dict
        """

        # HOT: shared metadata read
        with self.table.page_directory_lock:
            base = self.table.page_directory[base_rid]

        with self.table.page_metadata_lock:
            base_positions = getattr(self.table, "base_positions", {}).get(base_rid)

        # Read base version
        if base_positions:
            user_cols = []
            for c in range(self.table.num_columns):
                pos = base_positions[c]
                if pos is not None:
                    val = self.table._read_value_at(True, c, pos[0], pos[1])
                    if val is None:
                        val = base[4 + c]
                    user_cols.append(val)
                else:
                    user_cols.append(base[4 + c])
        else:
            user_cols = list(base[4:])

        # Traverse tail chain
        tail_rid = base[0]
        tail_records = []

        # HOT: repeated access to page_directory
        with self.table.page_directory_lock:
            while tail_rid != 0:
                tail = self.table.page_directory[tail_rid]
                tail_records.append(tail)
                tail_rid = tail[0]

        # Apply tail updates
        for tail in reversed(tail_records):
            schema = tail[3]
            tail_rid_local = tail[1]
            with self.table.page_metadata_lock:
                tail_positions = getattr(self.table, "tail_positions", {}).get(tail_rid_local)

            for i in range(self.table.num_columns):
                if schema[i] == '1':

                    val = None

                    if tail_positions and tail_positions[i] is not None:
                        pos = tail_positions[i]
                        val = self.table._read_value_at(False, i, pos[0], pos[1])

                    if val is None:
                        val = tail[4 + i]

                    user_cols[i] = val

        # Projection
        projected = []
        for i, include in enumerate(projected_columns_index):
            projected.append(user_cols[i] if include == 1 else None)

        primary_key = user_cols[self.table.key]

        return Record(base_rid, primary_key, projected)

    """
    =============================
    SELECT VERSION
    =============================
    """

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
        """
        Read older versions.

        HOT: heavy traversal of tail chain
        RECORD LOCK: base + tail records
        MUTEX: page_directory and position maps
        """

        try:
            txn = transaction
            if txn is not None and search_key_index == self.table.key:
                if not self.table.lock_manager.acquire_shared(search_key, txn.id):
                    return False
            if search_key_index != self.table.key:
                return False

            base_rid = self.table.key_to_rid.get(search_key)
            if base_rid is None:
                return []

            base = self.table.page_directory[base_rid]
            base_positions = getattr(self.table, "base_positions", {}).get(base_rid)

            if base_positions:
                user_cols = []
                for c in range(self.table.num_columns):
                    pos = base_positions[c]
                    if pos is not None:
                        val = self.table._read_value_at(True, c, pos[0], pos[1])
                        if val is None:
                            val = base[4 + c]
                        user_cols.append(val)
                    else:
                        user_cols.append(base[4 + c])
            else:
                user_cols = list(base[4:])

            tail_rid = base[0]

            version_to_find = abs(relative_version)
            current_version = 0

            # Skip newer versions
            while tail_rid != 0 and current_version < version_to_find:
                tail = self.table.page_directory[tail_rid]
                tail_rid = tail[0]
                current_version += 1

            tail_records = []

            while tail_rid != 0:
                tail = self.table.page_directory[tail_rid]
                tail_records.append(tail)
                tail_rid = tail[0]

            # Apply tail updates
            for tail in reversed(tail_records):
                schema = tail[3]
                tail_rid_local = tail[1]
                tail_positions = getattr(self.table, "tail_positions", {}).get(tail_rid_local)

                for i in range(self.table.num_columns):
                    if schema[i] == '1':

                        val = None

                        if tail_positions and tail_positions[i] is not None:
                            pos = tail_positions[i]
                            val = self.table._read_value_at(False, i, pos[0], pos[1])

                        if val is None:
                            val = tail[4 + i]

                        user_cols[i] = val

            projected = []
            for i, include in enumerate(projected_columns_index):
                projected.append(user_cols[i] if include == 1 else None)

            primary_key = user_cols[self.table.key]

            return [Record(base_rid, primary_key, projected)]

        except Exception:
            return False

    """
    =============================
    UPDATE
    =============================
    """

    def update(self, primary_key, *columns, transaction=None):
        """
        Update an existing record.

        HOT: modifies tail chain, page_directory, index
        Required locking:
            - RECORD LOCK on base_rid
            - RECORD LOCK on new tail_rid
            - MUTEX on table.page_directory
            - MUTEX or atomic on table.next_rid
            - MUTEX on index structures
        """

        try:
            txn = transaction

            if txn is not None:
                if not self.table.lock_manager.acquire_exclusive(primary_key, txn.id):
                    return False

            if len(columns) != self.table.num_columns:
                return False

            with self.table.key_to_rid_lock:
                base_rid = self.table.key_to_rid.get(primary_key)
                if base_rid is None:
                    return False

            # RECORD LOCK should be acquired on base_rid here

            # Old value snapshot for index updates
            old_full = self._build_record_from_data(base_rid, [1] * self.table.num_columns)

            with self.table.page_directory_lock:
                base = self.table.page_directory[base_rid]
                latest_tail_rid = base[0]

            schema_bits = ['1' if c is not None else '0' for c in columns]
            schema_encoding = "".join(schema_bits)

            if '1' not in schema_encoding:
                return True

            # HOT: assign new tail RID
            # MUTEX or atomic needed
            with self.table.next_rid_lock:
                tail_rid = self.table.next_rid
                self.table.next_rid += 1

            tail_values = [c if c is not None else 0 for c in columns]
            tail_data = [latest_tail_rid, tail_rid, int(time()), schema_encoding, *tail_values]

            # HOT: add new tail entry
            # MUTEX required
            with self.table.page_directory_lock:
                self.table.page_directory[tail_rid] = tail_data

            # HOT: modify base indirection pointer
            # RECORD LOCK protects this
            with self.table.page_directory_lock:
                base[0] = tail_rid

            # HOT: write to tail pages
            with self.table.page_metadata_lock:
                if getattr(self.table, "_append_tail_updates", None):
                    positions = self.table._append_tail_updates(columns)
                    if positions is not None:
                        # HOT: tail_positions mutation
                        # MUTEX required
                        self.table.tail_positions[tail_rid] = positions

            # HOT: index updates
            # MUTEX on index
            for c, new_val in enumerate(columns):
                if new_val is not None and self.table.index.indices[c] is not None:
                    self.table.index._remove(c, old_full.columns[c], base_rid)
                    self.table.index._add(c, new_val, base_rid)

            if txn is not None:
                txn.log_action(
                    "update",
                    table=self.table,
                    rid=base_rid,
                    prev_tail=latest_tail_rid,
                    new_tail=tail_rid,
                    old_values=list(old_full.columns),
                    new_values=list(columns),
                )

            return True

        except Exception:
            return False

    """
    =============================
    SUM RANGE
    =============================
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Aggregate across key range.

        HOT: reads key_to_rid and page_directory repeatedly
        MUTEX required for safe iteration
        """

        try:
            total = 0
            found_any = False

            # HOT: iterating shared dictionary
            for key, rid in self.table.key_to_rid.items():
                if start_range <= key <= end_range:
                    found_any = True
                    rec = self._build_record_from_data(rid, [1] * self.table.num_columns)
                    total += rec.columns[aggregate_column_index]

            return total if found_any else False

        except Exception:
            return False

    """
    =============================
    SUM VERSION
    =============================
    """

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
        Aggregation over historical versions.

        HOT: repeated record materialization
        MUTEX: required for safe structure access
        """

        try:
            total = 0
            found_any = False

            for key, rid in self.table.key_to_rid.items():
                if start_range <= key <= end_range:

                    rec_list = self.select_version(
                        key,
                        self.table.key,
                        [1] * self.table.num_columns,
                        relative_version
                    )

                    if rec_list:
                        found_any = True
                        record = rec_list[0]
                        value = record.columns[aggregate_column_index]
                        if value is not None:
                            total += value

            return total if found_any else False

        except Exception:
            return False

    """
    =============================
    INCREMENT
    =============================
    """

    def increment(self, key, column, transaction=None):
        """
        Convenience wrapper: select + update.

        HOT: invokes both select and update paths
        Locks required are inherited from those operations.
        """

        rlist = self.select(key, self.table.key, [1] * self.table.num_columns, transaction=transaction)
        if rlist is False or not rlist:
            return False
        r = rlist[0]

        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns, transaction=transaction)
            return u

        return False
