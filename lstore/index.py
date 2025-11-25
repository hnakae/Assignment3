"""
A data structure holding indices for various columns of a table.
Key column should be indexed by default, other columns can be indexed
through this object.

📌 Milestone 1 purpose:
We annotate where shared state is READ or WRITTEN so we can later
protect it with locks (record-level via 2PL and structure-level mutex).
"""

from collections import defaultdict

class Index:

    def __init__(self, table):
        self.table = table

        # 🔴 SHARED MUTABLE STRUCTURE
        # List of index structures (one per column).
        # Each element may be None or a defaultdict(set).
        # Mutated by: create_index(), drop_index()
        # Read by: locate(), locate_range(), update(), delete(), insert()
        self.indices = [None] * table.num_columns

    """
    Returns the location of all records with the given value
    on column "column".
    READ-ONLY from index structure, but not thread-safe yet.
    """
    def locate(self, column, value):
        # 🔵 READ: shared structure "indices"
        idx = self.indices[column]
        if idx is None:
            return None

        # 🔵 READ: idx[value] (set of RIDs)
        # Potential race if another thread mutates set during iteration
        return list(idx.get(value, set()))

    """
    Returns the RIDs of all records with values in column "column"
    between "begin" and "end".
    READ-ONLY but iterates over full index map.
    """
    def locate_range(self, begin, end, column):
        # 🔵 READ: shared structure "indices"
        idx = self.indices[column]
        if idx is None:
            return None

        result = set()

        # 🔵 READ: iterating over all index buckets
        # RISK: another thread modifying idx during iteration
        for val, rids in idx.items():
            if begin <= val <= end:
                result.update(rids)

        return list(result)

    """
    Create index on specific column.
    This is a structural write and must be mutex-protected later.
    """
    def create_index(self, column_number):
        if not (0 <= column_number < self.table.num_columns):
            raise ValueError("Invalid column_number")

        # 🔴 LOCAL temp structure (not yet shared)
        new_idx = defaultdict(set)

        # 🔵 READ: table.key_to_rid (shared in Table)
        # 🔵 READ: table.page_directory (shared)
        for base_rid in self.table.key_to_rid.values():
            value = self._latest_value_for_rid_column(base_rid, column_number)
            new_idx[value].add(base_rid)

        # 🔴 WRITE: shared structure "indices"
        self.indices[column_number] = new_idx

    """
    Drop index on specific column.
    Structural write to shared structure.
    """
    def drop_index(self, column_number):
        if not (0 <= column_number < self.table.num_columns):
            raise ValueError("Invalid column_number")

        # 🔴 WRITE: shared structure "indices"
        self.indices[column_number] = None

    """
    INTERNAL helper: add a RID to index bucket.
    Called during insert/update.
    """
    def _add(self, column, value, rid):
        # 🔵 READ: indices list
        idx = self.indices[column]
        if idx is None:
            return
        
        # 🔴 WRITE: modifies internal bucket set
        idx.setdefault(value, set()).add(rid)

    """
    INTERNAL helper: remove a RID from index bucket.
    Called during update/delete.
    """
    def _remove(self, column, value, rid):
        # 🔵 READ: indices list
        idx = self.indices[column]
        if idx is None:
            return

        # 🔵 READ/🔴 WRITE: shared bucket set
        bucket = idx.get(value)
        if bucket is not None:
            bucket.discard(rid)  # 🔴 modifies set
            if not bucket:
                idx.pop(value, None)  # 🔴 modifies dict

    """
    Walk the tail chain to resolve latest value for a column.
    Used during index building.
    """
    def _latest_value_for_rid_column(self, base_rid, col_idx):
        # 🔵 READ: shared structure table.page_directory
        base = self.table.page_directory[base_rid]
        value = base[4 + col_idx]

        # 🔵 READ: indirection pointer
        tail_rid = base[0]

        while tail_rid != 0:
            # 🔵 READ: shared structure table.page_directory
            tail = self.table.page_directory[tail_rid]
            schema = tail[3]

            # 🔵 READ: tail data
            if len(schema) > col_idx and schema[col_idx] == '1':
                value = tail[4 + col_idx]
                break

            tail_rid = tail[0]

        return value
