"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from collections import defaultdict

class Index:

    def __init__(self, table):
        self.table = table
        # One index dict per column (or None if not built)
        self.indices = [None] * table.num_columns

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        idx = self.indices[column]
        if idx is None:
            return None
        return list(idx.get(value, set()))

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        idx = self.indices[column]
        if idx is None:
            return None
        result = set()
        # linear scan over buckets 
        for val, rids in idx.items():
            if begin <= val <= end:
                result.update(rids)
        return list(result)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if not (0 <= column_number < self.table.num_columns):
            raise ValueError("Invalid column_number")

        new_idx = defaultdict(set)

        
        for base_rid in self.table.key_to_rid.values():
            value = self._latest_value_for_rid_column(base_rid, column_number)
            new_idx[value].add(base_rid)

        self.indices[column_number] = new_idx


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if not (0 <= column_number < self.table.num_columns):
            raise ValueError("Invalid column_number")
        self.indices[column_number] = None

    # helpers for index incrementals
    def _add(self, column, value, rid):
        idx = self.indices[column]
        if idx is None:
            return
        idx.setdefault(value, set()).add(rid)

    def _remove(self, column, value, rid):
        idx = self.indices[column]
        if idx is None:
            return
        bucket = idx.get(value)
        if bucket is not None:
            bucket.discard(rid)
            if not bucket:
                idx.pop(value, None)

    # resolve latest value of a column for a given base RID by walking tail chain
    def _latest_value_for_rid_column(self, base_rid, col_idx):
        base = self.table.page_directory[base_rid]
        value = base[4 + col_idx]
        tail_rid = base[0]
        while tail_rid != 0:
            tail = self.table.page_directory[tail_rid]
            schema = tail[3]
            if len(schema) > col_idx and schema[col_idx] == '1':
                value = tail[4 + col_idx]
                break
            tail_rid = tail[0]
        return value

