from lstore.table import Table, Record
from lstore.index import Index
from time import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        # pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            base_rid = self.table.key_to_rid.get(primary_key)
            if base_rid is None:
                return False
            # remove from any built indexes
            full = self._build_record_from_data(base_rid, [1] * self.table.num_columns)
            for c in range(self.table.num_columns):
                if self.table.index.indices[c] is not None:
                    self.table.index._remove(c, full.columns[c], base_rid)

            base_record = self.table.page_directory[base_rid]
            base_record[1] = 0  # tombstone
            del self.table.key_to_rid[primary_key]
            return True
        except Exception:
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        try:
            if len(columns) != self.table.num_columns:
                return False
            primary_key = columns[self.table.key]
            if primary_key in self.table.key_to_rid:
                return False

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

            self.table.page_directory[rid] = record_data
            self.table.key_to_rid[primary_key] = rid

            # write-through to base column pages via bufferpool (Phase 3 minimal)
            if getattr(self.table, "_append_base_record", None):
                positions = self.table._append_base_record(columns)
                if positions is not None:
                    self.table.base_positions[rid] = positions

            # update built secondary indexes
            for c in range(self.table.num_columns):
                if self.table.index.indices[c] is not None:
                    self.table.index._add(c, columns[c], rid)

            return True
        except Exception:
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            # PK fast path
            if search_key_index == self.table.key:
                rid = self.table.key_to_rid.get(search_key)
                if rid is None:
                    return []
                return [self._build_record_from_data(rid, projected_columns_index)]

            # use secondary index if available
            if self.table.index.indices[search_key_index] is not None:
                rids = self.table.index.locate(search_key_index, search_key) or []
                return [self._build_record_from_data(rid, projected_columns_index) for rid in rids]

            # or linear scan over base records
            results = []
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

    def _build_record_from_data(self, base_rid, projected_columns_index):
        base = self.table.page_directory[base_rid]
        # base values via bufferpool if we have positions, else fallback to in-memory snapshot
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
        tail_records = []
        while tail_rid != 0:
            tail = self.table.page_directory[tail_rid]
            tail_records.append(tail)
            tail_rid = tail[0]

        
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
        return Record(base_rid, primary_key, projected)

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
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
            while tail_rid != 0 and current_version < version_to_find:
                tail = self.table.page_directory[tail_rid]
                tail_rid = tail[0]
                current_version += 1
            
            tail_records = []
            while tail_rid != 0:
                tail = self.table.page_directory[tail_rid]
                tail_records.append(tail)
                tail_rid = tail[0]

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
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            if len(columns) != self.table.num_columns:
                return False

            base_rid = self.table.key_to_rid.get(primary_key)
            if base_rid is None:
                return False

            # capture old values for index maintenance
            old_full = self._build_record_from_data(base_rid, [1] * self.table.num_columns)

            base = self.table.page_directory[base_rid]
            latest_tail_rid = base[0]

            schema_bits = ['1' if c is not None else '0' for c in columns]
            schema_encoding = "".join(schema_bits)
            if '1' not in schema_encoding:
                return True

            tail_rid = self.table.next_rid
            self.table.next_rid += 1

            tail_values = [c if c is not None else 0 for c in columns]
            tail_data = [latest_tail_rid, tail_rid, int(time()), schema_encoding, *tail_values]
            self.table.page_directory[tail_rid] = tail_data

            base[0] = tail_rid

            # write-through to tail column pages via bufferpool (Phase 3 minimal)
            if getattr(self.table, "_append_tail_updates", None):
                positions = self.table._append_tail_updates(columns)
                if positions is not None:
                    self.table.tail_positions[tail_rid] = positions

            # update indexes for changed columns
            for c, new_val in enumerate(columns):
                if new_val is not None and self.table.index.indices[c] is not None:
                    self.table.index._remove(c, old_full.columns[c], base_rid)
                    self.table.index._add(c, new_val, base_rid)

            return True
        except Exception:
            return False
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            total = 0
            found_any = False
            for key, rid in self.table.key_to_rid.items():
                if start_range <= key <= end_range:
                    found_any = True
                    rec = self._build_record_from_data(rid, [1] * self.table.num_columns)
                    total += rec.columns[aggregate_column_index]
            return total if found_any else False
        except Exception:
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            total = 0
            found_any = False

            for key, rid in self.table.key_to_rid.items():
                if start_range <= key <= end_range:
                    rec_list = self.select_version(key, self.table.key, [1] * self.table.num_columns, relative_version)
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
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
