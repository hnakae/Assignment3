from lstore.index import Index
from lstore.page import MAX_RECORDS
from lstore.storage import (
    ensure_table_dir,
    save_metadata,
    load_metadata,
)

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        return str(self.columns)

    def __repr__(self):
        return str(self.columns)


class Table:
    """
    Table(name, num_columns, key_index)
    """

    def __init__(self, name, num_columns, key_index, bufferpool=None):
        self.name = name
        self.num_columns = num_columns
        self.key = key_index
        self.bufferpool = bufferpool

        # ensure table directory exists
        ensure_table_dir(self.name)

        # =============================
        # HOT SHARED METADATA STRUCTURES
        # =============================

        # rid -> record metadata
        # HOT: read/write in insert, update, delete, select
        # MUTEX: protect dict structure
        # RECORD LOCK: protect individual RID entries
        self.page_directory = {}

        # primaryKey -> rid
        # HOT: modified during insert/delete
        # MUTEX: protect dict
        self.key_to_rid = {}

        # next RID to assign
        # HOT: incremented on every insert
        # MUTEX: must be atomic
        self.next_rid = 1

        # =============================
        # HOT PAGE ALLOCATION METADATA
        # =============================

        # Page counts per column
        # HOT: modified when new pages are created
        # MUTEX
        self.base_page_counts = [0] * num_columns
        self.tail_page_counts = [0] * num_columns

        # Next free slot per column
        # HOT: modified on every append
        # MUTEX
        self.base_page_next_slot = [0] * num_columns
        self.tail_page_next_slot = [0] * num_columns

        # rid -> list of [page_index, slot] per column
        # HOT: modified during insert/update
        # MUTEX
        self.base_positions = {}     # base_rid -> list of positions
        self.tail_positions = {}     # tail_rid -> list of positions

        # Index structure (has its own concurrency issues)
        self.index = Index(self)

    """
    =============================
    METADATA EXPORT
    =============================
    """
    def to_metadata(self):
        """
        Convert table state into a JSON-serializable dictionary.

        PERSISTENCE: Should be called under table-level metadata lock
        to ensure snapshot consistency.
        """
        return {
            "name": self.name,
            "num_columns": self.num_columns,
            "key": self.key,
            "next_rid": self.next_rid,
            "key_to_rid": self.key_to_rid,
            "page_directory": self.page_directory,
            "base_page_counts": self.base_page_counts,
            "tail_page_counts": self.tail_page_counts,
            "base_page_next_slot": self.base_page_next_slot,
            "tail_page_next_slot": self.tail_page_next_slot,
            "base_positions": self.base_positions,
            "tail_positions": self.tail_positions,
        }

    """
    =============================
    METADATA IMPORT
    =============================
    """
    def from_metadata(self, meta):
        """
        Load table state from metadata dictionary.

        Assumes single-threaded execution during database startup.
        """
        self.next_rid = meta["next_rid"]

        # convert keys to int
        # Not thread-safe: should only run at initialization time
        self.key_to_rid = {int(k): v for k, v in meta["key_to_rid"].items()}
        self.page_directory = {int(k): v for k, v in meta["page_directory"].items()}

        self.base_page_counts = meta["base_page_counts"]
        self.tail_page_counts = meta["tail_page_counts"]
        self.base_page_next_slot = meta.get("base_page_next_slot", [0] * self.num_columns)
        self.tail_page_next_slot = meta.get("tail_page_next_slot", [0] * self.num_columns)
        self.base_positions = {int(k): v for k, v in meta.get("base_positions", {}).items()}
        self.tail_positions = {int(k): v for k, v in meta.get("tail_positions", {}).items()}

    """
    =============================
    CONSTRUCT TABLE FROM METADATA
    =============================
    """
    @classmethod
    def load_from_disk(cls, meta):
        """
        Rebuild a Table object from metadata.json.
        Called by Database.open().
        """
        table = cls(meta["name"], meta["num_columns"], meta["key"])
        table.from_metadata(meta)
        return table

    """
    =============================
    PERSIST TABLE STATE
    =============================
    """
    def flush_to_disk(self):
        """
        Only persist metadata.json.
        (page flush handled when BufferPool is added.)

        PERSISTENCE: requires table metadata lock.
        """
        meta = self.to_metadata()
        save_metadata(self.name, meta)

    """
    =============================
    BUFFERPOOL APPEND HELPERS
    =============================
    """

    def _append_to_column(self, is_base: bool, col_id: int, value: int):
        """
        Append a single 64-bit integer value to the specified column's page stream.

        HOT: updates shared page metadata and bufferpool
        MUTEX: protect page counters and slot counters
        """

        if self.bufferpool is None:
            return None
        if not (0 <= col_id < self.num_columns):
            return None

        counts = self.base_page_counts if is_base else self.tail_page_counts
        slots = self.base_page_next_slot if is_base else self.tail_page_next_slot

        current_page_index = counts[col_id] - 1 if counts[col_id] > 0 else -1

        # HOT: page creation path
        # MUTEX required: must serialize page allocation
        if current_page_index == -1 or slots[col_id] >= MAX_RECORDS:
            current_page_index += 1
            counts[col_id] = current_page_index + 1
            slots[col_id] = 0

            frame = getattr(self, "bufferpool", None).get_page(
                self.name, is_base, col_id, current_page_index, create_if_missing=True
            )
            if frame is not None:
                # ensure fresh page's cursor aligned
                frame.page.num_records = 0
                self.bufferpool.mark_dirty(frame)
                self.bufferpool.unpin(frame)

        slot_index = slots[col_id]

        frame = self.bufferpool.get_page(
            self.name, is_base, col_id, current_page_index, create_if_missing=True
        )
        if frame is None:
            return None

        # HOT: updating page and slot count
        frame.page.num_records = slot_index
        frame.page.write(int(value))
        slots[col_id] += 1

        self.bufferpool.mark_dirty(frame)
        self.bufferpool.unpin(frame)

        return [current_page_index, slot_index]

    def _append_base_record(self, user_columns):
        """
        Append all user columns of a new base record.

        HOT: called during insert
        MUTEX: required indirectly via _append_to_column
        """

        positions = []
        for c, val in enumerate(user_columns):
            pos = self._append_to_column(is_base=True, col_id=c, value=val)
            positions.append(pos)
        return positions

    def _append_tail_updates(self, updated_columns):
        """
        Append updated user columns of a tail record.

        HOT: called during update
        MUTEX: required indirectly via _append_to_column
        """

        positions = [None] * self.num_columns
        for c, val in enumerate(updated_columns):
            if val is not None:
                positions[c] = self._append_to_column(is_base=False, col_id=c, value=val)
        return positions

    """
    =============================
    READ HELPERS
    =============================
    """

    def _read_value_at(self, is_base: bool, col_id: int, page_index: int, slot_index: int):
        """
        Read a 64-bit value from the specified page position.

        HOT: called during selects
        Concurrency depends on bufferpool frame latching.
        """

        if self.bufferpool is None:
            return None
        frame = self.bufferpool.get_page(
            self.name, is_base, col_id, page_index, create_if_missing=False
        )
        if frame is None:
            return None
        try:
            return frame.page.read(slot_index)
        finally:
            self.bufferpool.unpin(frame)

    """
    =============================
    MERGE STUB
    =============================
    """

    def __merge(self):
        """
        Merge implementation is done in later phases / Assignment 3.

        Future HOT path:
        - Will read and modify base/tail pages
        - Will require RECORD LOCKS and MUTEX protection on metadata
        """
        print("merge is happening")
        pass
