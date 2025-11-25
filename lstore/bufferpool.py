# Simple buffer pool for Page objects with pin/unpin and dirty tracking.
# Policy: Toss Immediate eviction (first unpinned frame).

import time
from collections import OrderedDict
from lstore.page import Page
from lstore.storage import read_page_bytes, write_page_bytes, page_path


class PageFrame:
    def __init__(self, key, page: Page):
        self.key = key  # (table_name, is_base, col_id, page_index)

        # 🔵 SHARED: actual page data (mutable Page object)
        self.page = page

        # 🔵 SHARED: pin count (read + write during concurrency)
        self.pin_count = 0

        # 🔵 SHARED: dirty flag
        self.dirty = False

        # 🔵 SHARED: recency / eviction metadata
        self.last_used_ts = time.time()

    def pin(self):
        # 🔴 WRITE: pin_count and timestamp
        self.pin_count += 1
        self.last_used_ts = time.time()

    def unpin(self):
        # 🔴 WRITE: pin_count and timestamp
        if self.pin_count > 0:
            self.pin_count -= 1
        self.last_used_ts = time.time()


class BufferPool:
    def __init__(self, max_pages: int):
        self.max_pages = max_pages

        # 🔴 SHARED MUTABLE STRUCTURE
        # Maps page key -> PageFrame
        # Mutated by: get_page(), _evict_one(), flush_all()
        self._frames = {}

        # 🔴 SHARED MUTABLE STRUCTURE
        # Tracks recency info for LRU-style eviction
        # Mutated by: _touch_lru(), _evict_one(), get_page(), unpin()
        self._lru = OrderedDict()

        # 🚨 MISSING: structure lock (to be added in Milestone 2/3)
        # something like: self.pool_lock = threading.Lock()

    def _touch_lru(self, key):
        # 🔴 WRITE: modifies shared LRU state
        if key in self._lru:
            self._lru.move_to_end(key)
        else:
            self._lru[key] = True

    def get_page(self, table_name: str, is_base: bool, col_id: int, page_index: int,
                 create_if_missing: bool = False):
        """
        Fetch a page into the buffer pool and pin it.

        🔵 READ: self._frames
        🔴 WRITE: self._frames, self._lru
        ⚠️ RACE: two threads could load same page concurrently
        """

        key = (table_name, bool(is_base), int(col_id), int(page_index))

        # 🔵 READ shared map
        frame = self._frames.get(key)
        if frame is not None:
            frame.pin()          # 🔴 write on PageFrame
            self._touch_lru(key) # 🔴 write on LRU
            return frame

        # 🔴 READ/WRITE: checking capacity + evict
        if len(self._frames) >= self.max_pages:
            self._evict_one()

        # 🔵 READ: disk storage
        path = page_path(table_name, is_base, col_id, page_index)
        byte_data = read_page_bytes(path)

        if byte_data is None:
            if not create_if_missing:
                return None
            page = Page()
        else:
            page = Page.from_bytes(byte_data)

        frame = PageFrame(key, page)

        # 🔴 WRITE: frame state
        frame.pin()

        # 🔴 WRITE: shared structures
        self._frames[key] = frame
        self._touch_lru(key)

        return frame

    def mark_dirty(self, frame: PageFrame):
        # 🔴 WRITE: shared frame metadata
        frame.dirty = True
        frame.last_used_ts = time.time()

    def unpin(self, frame: PageFrame):
        # 🔴 WRITE: pin count and LRU state
        frame.unpin()
        self._touch_lru(frame.key)

    def _evict_one(self):
        """
        Evict one unpinned frame.

        🔵 READ: _frames
        🔴 WRITE: _frames, _lru
        🔴 WRITE: filesystem via flush
        """

        for key, frame in list(self._frames.items()):
            # 🔵 READ: frame.pin_count
            if frame.pin_count == 0:
                self._flush_if_dirty(frame)  # may write to disk
                del self._frames[key]        # 🔴 modify shared dict
                if key in self._lru:
                    del self._lru[key]       # 🔴 modify recency structure
                return

        # No eviction candidate found
        raise RuntimeError("BufferPool is full and all pages are pinned; cannot evict")

    def _flush_if_dirty(self, frame: PageFrame):
        # 🔵 READ: dirty flag
        if not frame.dirty:
            return

        # 🔵 READ: frame key and page
        table_name, is_base, col_id, page_index = frame.key

        # 🔴 WRITE: filesystem
        path = page_path(table_name, is_base, col_id, page_index)
        write_page_bytes(path, frame.page.to_bytes())

        # 🔴 WRITE: frame metadata
        frame.dirty = False

    def flush_all(self):
        """
        Flush all frames to disk.

        🔵 READ: _frames
        🔴 WRITE: multiple disk writes
        """
        for frame in list(self._frames.values()):
            self._flush_if_dirty(frame)