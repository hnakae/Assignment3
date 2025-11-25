
# Simple buffer pool for Page objects with pin/unpin and dirty tracking. Toss Immediate.


import time
from collections import OrderedDict
from lstore.page import Page
from lstore.storage import read_page_bytes, write_page_bytes, page_path


class PageFrame:
    def __init__(self, key, page: Page):
        self.key = key  # (table_name, is_base, col_id, page_index)
        self.page = page
        self.pin_count = 0
        self.dirty = False
        self.last_used_ts = time.time()

    def pin(self):
        self.pin_count += 1
        self.last_used_ts = time.time()

    def unpin(self):
        if self.pin_count > 0:
            self.pin_count -= 1
        self.last_used_ts = time.time()


class BufferPool:
    def __init__(self, max_pages: int):
        self.max_pages = max_pages
        self._frames = {}
        self._lru = OrderedDict() # ordered keys by recency for LRU if needed

    def _touch_lru(self, key):
        # maintain recency
        if key in self._lru:
            self._lru.move_to_end(key)
        else:
            self._lru[key] = True

    def get_page(self, table_name: str, is_base: bool, col_id: int, page_index: int, create_if_missing: bool = False) -> PageFrame:
        """
        Fetch a page into the buffer pool and pin it. If not present, load from disk.
        If missing on disk and create_if_missing=True, allocate an empty page.
        """
        key = (table_name, bool(is_base), int(col_id), int(page_index))

        frame = self._frames.get(key)
        if frame is not None:
            frame.pin()
            self._touch_lru(key)
            return frame

        
        if len(self._frames) >= self.max_pages:
            self._evict_one()

        
        path = page_path(table_name, is_base, col_id, page_index)
        byte_data = read_page_bytes(path)
        if byte_data is None:
            if not create_if_missing:
                return None
            page = Page()
        else:
            page = Page.from_bytes(byte_data)

        frame = PageFrame(key, page)
        frame.pin()
        self._frames[key] = frame
        self._touch_lru(key)
        return frame

    def mark_dirty(self, frame: PageFrame):
        frame.dirty = True
        frame.last_used_ts = time.time()

    def unpin(self, frame: PageFrame):
        frame.unpin()
        self._touch_lru(frame.key)

    def _evict_one(self):
        """
        Evict one unpinned frame. Policy: Toss Immediate (first unpinned found).
        Fallback to LRU among unpinned if needed.
        """
        # toss immediate
        for key, frame in list(self._frames.items()):
            if frame.pin_count == 0:
                self._flush_if_dirty(frame)
                del self._frames[key]
                if key in self._lru:
                    del self._lru[key]
                return

        
        raise RuntimeError("BufferPool is full and all pages are pinned; cannot evict")

    def _flush_if_dirty(self, frame: PageFrame):
        if not frame.dirty:
            return
        table_name, is_base, col_id, page_index = frame.key
        path = page_path(table_name, is_base, col_id, page_index)
        write_page_bytes(path, frame.page.to_bytes())
        frame.dirty = False

    def flush_all(self):
        for frame in list(self._frames.values()):
            self._flush_if_dirty(frame)


