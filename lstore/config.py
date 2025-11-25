# ===============================================================
# lstore/config.py
# ---------------------------------------------------------------
# Centralized configuration for L-Store project.
# ===============================================================

# Page sizing
PAGE_SIZE_BYTES = 4096
INT_SIZE_BYTES = 8

# Buffer pool
DEFAULT_BUFFERPOOL_PAGES = 128
EVICTION_POLICY = "toss_immediate"  # or "lru"

# Merge/background
MERGE_INTERVAL_SECONDS = 0  # disabled by default


