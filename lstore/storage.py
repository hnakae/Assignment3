

import os
import json

# Root directory where all table folders live.
DATA_DIR = "data"


# ---------------------------------------------------------------
#  Directory / Path Helpers
# ---------------------------------------------------------------

def ensure_data_dir():
    """
    Ensure the ./data directory exists.
    """
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)


def ensure_table_dir(table_name):
    """
    Ensures the directory structure:
        data/<table_name>/
            base/
            tail/
            metadata.json  (created if missing)

    Returns: full table directory path.
    """
    ensure_data_dir()

    table_dir = os.path.join(DATA_DIR, table_name)
    base_dir = os.path.join(table_dir, "base")
    tail_dir = os.path.join(table_dir, "tail")

    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(tail_dir, exist_ok=True)

    # Create metadata if missing
    meta_path = os.path.join(table_dir, "metadata.json")
    if not os.path.exists(meta_path):
        with open(meta_path, "w") as f:
            json.dump({}, f, indent=4)

    return table_dir


def metadata_path(table_name):
    """
    Returns the metadata.json path for a given table.
    """
    return os.path.join(DATA_DIR, table_name, "metadata.json")


# ---------------------------------------------------------------
#  Metadata Load / Save
# ---------------------------------------------------------------

def load_metadata(table_name):
    """
    Load metadata.json for a table.
    Returns None if missing.
    """
    path = metadata_path(table_name)
    if not os.path.exists(path):
        return None

    with open(path, "r") as f:
        return json.load(f)


def save_metadata(table_name, meta_dict):
    """
    Save metadata.json for a table.
    Ensures the table directory exists.
    """
    ensure_table_dir(table_name)
    path = metadata_path(table_name)

    with open(path, "w") as f:
        json.dump(meta_dict, f, indent=4)


# ---------------------------------------------------------------
#  Page File Naming + Paths
# ---------------------------------------------------------------

def page_filename(is_base, col_id, page_index):
    """
    Generate a filename like:
        base_col0_pg4.bin
        tail_col1_pg32.bin
    """
    prefix = "base" if is_base else "tail"
    return f"{prefix}_col{col_id}_pg{page_index}.bin"


def page_path(table_name, is_base, col_id, page_index):
    """
    Returns the full on-disk path for a page.
    """
    table_dir = os.path.join(DATA_DIR, table_name)
    subdir = "base" if is_base else "tail"
    filename = page_filename(is_base, col_id, page_index)
    return os.path.join(table_dir, subdir, filename)


# ---------------------------------------------------------------
#  Raw Page I/O
# ---------------------------------------------------------------

def write_page_bytes(path, byte_data):
    """
    Write raw page bytes to disk (overwrite).
    """
    # Ensure folder exists
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "wb") as f:
        f.write(byte_data)


def read_page_bytes(path):
    """
    Read raw page bytes from disk.
    Returns None if file does not exist.
    """
    if not os.path.exists(path):
        return None

    with open(path, "rb") as f:
        return f.read()
