

import os
from lstore.table import Table
from lstore.bufferpool import BufferPool
from lstore import config
from lstore.storage import (
    ensure_data_dir,
    ensure_table_dir,
    load_metadata,
    save_metadata,
    DATA_DIR,
)


class Database:

    def __init__(self):
        # name → Table object
        self.tables = {}

        # where DB lives on disk (set by open())
        self.path = None
        # buffer pool instance
        self.bufferpool = None


    
    def open(self, path, pool_pages: int = None):
        """
        Open a database stored in the given path.
        For Assignment 2, 'path' is typically './CS451'.
        All tables live under path/data/.
        """
        self.path = path
        # Initialize buffer pool
        if pool_pages is None:
            pool_pages = getattr(config, "DEFAULT_BUFFERPOOL_PAGES", 128)
        self.bufferpool = BufferPool(pool_pages)

        # Ensure root folder exists
        if not os.path.exists(path):
            os.makedirs(path)

        # Ensure "data/" directory exists inside path
        os.chdir(path)
        ensure_data_dir()

        # Scan existing tables by looking for folders in data/
        data_root = DATA_DIR
        if not os.path.exists(data_root):
            return

        for table_name in os.listdir(data_root):
            table_dir = os.path.join(data_root, table_name)
            if not os.path.isdir(table_dir):
                continue

        
            meta = load_metadata(table_name)
            if meta is None:
                continue

            table = Table.load_from_disk(meta)
            table.bufferpool = self.bufferpool
            self.tables[table_name] = table


    
    def close(self):
        """
        Flush all table metadata and page data to disk.
        (Page data handled by bufferpool; metadata by Table.flush_to_disk())
        """
        for table in self.tables.values():
            table.flush_to_disk()
        # Flush all dirty pages
        if self.bufferpool is not None:
            try:
                self.bufferpool.flush_all()
            except Exception:
                pass
        if self.path:
            try:
                os.chdir("..")
            except:
                pass


    
    def create_table(self, name, num_columns, key_index):
        """
        Create a table and register it with the DB.
        """
        if name in self.tables:
            return self.tables[name]

        ensure_table_dir(name)

        table = Table(name, num_columns, key_index, bufferpool=self.bufferpool)
        self.tables[name] = table

        
        table.flush_to_disk()

        return table


    def get_table(self, name):
        """
        Retrieve an existing table by name.
        """
        if name not in self.tables:
            raise Exception(f"Table {name} does not exist")
        return self.tables[name]
