from lstore.table import Table, Record
from lstore.index import Index
import threading

class Transaction:
    transaction_counter = 0
    
    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.id = Transaction.transaction_counter
        Transaction.transaction_counter += 1
        self.locks = []
        self.undo_log = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args, table))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args, table in self.queries:
            primary_key = None
            if query.__name__ == 'insert':
                primary_key = args[table.key]
                if not table.lock_manager.acquire_exclusive(primary_key, self.id):
                    return self.abort()
            elif query.__name__ == 'update' or query.__name__ == 'delete':
                primary_key = args[0]
                if not table.lock_manager.acquire_exclusive(primary_key, self.id):
                    return self.abort()
            elif query.__name__ == 'select' or query.__name__ == 'select_version':
                primary_key = args[0]
                if not table.lock_manager.acquire_shared(primary_key, self.id):
                    return self.abort()
            
            if primary_key:
                self.locks.append((table, primary_key))

            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        # Release all locks
        for table, lock_key in self.locks:
            table.lock_manager.release(lock_key, self.id)
        self.locks.clear()
        return False

    
    def commit(self):
        # Release all locks
        for table, lock_key in self.locks:
            table.lock_manager.release(lock_key, self.id)
        self.locks.clear()
        return True

