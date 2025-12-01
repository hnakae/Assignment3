from lstore.table import Table, Record
from lstore.index import Index
import threading


class TransactionWorker:

    """
    Creates a transaction worker object.
    """
    def __init__(self, transactions=None):
        self.stats = []
        self.transactions = list(transactions) if transactions else []
        self.result = 0
        self.thread = None
        self._running = False

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """
    def run(self):
        if self._running:
            return
        self._running = True
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()
    
    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread:
            self.thread.join()

    def __run(self):
        self.stats = []
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        self.result = len(list(filter(lambda x: x, self.stats)))
        self._running = False
