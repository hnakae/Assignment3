import threading

class LockManager:
    def __init__(self):
        self.locks = {}
        self.lock = threading.Lock()

    def acquire_shared(self, rid, transaction_id):
        with self.lock:
            if rid not in self.locks:
                self.locks[rid] = {'exclusive': False, 'shared': {transaction_id}}
                return True
            
            if self.locks[rid]['exclusive']:
                return False
            
            self.locks[rid]['shared'].add(transaction_id)
            return True

    def acquire_exclusive(self, rid, transaction_id):
        with self.lock:
            if rid not in self.locks:
                self.locks[rid] = {'exclusive': True, 'shared': set()}
                return True
            
            if self.locks[rid]['exclusive'] or len(self.locks[rid]['shared']) > 0:
                return False
            
            self.locks[rid]['exclusive'] = True
            return True

    def release(self, rid, transaction_id):
        with self.lock:
            if rid in self.locks:
                if self.locks[rid]['exclusive'] and transaction_id in self.locks[rid]['shared']:
                    self.locks[rid]['exclusive'] = False
                elif transaction_id in self.locks[rid]['shared']:
                    self.locks[rid]['shared'].remove(transaction_id)
                
                if not self.locks[rid]['exclusive'] and len(self.locks[rid]['shared']) == 0:
                    del self.locks[rid]
                return True
            return False
