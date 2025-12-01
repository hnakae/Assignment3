import threading


class LockManager:
    """
    Simple per-record lock manager implementing strict 2PL with a no-wait policy.
    Shared (S) and Exclusive (X) locks are tracked per RID. If a lock cannot be
    granted immediately, the caller should abort.
    """

    def __init__(self):
        # rid -> {"exclusive": txn_id or None, "shared": set(txn_ids)}
        self.locks = {}
        self.lock = threading.Lock()

    def acquire_shared(self, rid, txn_id):
        """
        Grant S lock if no conflicting X lock. X held by same txn is allowed.
        """
        with self.lock:
            state = self.locks.get(rid)
            if state is None:
                self.locks[rid] = {"exclusive": None, "shared": {txn_id}}
                return True

            # conflict if X held by other txn
            if state["exclusive"] is not None and state["exclusive"] != txn_id:
                return False

            state["shared"].add(txn_id)
            return True

    def acquire_exclusive(self, rid, txn_id):
        """
        Grant/upgrade to X lock. Upgrade succeeds only if this txn is the sole
        shared holder.
        """
        with self.lock:
            state = self.locks.get(rid)
            if state is None:
                self.locks[rid] = {"exclusive": txn_id, "shared": set()}
                return True

            # already exclusively owned
            if state["exclusive"] == txn_id:
                return True

            # conflict if X owned by another txn
            if state["exclusive"] is not None and state["exclusive"] != txn_id:
                return False

            # upgrade path: allow only if this txn is sole sharer
            if state["shared"] == {txn_id}:
                state["shared"].clear()
                state["exclusive"] = txn_id
                return True

            # other sharers exist -> conflict
            if state["shared"]:
                return False

            # no sharers, no owner
            state["exclusive"] = txn_id
            return True

    def release(self, rid, txn_id):
        """
        Release any lock held by txn on rid.
        """
        with self.lock:
            state = self.locks.get(rid)
            if state is None:
                return False

            changed = False
            if state["exclusive"] == txn_id:
                state["exclusive"] = None
                changed = True
            if txn_id in state["shared"]:
                state["shared"].discard(txn_id)
                changed = True

            if state["exclusive"] is None and not state["shared"]:
                self.locks.pop(rid, None)

            return changed

    def release_all(self, txn_id):
        """
        Drop all locks held by txn_id across all RIDs.
        """
        with self.lock:
            to_delete = []
            for rid, state in list(self.locks.items()):
                if state["exclusive"] == txn_id:
                    state["exclusive"] = None
                if txn_id in state["shared"]:
                    state["shared"].discard(txn_id)
                if state["exclusive"] is None and not state["shared"]:
                    to_delete.append(rid)

            for rid in to_delete:
                self.locks.pop(rid, None)
