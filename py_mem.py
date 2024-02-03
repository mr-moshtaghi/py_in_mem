import threading
import time
import pickle
import os


class PyInMemStore:
    def __init__(self):
        self.data = {}
        self.lock = threading.RLock()
        self.in_transaction = False
        self.original_data = None

    def set(self, key, value):
        with self.lock:
            self.data[key] = {}
            self.data[key]["value"] = value

    def get(self, key):
        if key not in self.data:
            return None
        return self.data[key]["value"]

    def delete(self, key):
        with self.lock:
            if key not in self.data:
                raise KeyError(f"Key not found: {key}")
            del self.data[key]

    def expire(self, key, seconds):
        with self.lock:
            if seconds <= 0:
                raise ValueError(f"Invalid expiration time: {seconds}")
            if key not in self.data:
                raise KeyError(f"Key not found: {key}")
            self.data[key]["expires"] = time.time() + seconds

            threading.Timer(seconds, self._delete, args=[key]).start()

    def ttl(self, key):
        if key not in self.data:
            return -2
        if not self.data[key].get("expires"):
            return -1
        return int(self.data[key]["expires"] - time.time())

    def persist(self, filename):
        with self.lock:
            with open(filename, "wb") as f:
                pickle.dump(self.data, f)

    def load(self, filename):
        with self.lock:
            if not os.path.exists(filename):
                return
            with open(filename, "rb") as f:
                try:
                    self.data = pickle.load(f)
                except Exception as e:
                    print("Error loading data: {}".format(e))

    def begin(self):
        with self.lock:
            self.in_transaction = True
            self.original_data = self.data.copy()

    def commit(self):
        with self.lock:
            self.in_transaction = False

    def rollback(self):
        with self.lock:
            if self.in_transaction:
                self.in_transaction = False
                self.data = self.original_data

    def _delete(self, key):
        if key in self.data:
            del self.data[key]
