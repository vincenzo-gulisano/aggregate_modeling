import csv
import time
from threading import Lock  # Import threading.Lock


class ThreadIdLogger:

    _singleton_instance = None

    @classmethod
    def get_singleton(cls):
        return cls._singleton_instance

    @classmethod
    def set_singleton(cls, singleton):
        cls._singleton_instance = singleton

    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.file = None  # We won't open the file here, only when needed
        self.lock = Lock()  # Add a threading lock

    def _open_file(self):
        if self.file is None:
            self.file = open(self.csv_path, mode='w', newline='')
            self.writer = csv.writer(self.file)
            # Only write headers the first time
            self.writer.writerow(["id", "name"])
            self.file.flush()

    def report(self, id, name):
        with self.lock:  # Acquire the lock before entering the critical section
            # Open file if not already open
            self._open_file()
            print(f"Writing to file: {self.csv_path} with id: {id} and name: {name}")
            self.writer.writerow([id, name])
            self.file.flush()

    def close(self):
        with self.lock:  # Ensure thread-safe access to the file
            if self.file:
                self.file.flush()
                self.file.close()
                self.file = None
