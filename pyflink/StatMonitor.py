import csv
import time
from enum import Enum


class StatType(Enum):
    SUM = "SUM"
    ABSOLUTE = "ABSOLUTE"


class ReportingType(Enum):
    CUMULATIVE = "CUMULATIVE"
    RESET = "RESET"


class StatMonitor:
    
    _singleton_instance = None

    @classmethod
    def get_singleton(cls):
        return cls._singleton_instance

    @classmethod
    def set_singleton(cls, monitor):
        cls._singleton_instance = monitor


    def __init__(self, csv_path, reset_value, stat_type, reporting_type, default_value=-1):
        self.csv_path = csv_path
        self.reset_value = reset_value
        self.stat_type = StatType(stat_type)
        self.reporting_type = ReportingType(reporting_type)
        self.default_value = default_value

        self.v = reset_value
        self.ts = int(time.time())

        self.file = None  # We won't open the file here, only when needed

    def _open_file(self):
        if self.file is None:
            self.file = open(self.csv_path, mode='w', newline='')
            self.writer = csv.writer(self.file)
            # Only write headers the first time
            self.writer.writerow(["timestamp", "value"])
            self.file.flush()

    def log(self, all_timestamps=False):

        now = int(time.time())

        reset = False
        # Write missing timestamps
        while self.ts < now-1:
            self.writer.writerow([self.ts, self.default_value])
            self.ts += 1
            reset = True
        if self.ts == now-1 or (self.ts == now and all_timestamps):
            self.writer.writerow([self.ts, self.v])
            self.ts += 1
            reset = True

        # Flush and reset if required
        if reset:
            self.file.flush()

            if self.reporting_type == ReportingType.RESET:
                self.v = self.reset_value

    def report(self, value):

        # Open file if not already open
        self._open_file()

        self.log()

        # Update stat value
        if self.stat_type == StatType.SUM:
            self.v += value
        elif self.stat_type == StatType.ABSOLUTE:
            self.v = value

    def close(self):

        if self.file:
            self.log(all_timestamps=True)
            self.file.flush()
            self.file.close()
            self.file = None
            
