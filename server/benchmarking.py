import time

class Timer:
    def __init__(self):
        self.start_time = time.time()
        self.interval_starts = {}
        self.interval_ends = {}
        self.intervals = {}

    def begin(self, key):
        self.interval_starts[key] = time.time()

    def end(self, key):
        self.interval_ends[key] = time.time()
        if key in self.intervals:
            self.intervals[key] = self.intervals[key] + self.interval_ends[key] - self.interval_starts[key]
        else:
            self.intervals[key] = self.interval_ends[key] - self.interval_starts[key]

    def printTime(self):
        total_time = time.time() - self.start_time
        print("Total time: " + str(total_time))
        for key in self.intervals:
            interval_time = self.intervals[key]
            print("\t" + str(key) + ": " + str(interval_time) + " " + str(100.0 * interval_time / total_time) + "%%")

    def elapsed(self):
        return time.time() - self.start_time
