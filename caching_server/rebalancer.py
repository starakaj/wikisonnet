import dbconnect
import testprocessor
from task import Task, prepareCachedResource, testCacheDatabase
from multiprocessing import Queue, Process

def targetCountForResource(resource_key):
    dbconn = dbconnect.MySQLDatabaseConnection.connectionWithConfiguration('local-cachetest')
    cursor = dbconn.connection.cursor()
    query = """SELECT count FROM target_cache_sizes WHERE resource_key=%s"""
    values = (resource_key, )
    cursor.execute(query, values)
    res = cursor.fetchall()
    dbconn.close()
    if len(res) > 0:
        return res[0][0]
    return 0

def currentCountForResource(resource_key):
    dbconn = dbconnect.MySQLDatabaseConnection.connectionWithConfiguration('local-cachetest')
    cursor = dbconn.connection.cursor()
    query = """SELECT count(id) FROM cached_resources WHERE resource_key=%s AND used=0"""
    values = (resource_key, )
    cursor.execute(query, values)
    res = cursor.fetchall()
    dbconn.close()
    if len(res) > 0:
        return res[0][0]
    return 0

def processTask(queue):
    while True:
        t = queue.get()
        print "Dequeued process task with resource key: {}".format(t.resource_key)
        prepareCachedResource(testprocessor.process, t.resource_key, testCacheDatabase())

class TaskMaster:
    def __init__(self, process_count=4):
        self.process_count = process_count
        self.queue = Queue()
        self.processes = []
        for _ in range(process_count):
            p = Process(target=processTask, args=(self.queue,))
            self.processes.append(p)
            p.start()

    def notifyResourceConsumed(self, resource_key):
        available = currentCountForResource(resource_key)
        goal = targetCountForResource(resource_key)
        target = goal - available
        if target > 0:
            t = Task(resource_key)
            self.queue.put(t)

    def close(self):
        self.queue.close()
        for p in self.processes:
            p.terminate()
