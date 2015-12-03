import mysql.connector
import time
import random
import util
import testprocessor

path = "/Users/samtarakajian/Documents/wikisonnet/caching-server/data/"

def testCacheDatabase():
    return {"user":"william",
            "password":"Sh4kespeare",
            "host":"localhost",
            "database":"cache_test"}

def uid():
    return "{}-{}".format(random.randint(1, 10000), int(time.time() * 10000.0))

def processAndSave(pinput, pfunc):
    fname = uid()
    with open(path + fname, 'w') as f:
        f.write(pfunc(pinput))
    return fname

def prepareCachedResource(pfunc, pinput, dbconfig):
    out_path = processAndSave(pinput, pfunc)
    dbconn = mysql.connector.connect(user=dbconfig["user"], password=dbconfig["password"], host=dbconfig["host"], database=dbconfig["database"])
    query = """INSERT INTO cached_resources (resource_key, path) VALUES(%s, %s)"""
    values = (pinput, out_path)
    dbconn.cursor().execute(query, values)
    dbconn.commit()
    dbconn.close()

def createTestTasks(number_of_keys, max_count_per_key, min_count_per_key, exponent):
    task_counts = util.allocate_pdist(number_of_keys, exponent)
    util.scramble(task_counts)
    task_counts = util.scaled_list(task_counts, min_count_per_key, max_count_per_key)
    return [(i+1, int(round(t))) for (i, t) in enumerate(task_counts)]

class TaskMaker:
    def __init__(self, dbconfig, pfunc, plist):
        self.dbconfig = dbconfig
        self.pfunc = pfunc
        self.plist = plist

    def generate(self):
        for (resource_key, cnt) in self.plist:
            for _ in range(cnt):
                prepareCachedResource(self.pfunc, resource_key, self.dbconfig)
