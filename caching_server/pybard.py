import hashlib
import dbconnect
import testprocessor
import task

path = "/Users/samtarakajian/Documents/wikisonnet/caching_server/data/"

def page_exists(wiki_page_name):
    return True

def page_suggestions(wiki_page_name):
    return ["Fuck all"]

def get_cached_resource(wiki_page_name, task_master):
    resource_key = int(hashlib.md5(wiki_page_name).hexdigest(), 16) % 100 + 1
    dbconn = dbconnect.MySQLDatabaseConnection.connectionWithConfiguration('local-cachetest')
    query = """SELECT id, path FROM cached_resources WHERE resource_key=%s AND used=0 LIMIT 1 FOR UPDATE;"""
    values = (resource_key, )
    cursor = dbconn.connection.cursor()
    cursor.execute("BEGIN;")
    cursor.execute(query, values)
    res = cursor.fetchall()

    has_cached_resource = len(res) > 0
    retval = None
    if has_cached_resource:
        query = """UPDATE cached_resources SET used=1 WHERE id=%s"""
        values = (res[0][0], )
        cursor.execute(query, values)
        print cursor.statement
        cursor.execute("COMMIT;")
        with open(path + str(res[0][1]), 'r') as f:
            retval = f.read()
    else:
        print "Cache miss: creating resource synchronously"
        retval = testprocessor.process(resource_key)
    dbconn.close()

    task_master.notifyResourceConsumed(resource_key)

    return retval

def poem_page(wiki_page_name, task_master):
    ## Check if the page exists
    if not page_exists(wiki_page_name):
        return page_suggestions(wiki_page_name)

    return get_cached_resource(wiki_page_name, task_master)
