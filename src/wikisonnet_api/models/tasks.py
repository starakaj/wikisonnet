import mysql.connector
import db.dbconnect as dbconnect

def enqueuePoemTaskForPageID(dbconfig, pageID, poemID, task_condition, userdata):
    dbconn = dbconnect.MySQLDatabaseConnection(dbconfig['database'], dbconfig['user'], dbconfig['host'], dbconfig['password'])
    cur = dbconn.connection.cursor()
    query = (
        """INSERT INTO poem_tasks (source, session, twitter_handle, page_id, poem_id)"""
        """ VALUES (%s, %s, %s, %s, %s)"""
    )
    values = (userdata.get("source"), userdata.get("session"), userdata.get("twitter"), pageID, poemID)
    cur.execute(query, values)
    cur.execute("""COMMIT;""")
    dbconn.close()

    task_condition.acquire()
    task_condition.notify()
    task_condition.release()

def getIncompleteTasks(dbconfig, offset=0, limit=0):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT * FROM poem_tasks WHERE complete=0 ORDER BY id LIMIT %s,%s;"""
    if limit is 0:
        limit = 1000
    values = (offset, limit)
    cursor.execute(query, values)
    res = cursor.fetchall()
    conn.close()
    return res

def markTaskCompleted(dbconfig, task):
    dbconn = dbconnect.MySQLDatabaseConnection(dbconfig['database'], dbconfig['user'], dbconfig['host'], dbconfig['password'])
    cur = dbconn.connection.cursor(dictionary=True)
    query = """UPDATE poem_tasks SET complete=1, completed_at=CURRENT_TIMESTAMP WHERE id=%s;"""
    values = (task["id"], )
    cur.execute(query, values)
    cur.execute("""COMMIT;""")
    dbconn.close()
