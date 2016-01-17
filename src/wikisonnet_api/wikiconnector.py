import mysql.connector
from flask import jsonify
import wikibard.wikibard as wikibard
import db.dbconnect as dbconnect
from multiprocessing import Process
import time

def task_processor(dbconfig, condition):
    # Check if there are any poems that need to be written
    while (True):
        condition.acquire()
        dbconn = dbconnect.MySQLDatabaseConnection(dbconfig['database'], dbconfig['user'], dbconfig['host'], dbconfig['password'])
        cur = dbconn.connection.cursor(dictionary=True)
        query = """SELECT * FROM poem_tasks WHERE complete=0 ORDER BY id LIMIT 1;"""
        cur.execute(query)
        res  = cur.fetchall()
        if not res:
            condition.wait()
        else:
            writePoemForTaskRow(dbconfig, res[0])
        dbconn.close()
        condition.release()

def writePoemForTaskRow(dbconfig, task):
    page_id = task["page_id"]
    poem_id = task["poem_id"]
    wikibard.poemForPageID(page_id, 'elizabethan', dbconfig, multi=True, callback=stanzaWrite, user_info=(poem_id, dbconfig))
    dbconn = dbconnect.MySQLDatabaseConnection(dbconfig['database'], dbconfig['user'], dbconfig['host'], dbconfig['password'])
    cur = dbconn.connection.cursor(dictionary=True)
    query = """UPDATE poem_tasks SET complete=1, completed_at=CURRENT_TIMESTAMP WHERE id=%s;"""
    values = (task["id"], )
    cur.execute(query, values)
    cur.execute("""COMMIT;""")

def enqueuePoemTaskForPageID(dbconfig, pageID, poemID, task_condition, userdata):
    dbconn = dbconnect.MySQLDatabaseConnection(dbconfig['database'], dbconfig['user'], dbconfig['host'], dbconfig['password'])
    cur = dbconn.connection.cursor()
    query = (
        """INSERT INTO poem_tasks (source, session, twitter_handle, page_id, poem_id)"""
        """ VALUES (%s, %s, %s, %s, %s)"""
    )
    values = (userdata.get("source"), userdata.get("session"), userdata.get("twitter"), pageID, poemID)

    task_condition.acquire()
    cur.execute(query, values)
    cur.execute("""COMMIT;""")
    dbconn.close()
    task_condition.notify()
    task_condition.release()

def stanzaWrite(lines, user_info):
    print lines
    poem_id = user_info[0]
    dbconfig = user_info[1]
    line_ids = [line['id'] for line in lines if line is not None]

    ## Store the poem
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor()
    query = """UPDATE cached_poems SET"""
    for i, line in enumerate(lines):
        if line is not None:
            query = query + """ line_{}=%s,""".format(i)
    if query.endswith(","):
        query = query[:-1]
    query = query + """ WHERE id=%s"""
    values = tuple(line_ids + [poem_id])
    cursor.execute(query, values)
    cursor.execute("""COMMIT;""")

    query = (
            """UPDATE cached_poems SET """
            """complete = CASE """
	        """WHEN line_0 IS NOT NULL AND """
		"""line_1 IS NOT NULL AND """
		"""line_2 IS NOT NULL AND """
		"""line_3 IS NOT NULL AND """
		"""line_4 IS NOT NULL AND """
		"""line_5 IS NOT NULL AND """
		"""line_6 IS NOT NULL AND """
		"""line_7 IS NOT NULL AND """
		"""line_8 IS NOT NULL AND """
		"""line_9 IS NOT NULL AND """
		"""line_10 IS NOT NULL AND """
		"""line_11 IS NOT NULL AND """
		"""line_12 IS NOT NULL AND """
		"""line_13 IS NOT NULL """
	    """THEN 1 """
	    """ELSE 0 """
        """END WHERE id=%s;"""
        )
    values = (poem_id, )
    cursor.execute(query, values)
    cursor.execute("""COMMIT;""")
    conn.close()

def dbconfigForName(name='local'):
    return dbconnect.MySQLDatabaseConnection.dbconfigForName(name)

def getRandomPoemTitle(dbconfig):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor()
    query = """SELECT id FROM page_categories ORDER BY RAND() LIMIT 1;"""
    cursor.execute(query)
    res = cursor.fetchall()
    retval = None
    if res:
        query = """SELECT name FROM page_names WHERE page_id = %s;"""
        values = (res[0][0],)
        cursor.execute(query, values)
        res = cursor.fetchall()
        if res:
            retval = res[0][0]
    conn.close()
    return retval

def dictFromPoemRow(cursor, poem_row_dict):
    d = {}
    d['complete'] = poem_row_dict['complete']
    d['starting_page'] = poem_row_dict['page_id']
    d['id'] = poem_row_dict['id']
    d['title'] = poem_row_dict['name'].decode('utf-8').replace("_", " ")

    ## Get the text for the line ID's
    line_count = len(filter(lambda x:x.startswith('line_'), poem_row_dict.keys()))
    line_ids = [poem_row_dict['line_'+str(line_num)] for line_num in range(line_count)]
    line_ids_nonone = filter(lambda x: x is not None, line_ids)
    empty_dict = {'page_id':0, 'text':""}
    if len(line_ids_nonone) > 0:
        format_strings = ','.join(['%s'] * len(line_ids_nonone))
        query = (
            """SELECT iambic_lines.id, page_id, line, revision FROM iambic_lines"""
            """ LEFT JOIN lines_revisions ON iambic_lines.id = lines_revisions.line_id"""
            """ WHERE iambic_lines.id IN (%s);""" % format_strings
        )
        values = tuple(line_ids_nonone)
        cursor.execute(query, values)
        res = cursor.fetchall()
        line_dict = {r['id']:(r['page_id'], r['line'], r['revision']) for r in res}
        d['lines'] = [{'page_id':line_dict[_id][0], 'text':line_dict[_id][1], 'revision':line_dict[_id][2]} if _id else empty_dict for _id in line_ids]

    return d

def getCachedPoemForPage(dbconfig, page_id=21, complete=True, session_id=0):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT cached_poems.*, page_names.name FROM cached_poems
                LEFT OUTER JOIN sessions_poems ON cached_poems.id = sessions_poems.poem_id
                JOIN page_names on page_names.page_id = cached_poems.page_id
                WHERE cached_poems.page_id=%s AND complete=%s AND (session_id!=%s OR session_id IS NULL)
                ORDER BY RAND() LIMIT 1;"""
    values = (page_id, complete, session_id)
    cursor.execute(query, values)
    res = cursor.fetchall()
    retval = None;
    if res:
        retval = dictFromPoemRow(cursor, res[0])
    conn.close()
    return retval

def getSpecificPoem(dbconfig, poem_id=181):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT cached_poems.*, page_names.name FROM cached_poems
                JOIN page_names on page_names.page_id = cached_poems.page_id
                WHERE cached_poems.id=%s LIMIT 1;"""
    values = (poem_id,)
    cursor.execute(query, values)
    res = cursor.fetchall()
    retval = None;
    if res:
        retval = dictFromPoemRow(cursor, res[0])
    conn.close()
    return retval

def writeNewPoemForPage(dbconfig, pageID, task_condition, userdata):
    ## Create the row for the cached posStringForPoemLines
    write_conn = mysql.connector.connect(user=dbconfig['user'],
                                        password=dbconfig['password'],
                                        host=dbconfig['host'],
                                        database=dbconfig['database'])
    cursor = write_conn.cursor()
    query = """INSERT INTO cached_poems (page_id) VALUES (%s);"""
    values = (pageID,)
    cursor.execute(query, values)
    cursor.execute("""COMMIT;""");
    query = """SELECT LAST_INSERT_ID();"""
    cursor.execute(query)
    res = cursor.fetchall()
    poem_id = res[0][0]
    write_conn.close()

    ## Create the return dictionary
    d = {}
    d['complete'] = 0
    d['starting_page'] = pageID
    d['id'] = poem_id

    ## Add the poem to the task queue database
    enqueuePoemTaskForPageID(dbconfig, pageID, poem_id, task_condition, userdata)

    return d

def createSession(dbconfig):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor()
    query = """INSERT INTO sessions (created_at) VALUES(current_timestamp);"""
    cursor.execute(query)
    cursor.execute("""COMMIT;""");
    query = """SELECT LAST_INSERT_ID();"""
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()

    return res[0][0]

def getPageId(dbconfig, title):
    title_slug = title.replace(" ", "_")
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor()
    query = """SELECT page_id FROM page_names WHERE name = %s"""
    values = (title_slug,)
    cursor.execute(query, values)
    res = cursor.fetchall()
    cursor.close()
    if len(res) is not 0:
        pageID = res[0][0]
        return pageID
    else:
        return None

def getPageTitle(dbconfig, page_id):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor()
    query = """SELECT name FROM page_names WHERE page_id = %s"""
    values = (page_id,)
    cursor.execute(query, values)
    res = cursor.fetchall()
    cursor.close()
    if len(res) is not 0:
        title = res[0][0]
        title = title.replace("_", " ")
        return title
    else:
        return None


def addPoemToSession(dbconfig, poem_id, session_id):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor()
    query = """INSERT IGNORE INTO sessions_poems (session_id, poem_id) VALUES(%s, %s);"""
    values=(session_id, poem_id)
    cursor.execute(query, values)
    cursor.execute("""COMMIT;""");
    query = """SELECT LAST_INSERT_ID();"""
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()

    return res[0][0]

def getIncompleteTasks(dbconfig, offset, limit):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT * FROM poem_tasks WHERE complete=0 LIMIT %s,%s;"""
    if limit is 0:
        limit = 1000
    values = (offset, limit)
    cursor.execute(query, values)
    res = cursor.fetchall()
    conn.close()

    return res
