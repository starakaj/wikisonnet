import mysql.connector
import db.dbconnect as dbconnect
import wikibard.wikibard as wikibard
from multiprocessing import Process
import time
from models import tasks

def task_processor(dbconfig, condition):
    # Check if there are any poems that need to be written
    while (True):
        condition.acquire()
        incomplete_tasks = tasks.getIncompleteTasks(dbconfig)
        if not incomplete_tasks:
            condition.wait()
            condition.release()
        else:
            condition.release()
            writePoemForTask(dbconfig, incomplete_tasks[0])

def writePoemForTask(dbconfig, task):
    page_id = task["page_id"]
    poem_id = task["poem_id"]
    wikibard.poemForPageID(page_id, 'elizabethan', dbconfig, multi=True, callback=stanzaWrite, user_info=(poem_id, dbconfig))
    tasks.markTaskCompleted(dbconfig, task)

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
