import mysql.connector
import db.dbconnect as dbconnect

def getRandomArticleTitle(dbconfig):
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

def getArticleTitleForId(dbconfig, page_id):
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

def getArticleIdForTitle(dbconfig, title):
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
