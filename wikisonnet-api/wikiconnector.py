import mysql.connector
from flask import jsonify
import wikibard.wikibard as wikibard
import wikibard.dbconnect as dbconnect
from multiprocessing import Process

def dbconfigForName(name='local'):
    return dbconnect.MySQLDatabaseConnection.dbconfigForName(name)

def writePoem(dbconfig, page_id, poem_id):
    ## Write the poem
    poem = wikibard.poemForPageID(page_id, 'elizabethan', dbconfig)
    print(poem)
    line_ids = [line['id'] for line in poem]

    ## Store the poem
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor()
    query = (
        """UPDATE cached_poems SET"""
        """ line_0=%s, line_1=%s, line_2=%s, line_3=%s,"""
        """ line_4=%s, line_5=%s, line_6=%s, line_7=%s,"""
        """ line_8=%s, line_9=%s, line_10=%s, line_11=%s,"""
        """ line_12=%s, line_13=%s, complete=1"""
        """ WHERE id=%s;"""
    )
    values = tuple(line_ids + [poem_id])
    cursor.execute(query, values)
    cursor.execute("""COMMIT;""")
    conn.close()

def writePoemAsync(dbconfig, page_id, poem_id):
    p = Process(target=writePoem, args=(dbconfig, page_id, poem_id))
    p.start()

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
    if d['complete'] == 1:
        line_count = len(filter(lambda x:x.startswith('line_'), poem_row_dict.keys()))
        line_ids = [poem_row_dict['line_'+str(line_num)] for line_num in range(line_count)]
        format_strings = ','.join(['%s'] * len(line_ids))
        query = """SELECT id, page_id, line FROM iambic_lines WHERE id IN (%s);""" % format_strings
        values = tuple(line_ids)
        cursor.execute(query, values)
        res = cursor.fetchall()
        line_dict = {r['id']:(r['page_id'], r['line']) for r in res}
        d['lines'] = [{'page_id':line_dict[_id][0], 'text':line_dict[_id][1]} for _id in line_ids]
    return d

def getCachedPoemForPage(dbconfig, page_id=21, complete=True):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT * FROM cached_poems WHERE page_id=%s AND complete=%s LIMIT 1;"""
    values = (page_id, complete)
    cursor.execute(query, values)
    res = cursor.fetchall()
    retval = None;
    print(res)
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
    query = """SELECT * FROM cached_poems WHERE id=%s LIMIT 1;"""
    values = (poem_id,)
    cursor.execute(query, values)
    res = cursor.fetchall()
    retval = None;
    if res:
        retval = dictFromPoemRow(cursor, res[0])
    conn.close()
    return retval

def writeNewPoemForPage(dbconfig, page_id=21):
    d = {}
    d['complete'] = 0
    d['starting_page'] = page_id
    d['id'] = 1337
    writePoemAsync(dbconfig, page_id)
    return d;

def writeNewPoemForPage(dbconfig, pageID):
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

    ## Write the poem asynchronously
    writePoemAsync(dbconfig, pageID, poem_id)

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

     
