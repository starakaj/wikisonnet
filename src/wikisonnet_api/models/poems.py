import mysql.connector
import db.dbconnect as dbconnect
import tasks
import random

sort_fields = ["lauds", "date"]
fields_columns = {"lauds":"laud_count", "date":"created_on"}

def getNextPoemForPoem(cursor, poem_dict, sortby='date'):
    query = (
        """SELECT cached_poems.id as poem_id, name as page_name FROM cached_poems """
        """INNER JOIN page_names ON cached_poems.page_id = page_names.page_id """
        """WHERE created_on > %s ORDER BY created_on LIMIT 0,1;"""
    )
    values = (poem_dict['created_on'], )
    cursor.execute(query, values)
    res = cursor.fetchall()
    if res:
        return {"poem_id":res[0]['poem_id'], 'page_name':res[0]['page_name'].decode('utf-8')}
    return None

def getPreviousPoemForPoem(cursor, poem_dict, sortby='date'):
    query = (
        """SELECT cached_poems.id as poem_id, name as page_name FROM cached_poems """
        """INNER JOIN page_names ON cached_poems.page_id = page_names.page_id """
        """WHERE created_on < %s ORDER BY created_on DESC LIMIT 0,1;"""
    )
    values = (poem_dict['created_on'], )
    cursor.execute(query, values)
    res = cursor.fetchall()
    if res:
        print res[0]
        return {"poem_id":res[0]['poem_id'], 'page_name':res[0]['page_name'].decode('utf-8')}
    return None

def dictFromPoemRow(cursor, poem_row_dict, sortby='date'):
    d = {}
    d['complete'] = poem_row_dict['complete']
    d['created_on'] = poem_row_dict['created_on']
    d['starting_page'] = poem_row_dict['page_id']
    d['id'] = poem_row_dict['id']
    d['title'] = poem_row_dict['name'].decode('utf-8').replace("_", " ")
    d['lauds'] = poem_row_dict['laud_count']
    d['lauded_by_session'] = poem_row_dict['session']

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

    ## Get the next and previous poem
    next_poem = getNextPoemForPoem(cursor, d, sortby)
    if next_poem:
        d['next'] = next_poem
    prev_poem = getPreviousPoemForPoem(cursor, d, sortby)
    if prev_poem:
        d['previous'] = prev_poem

    return d

def getCachedPoemForArticle(dbconfig, page_id=21, complete=True, session_id=0):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT cached_poems.*, page_names.name, COUNT(lauds.poem_id) as laud_count, session_lauds.session FROM cached_poems
                LEFT OUTER JOIN sessions_poems ON cached_poems.id = sessions_poems.poem_id
                JOIN page_names ON page_names.page_id = cached_poems.page_id
                LEFT JOIN lauds ON lauds.poem_id = cached_poems.id
                LEFT OUTER JOIN lauds AS session_lauds ON lauds.poem_id = cached_poems.id AND lauds.session = %s
                WHERE cached_poems.page_id=%s AND complete=%s AND (session_id!=%s OR session_id IS NULL)
                GROUP BY lauds.poem_id, cached_poems.id, page_names.name, session_lauds.session
                ORDER BY RAND() LIMIT 1;"""
    values = (session_id, page_id, complete, session_id)
    cursor.execute(query, values)
    res = cursor.fetchall()
    retval = None;
    if res:
        retval = dictFromPoemRow(cursor, res[0])
    conn.close()
    return retval

def getSpecificPoem(dbconfig, poem_id=181, session_id=0, sortby='date'):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT cached_poems.*, page_names.name, COUNT(lauds.poem_id) as laud_count, session_lauds.session FROM cached_poems
                JOIN page_names on page_names.page_id = cached_poems.page_id
                LEFT JOIN lauds ON lauds.poem_id = cached_poems.id
                LEFT OUTER JOIN lauds AS session_lauds ON lauds.poem_id = cached_poems.id AND lauds.session = %s
                WHERE cached_poems.id=%s
                GROUP BY lauds.poem_id, cached_poems.id, page_names.name, session_lauds.session
                LIMIT 1;"""
    values = (session_id, poem_id)
    cursor.execute(query, values)
    res = cursor.fetchall()
    retval = None;
    if res:
        retval = dictFromPoemRow(cursor, res[0], sortby)
    conn.close()
    return retval

def writeNewPoemForArticle(dbconfig, pageID, task_condition, userdata):
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
    tasks.enqueuePoemTaskForPageID(dbconfig, pageID, poem_id, task_condition, userdata)

    return d

def getPoems(dbconfig, offset, limit, session_id=0, options={}):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT cached_poems.*, page_names.name, COUNT(lauds.poem_id) as laud_count, session_lauds.session FROM cached_poems
                JOIN page_names on page_names.page_id = cached_poems.page_id
                LEFT JOIN lauds ON lauds.poem_id = cached_poems.id
                LEFT OUTER JOIN lauds AS session_lauds ON lauds.poem_id = cached_poems.id AND lauds.session = %s
                WHERE complete=1"""
    values = (session_id, )
    if "after" in options:
        query = query + """ AND created_on >= %s"""
        values = values + (options['after'], )
    if "before" in options:
        query = query + """ AND created_on <= %s"""
        values = values + (options['before'], )
    query = query + """ GROUP BY lauds.poem_id, cached_poems.id, page_names.name, session_lauds.session"""
    if "sortby" in options and options['sortby'] in sort_fields:
        query = query + """ ORDER BY {} DESC LIMIT %s,%s;""".format(fields_columns[options['sortby']])
    else:
        query = query + """ ORDER BY id DESC LIMIT %s,%s;"""
    if limit is 0:
        limit = 1000
    values = values + (offset, limit)
    cursor.execute(query, values)
    res = cursor.fetchall()
    poems = [dictFromPoemRow(cursor, row) for row in res]
    conn.close()
    return poems

def getRandomPoem(dbconfig, session_id, options={}):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT COUNT(*) as poem_count FROM cached_poems WHERE complete=1"""
    values = ()
    if "after" in options:
        query = query + """ AND created_on >= %s"""
        values = values + (options['after'], )
    if "before" in options:
        query = query + """ AND created_on <= %s"""
        values = values + (options['before'], )
    cursor.execute(query, values)
    res = cursor.fetchall()
    count = res[0]['poem_count']
    conn.close()
    poems = getPoems(dbconfig, random.randint(0, count-1), 1, session_id, options)
    return poems[0]
