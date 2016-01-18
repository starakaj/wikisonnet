import mysql.connector
import db.dbconnect as dbconnect
import tasks

def dictFromPoemRow(cursor, poem_row_dict):
    d = {}
    d['complete'] = poem_row_dict['complete']
    d['starting_page'] = poem_row_dict['page_id']
    d['id'] = poem_row_dict['id']
    d['title'] = poem_row_dict['name'].decode('utf-8').replace("_", " ")
    d['lauds'] = poem_row_dict['COUNT(lauds.poem_id)']
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

    return d

def getCachedPoemForArticle(dbconfig, page_id=21, complete=True, session_id=0):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT cached_poems.*, page_names.name, COUNT(lauds.poem_id), session_lauds.session FROM cached_poems
                LEFT OUTER JOIN sessions_poems ON cached_poems.id = sessions_poems.poem_id
                JOIN page_names ON page_names.page_id = cached_poems.page_id
                LEFT JOIN lauds ON lauds.poem_id = cached_poems.id
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

def getSpecificPoem(dbconfig, poem_id=181, session_id=0):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT cached_poems.*, page_names.name, COUNT(lauds.poem_id), session_lauds.session FROM cached_poems
                JOIN page_names on page_names.page_id = cached_poems.page_id
                LEFT JOIN lauds ON lauds.poem_id = cached_poems.id
                LEFT OUTER JOIN lauds AS session_lauds ON lauds.poem_id = cached_poems.id AND lauds.session = %s
                WHERE cached_poems.id=%s
                GROUP BY lauds.poem_id, cached_poems.id, page_names.name, session_lauds.session
                LIMIT 1;"""
    values = (poem_id, session_id)
    cursor.execute(query, values)
    res = cursor.fetchall()
    retval = None;
    if res:
        retval = dictFromPoemRow(cursor, res[0])
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

def getPoems(dbconfig, offset, limit, session_id=0):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """SELECT cached_poems.*, page_names.name, COUNT(lauds.poem_id), session_lauds.session FROM cached_poems
                JOIN page_names on page_names.page_id = cached_poems.page_id
                LEFT JOIN lauds ON lauds.poem_id = cached_poems.id
                LEFT OUTER JOIN lauds AS session_lauds ON lauds.poem_id = cached_poems.id AND lauds.session = %s
                WHERE complete=1
                GROUP BY lauds.poem_id, cached_poems.id, page_names.name, session_lauds.session
                ORDER BY id DESC LIMIT %s,%s;"""
    if limit is 0:
        limit = 1000
    values = (session_id, offset, limit)
    cursor.execute(query, values)
    res = cursor.fetchall()
    poems = [dictFromPoemRow(cursor, row) for row in res]
    conn.close()
    return poems
