# from dbhash import columnsDictToSHA
import mysql.connector
import hashlib
import json
import server.benchmarking as benchmarking
from server.wikibard import poemForPageID
import itertools
from multiprocessing import Pool
import gensim

def updateRhymeCounts(dbconn):
    cursor=dbconn.connection.cursor()
    query = """SELECT DISTINCT(word) AS word, rhyme_part FROM iambic_lines;"""
    cursor.execute(query)
    res = cursor.fetchall()
    for rp in res:
        query = """SELECT COUNT(*) FROM iambic_lines WHERE rhyme_part=%s AND word!=%s;"""
        values = (rp[1], rp[0])
        cursor.execute(query, values)
        res2 = cursor.fetchall()
        print rp[0] + " rhymed " + str(res2[0][0]) + " times"
        query = ("""INSERT INTO rhyme_counts (word, count)"""
                """ VALUES (%s, %s)"""
                """ ON DUPLICATE KEY UPDATE"""
                """ word=VALUES(word),"""
                """ count=VALUES(count);"""
                )
        values = (rp[0], res2[0][0])
        cursor.execute(query, values)
        dbconn.connection.commit()
    cursor.close()

def updateLinkCounts(commit_interval=1000, print_interval=1000):
    read_conn = mysql.connector.connect(user="william", password="sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    write_conn = mysql.connector.connect(user="william", password="sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    read_cursor = read_conn.cursor()
    write_cursor = write_conn.cursor()

    try:
        commit_timer = commit_interval
        print_timer = print_interval
        read_query = """SELECT count(page_id), page_id FROM internal_links;"""
        read_cursor.execute(read_query)
        written = 0
        toWrite = read_cursor.rowcount
        for row in read_cursor:
            write_query = """UPDATE indexed_pages SET link_count=%s WHERE page_id=%s"""
            values = (row[0], row[1])
            write_cursor.execute(write_query, values)
            written = written+1

            print_timer = print_timer-1
            if print_timer==0:
                print "Updated {} of {}".format(written, toWrite)
                print_timer = print_interval
            commit_timer = commit_timer-1
            if commit_timer==0:
                write_conn.commit()
        write_conn.commit()

    except Exception as e:
        write_conn.close()
        read_conn.close()
        raise e
    write_conn.close()
    read_conn.close()

def pruneRedirectsFromInternal(commit_interval=1000, print_interval=1000):
    read_conn = mysql.connector.connect(user="william", password="sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    write_conn = mysql.connector.connect(user="william", password="sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    read_cursor = read_conn.cursor()
    write_cursor = write_conn.cursor()

    try:
        print "Beginning"
        commit_timer = commit_interval
        print_timer = print_interval
        read_query = """SELECT page_id FROM redirects;"""
        read_cursor.execute(read_query)
        written = 0
        toWrite = read_cursor.rowcount
        print read_cursor
        for row in read_cursor:
            write_query = """DELETE FROM internal_links WHERE page_id=%s"""
            values = (row[0], )
            write_cursor.execute(write_query, values)
            written = written+1

            print_timer = print_timer-1
            if print_timer==0:
                print "Updated {} of {}".format(written, toWrite)
                print_timer = print_interval
            commit_timer = commit_timer-1
            if commit_timer==0:
                write_conn.commit()
        write_conn.commit()

    except Exception as e:
        write_conn.close()
        read_conn.close()
        raise e
    write_conn.close()
    read_conn.close()

def addSHAKeysToTable(table):
    n2p = {
        "pos_len_m1":"pos_m1",
        "pos_len":"pos_0",
        "pos_len_m2":"pos_m2",
        "pos_len_p1":"pos_1"
        }
    p2n = {n2p[k]:k for k in n2p}
    pconstraints = ["pos_1", "pos_m2", "pos_0", "pos_m1"]
    nconstraints = [p2n[k] for k in pconstraints]

    conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    conn2 = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    cursor = conn.cursor(dictionary=True)
    cursor2 = conn2.cursor()
    query = """SELECT id, rhyme_part, pos_m2, pos_m1, pos_0, pos_1, pos_len_m2, pos_len_m1, pos_len, pos_len_p1 FROM """ + table
    cursor.execute(query)
    for row in cursor:
        for i in range(len(pconstraints)):
            columns = {col:row[col] for col in pconstraints[i:]}
            (col, key) = columnAndKeyForPOSColumnsWithRhyme(row["rhyme_part"], columns, True)
            query = """UPDATE """ + table + """ SET """ + col + """=%s WHERE id=%s"""
            values = (key, row["id"])
            cursor2.execute(query, values)
        for i in range(len(nconstraints)):
            columns = {col:row[col] for col in nconstraints[i:]}
            (col, key) = columnAndKeyForPOSColumnsWithRhyme(row["rhyme_part"], columns, False)
            query = """UPDATE """ + table + """ SET """ + col + """=%s WHERE id=%s"""
            values = (key, row["id"])
            cursor2.execute(query, values)
    cursor.close()
    conn2.commit()
    conn.close()
    conn2.close()

def columnAndKeyForPOSColumnsWithRhyme(rhyme, columns={}, isPrevious=True):
    columns["rhyme_part"] = rhyme
    key = ("""prev""" if isPrevious else """next""") + """key""" + str(len(columns)-1)
    return (key, hashlib.sha1(json.dumps(columns, sort_keys=True)).digest())

leading_pos_keys = ['pos_m2', "pos_m1", 'pos_0', 'pos_1']
lagging_pos_keys = ['pos_len_m2', 'pos_len_m1', 'pos_len', 'pos_len_p1']
def countPOS(commit_interval=1000, print_interval=1000):
    read_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet")
    write_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet")
    read_cursor = read_conn.cursor(dictionary=True)
    write_cursor = write_conn.cursor()
    query = """SELECT """ + ", ".join(leading_pos_keys + lagging_pos_keys) + """ FROM iambic_lines"""
    read_cursor.execute(query)
    written = 0
    toWrite = read_cursor.rowcount
    commit_timer = commit_interval
    print_timer = print_interval
    for row in read_cursor:
        leading_dict = {k:row[k] for k in row if k in leading_pos_keys}
        lagging_dict = {k:row[k] for k in row if k in lagging_pos_keys}

        query = """INSERT INTO leading_pos_counts (leading_4gram, count) VALUES (%s, 1) ON DUPLICATE KEY UPDATE count=count+1;"""
        values = (columnsDictToSHA(leading_dict), )
        write_cursor.execute(query, values)

        query = """INSERT INTO lagging_pos_counts (lagging_4gram, count) VALUES (%s, 1) ON DUPLICATE KEY UPDATE count=count+1;"""
        values = (columnsDictToSHA(lagging_dict), )
        write_cursor.execute(query, values)

        written = written+1

        print_timer = print_timer-1
        if print_timer==0:
            print "Updated {} of {}".format(written, toWrite)
            print_timer = print_interval
        commit_timer = commit_timer-1
        if commit_timer==0:
            write_conn.commit()
            commit_timer = commit_interval
    read_cursor.close()
    write_conn.commit()
    read_conn.close()
    write_conn.close()

def populatePOSHashes(commit_interval=1000, print_interval=1000):
    read_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    write_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    read_cursor = read_conn.cursor(dictionary=True)
    write_cursor = write_conn.cursor()
    query = """SELECT id, pos_m2, pos_m1, pos_0, pos_1, pos_len_m2, pos_len_m1, pos_len, pos_len_p1 FROM iambic_lines"""
    read_cursor.execute(query)
    written=0
    commit_timer = commit_interval
    print_timer = print_interval

    tasks = {
        "leading_4gram" : ["pos_m2", "pos_m1", "pos_0", "pos_1"],
        "leading_3gram" : ["pos_m2", "pos_m1", "pos_0"],
        "leading_2gram" : ["pos_m1", "pos_0"],
        "lagging_4gram" : ["pos_len_m2", "pos_len_m1", "pos_len", "pos_len_p1"],
        "lagging_3gram" : ["pos_len_m2", "pos_len_m1", "pos_len"],
        "lagging_2gram" : ["pos_len_m1", "pos_len"]
    }

    for row in read_cursor:
        shas = {}
        for task_title in tasks:
            d = {key:row[key] for key in tasks[task_title]}
            dict_sha = columnsDictToSHA(d)
            shas[task_title] = dict_sha
        query = """INSERT INTO pos_hashes VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        values = (row["id"], shas["leading_4gram"], shas["leading_3gram"], shas["leading_2gram"], shas["lagging_4gram"], shas["lagging_3gram"], shas["lagging_2gram"])
        write_cursor.execute(query, values)
        written+=1

        print_timer = print_timer-1
        if print_timer==0:
            print "Updated {}".format(written)
            print_timer = print_interval
        commit_timer = commit_timer-1
        if commit_timer==0:
            write_conn.commit()
            commit_timer = commit_interval

    write_conn.commit()
    read_cursor.close()
    read_conn.close()
    write_conn.close()

def cachePoemForPageID(pageID):
    import server.dbconnect as dbconnect

    ## Create the row for the cached posStringForPoemLines
    write_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="cached_poems")
    cursor = write_conn.cursor()
    query = """INSERT INTO cached_poems (page_id) VALUES (%s);"""
    values = (pageID,)
    cursor.execute(query, values)
    cursor.execute("""COMMIT;""");
    query = """SELECT LAST_INSERT_ID();"""
    cursor.execute(query)
    res = cursor.fetchall()
    poem_id = res[0][0]

    ## Write the poem
    dbconfig = dbconnect.MySQLDatabaseConnection.dbconfigForName('local')
    poem = poemForPageID(pageID, 'elizabethan', dbconfig)
    line_ids = [line['id'] for line in poem]

    ## Store the poem
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
    cursor.execute("""COMMIT;""");
    write_conn.close()

def printCachedPoemForID(poemID):
    import server.dbconnect as dbconnect
    read_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="cached_poems")
    cursor = read_conn.cursor()
    query = """SELECT * FROM cached_poems WHERE id=%s;"""
    values = (poemID,)
    cursor.execute(query, values)
    res = cursor.fetchall()
    if len(res) > 0:
        dbconn = dbconnect.MySQLDatabaseConnection.connectionWithConfiguration('local')
        poem = [{'id':res[0][i+2]} for i in range(14)]
        print " "

def writeCachedPoems(limit=10, count=10):
    import server.dbconnect as dbconnect
    read_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet")
    cursor = read_conn.cursor()

    ## Get a whole bunch of poem id's
    query = """SELECT page_id FROM page_names LIMIT %s;"""
    values = (limit,)
    cursor.execute(query, values)
    res = cursor.fetchall()
    tasks = [[x[0] for _ in range(count)] for x in res]
    tasks = list(itertools.chain.from_iterable(tasks))
    read_conn.close()

    t = benchmarking.Timer()
    pool = Pool(processes=30)
    t.begin("writing")
    results = [pool.apply_async(cachePoemForPageID, args=(x,)) for x in tasks]
    output = [p.get() for p in results]
    t.end("writing")
    t.printTime()

def categorizeLines(commit_interval=1000, print_interval=1000):
    read_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet")
    write_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet")
    read_cursor = read_conn.cursor(dictionary=True)
    write_cursor = write_conn.cursor()
    query = """SELECT id, line FROM iambic_lines"""
    read_cursor.execute(query)
    written=0
    commit_timer = commit_interval
    print_timer = print_interval

    id2word = gensim.corpora.Dictionary.load_from_text('lda/results_wordids.txt.bz2')
    lda_1000 = gensim.models.ldamodel.LdaModel.load('lda/lda_1000')
    lda_100 = gensim.models.ldamodel.LdaModel.load('lda/lda_100')

    for row in read_cursor:
        text = row['line']
        bow = id2word.doc2bow(text.lower().split())
        minor_cat_list = lda_1000[bow]
        major_cat_list = lda_100[bow]
        minor_cat = None
        if len(minor_cat_list) > 0:
            minor_cat_list = sorted(minor_cat_list, key=lambda x: x[1], reverse=True)
            minor_cat = minor_cat_list[0][0]
        major_cat = None
        if len(major_cat_list) > 0:
            major_cat_list = sorted(major_cat_list, key=lambda x: x[1], reverse=True)
            major_cat = major_cat_list[0][0]

        query = """INSERT INTO line_categories VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE major_category=VALUES(major_category), minor_category=VALUES(minor_category);"""
        values = (row["id"], minor_cat, major_cat)
        write_cursor.execute(query, values)
        written+=1

        print_timer = print_timer-1
        if print_timer==0:
            print "Updated {}".format(written)
            print_timer = print_interval
        commit_timer = commit_timer-1
        if commit_timer==0:
            write_conn.commit()
            commit_timer = commit_interval

    write_conn.commit()
    read_cursor.close()
    read_conn.close()
    write_conn.close()
