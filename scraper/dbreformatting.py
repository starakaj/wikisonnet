import mysql.connector
import hashlib
import json

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

def columnsDictToSHA(columns):
    return hashlib.sha1(json.dumps(columns, sort_keys=True)).digest()

def countTrailingPOS(commit_interval=1000, print_interval=1000):
    read_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    write_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    read_cursor = read_conn.cursor(dictionary=True)
    write_cursor = write_conn.cursor()
    query = """SELECT pos_len_m2, pos_len_m1, pos_len, pos_len_p1 FROM iambic_lines"""
    read_cursor.execute(query)
    written = 0
    toWrite = read_cursor.rowcount
    commit_timer = commit_interval
    print_timer = print_interval
    for row in read_cursor:
        query = """INSERT INTO trailing_pos_counts (tail_sha, pos_len_m2, pos_len_m1, pos_len, pos_len_p1, count) VALUES (%s, %s, %s, %s, %s, 1) ON DUPLICATE KEY UPDATE count=count+1;"""
        values = (columnsDictToSHA(row), row['pos_len_m2'], row['pos_len_m1'], row['pos_len'], row['pos_len_p1'])
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
