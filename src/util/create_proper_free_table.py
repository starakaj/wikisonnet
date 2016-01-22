import mysql.connector
import hashlib
import json
import server.benchmarking as benchmarking
from server.wikibard import poemForPageID

pos_columns = ['pos_m2', 'pos_m1', 'pos_0', 'pos_1', 'pos_len_m2', 'pos_len_m1', 'pos_len', 'pos_len_p1']
pos_map = {'NNP':'NN', 'NNPS':'NNS', 'NNP-LOC':'NN', 'NNPS-LOC':'NNS'}

query = None

if __name__ == "__main__":
    read_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet")
    write_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet")

    read_cursor = read_conn.cursor(dictionary=True)
    write_cursor = write_conn.cursor()
    cnnt = 0
    total = 0

    read_cursor.execute("""SELECT * FROM iambic_lines""")
    for row in read_cursor:
        for pos_col in pos_columns:
            if row[pos_col] in pos_map:
                row[pos_col] = pos_map[row[pos_col]]

        if query is None:
            query = """INSERT INTO iambic_lines_no_proper ("""
            for col in row:
                query = query + col + """, """
            if query.endswith(""", """):
                query = query[:-2]
            query = query + """) VALUES ("""
            values = tuple([row[r] for r in row])
            for v in values:
                query = query + """%s, """
            if query.endswith(""", """):
                query = query[:-2]
            query = query + """);"""
        else:
            values = tuple([row[r] for r in row])
            
        write_cursor.execute(query, values)
        if cnnt == 1000:
            cnnt=0
            print "Total: {}".format(total)
            write_cursor.execute("""COMMIT;""")
        cnnt += 1
        total += 1

    print "Total: {}".format(total)
    write_cursor.execute("""COMMIT;""")
    read_conn.close()
    write_conn.close()
