import server.dbconnect as dbconnect
import server.dbreader as dbreader
import argparse
import codecs
import random

parser = argparse.ArgumentParser(description="Write a whole shitload of poems")
parser.add_argument('count', type=int, help="number of poems to write")
parser.add_argument('--output', action='store', type=str, default='poems.txt', help="Number of separate processes to run")

args = parser.parse_args()

if __name__ == '__main__':
    conn = dbconnect.MySQLDatabaseConnection.connectionWithConfiguration('local')
    cursor = conn.connection.cursor(dictionary=True)
    query = """SELECT COUNT(*) FROM cached_poems WHERE complete=1;"""
    cursor.execute(query);
    res = cursor.fetchall()
    if not res:
        print "Could not count cached poems"
        exit(1)
    poem_count = res[0]['COUNT(*)']


    with codecs.open(args.output, "w", "utf-8") as outf:
        for _ in range(args.count):
            query = """SELECT * FROM cached_poems WHERE complete=1 LIMIT 1 OFFSET %s;"""
            values = (random.randint(0, poem_count), )
            cursor.execute(query, values)
            res = cursor.fetchall()[0]
            title = dbreader.pageTitleForPageID(conn, res['page_id'])
            outf.write("{} -- {}:\n\n".format(res['id'], title));
            for i in range(14):
                col = "line_{}".format(i)
                lineID = res[col]
                line = dbreader.textForLineID(conn, lineID)
                outf.write(line)
                outf.write("\n")
            outf.write('\n\n\n')
    conn.close()
