import argparse
from server.benchmarking import Timer
import server.wikibard as wikibard
import server.dbreader as dbreader
import server.dbconnect as dbconnect
import random
import codecs

parser = argparse.ArgumentParser(description="Write a bunch of poems, see how long it takes")
parser.add_argument('count', type=int, help="number of poems to write")
parser.add_argument('--output', type=str, default=None, help="output file")

if __name__ == '__main__':
    args = parser.parse_args()
    t = Timer()
    t.begin("poems")
    outf = None
    if args.output:
        outf = codecs.open(args.output, 'w', 'utf-8')
    dbconfig = dbconnect.MySQLDatabaseConnection.dbconfigForName('local')
    dbconn = dbconnect.MySQLDatabaseConnection.connectionWithConfiguration('local')
    cursor = dbconn.connection.cursor()
    query = (
        """SELECT view_counts.id FROM view_counts INNER JOIN page_categories"""
        """ ON page_categories.id = view_counts.id WHERE view_counts.count < 202917"""
        """ ORDER BY view_counts.count DESC LIMIT %s;"""
        )
    values = (10000, )
    cursor.execute(query, values)
    res = cursor.fetchall()
    for i in range(args.count):
        random_id = random.sample(res, 1)[0][0]
        random_id = 35607283
        poem = wikibard.poemForPageID(random_id, 'elizabethan', dbconfig, multi=True)
        lines = [dbreader.textForLineID(dbconn, p['id']) for p in poem]
        if outf:
            outf.write(dbreader.pageTitleForPageID(dbconn, random_id))
            outf.write(u'\n\n')
            for l in lines:
                outf.write(l)
                outf.write(u'\n')
            outf.write(u"\n\n\n")
    if outf:
        outf.close()
    t.end("poems")
    t.printTime()
