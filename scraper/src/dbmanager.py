import psycopg2
import hashlib

def digest(text):
    m = hashlib.md5()
    m.update(text)
    return m.digest()

def mapdigest(page_url_tuple):
    plist = list(page_url_tuple)
    plist = map(lambda x: buffer(digest(x), 0, 16), plist)
    return tuple(plist)

class DatabaseConnection:

    def __init__(self, dbname, user, host, password):
        self.dbname = dbname;
        self.user = user;
        self.host = host;
        self.password = password;
        try:
            dbargs = (dbname, user, host, password)
            self.conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % dbargs)
        except:
            print("Unable to connect to the database")

    def debugDump(self):
        cur = self.conn.cursor()
        cur.execute("""SELECT datname from pg_database""")
        rows = cur.fetchall()
        print("\nShow me the databases:\n")
        for row in rows:
            print("\t", row[0])
        cur.close()

    def printTable(self, tablename):
        cur = self.conn.cursor()
        cur.execute("SELECT * from %s" % tablename)
        rows = cur.fetchall()
        print("\nTable %s:\n" % tablename)
        for row in rows:
            print("\t")
            for r in row:
                print(r)
                print(" ")
        cur.close()

    ## lsv is the last-stressed-vowel
    def storePoemLine(self, pageURL, word, text, pos, lsv, starts=False, ends=False):
        m = hashlib.md5()
        m.update(pageURL.encode('utf-8'))
        m.update(text.encode('utf-8'))
        cur = self.conn.cursor()

        ## First check if the particular line-page compination already exists
        query = "SELECT * FROM iambic_lines_2 WHERE id = E'%s'" % m.hexdigest()
        cur.execute(query)
        if not len(cur.fetchall()) > 0 and len(pos) >= 2:
            openpos = "|".join(pos[:2])
            closepos = "|".join(pos[-2:])
            valueStr = (m.hexdigest(), pageURL, pageURL, text, lsv, word, starts, ends, openpos, closepos)
            cur.execute("""INSERT INTO iambic_lines_2 (id, page_url, page_md5, line, last_stressed_vowel, word, starts, ends, openpos, closepos) VALUES (E%s, %s, (decode(md5(%s), 'hex')), %s, %s, %s, %s, %s, %s, %s)""", valueStr)
            self.conn.commit()
        cur.close()

    def updatePOSCounts(self, posdict):
        cur = self.conn.cursor()
        skipcount = 0

        for pos in posdict:

            ## Get the current value at that key
            query = """SELECT count FROM four_gram_pos_counts WHERE md5 = (decode(md5(%s), 'hex'))"""
            cur.execute(query, (pos,));
            rows = cur.fetchall()

            if (len(rows) > 0):

                count = posdict[pos] + rows[0][0]
                query = """UPDATE four_gram_pos_counts SET count = %s WHERE md5 = (decode(md5(%s), 'hex'))"""
                cur.execute(query, (count, pos))
            else:
                query = """INSERT INTO four_gram_pos_counts (four_gram, count, md5) VALUES (%s, %s, (decode(md5(%s), 'hex')))"""
                cur.execute(query, (pos, posdict[pos], pos))
                self.conn.commit()

        cur.close()

    def storeIndexForPage(self, pageTitle, revision, timestamp):
        cur = self.conn.cursor()
        print("Storing index for page " + pageTitle)
        valueStr = (pageTitle, pageTitle, revision, timestamp, pageTitle)
        query = ("""INSERT INTO indexed_pages (md5, link, revision_id, timestamp)"""
                """ SELECT (decode(md5(%s), 'hex')), %s, %s, %s"""
                """ WHERE NOT EXISTS (SELECT 1 FROM indexed_pages WHERE (md5 = decode(md5(%s), 'hex')))"""
                )
        cur.execute(query, valueStr)
        self.conn.commit()
        cur.close()

    def storeLinksForPage(self, pageTitle, linkedPages):
        cur = self.conn.cursor()
        for page in linkedPages:
            query = ("""INSERT INTO links (md5_1, md5_2)"""
                    """ SELECT decode(md5(%s), 'hex'), decode(md5(%s), 'hex')"""
                    """ WHERE NOT EXISTS (SELECT 1 FROM links WHERE"""
                    """ (md5_1 = decode(md5(%s), 'hex') AND md5_2 = decode(md5(%s), 'hex')))"""
                    )
            valueStr = (pageTitle, page, pageTitle, page)
            cur.execute(query, valueStr)
        self.conn.commit()
        cur.close()

    def pagesLinkedFromPage(self, pageTitle):
        cur = self.conn.cursor()
        query = """SELECT md5_2 FROM links WHERE md5_1 = (decode(md5(%s), 'hex'))"""
        values = (pageTitle,)
        cur.execute(query, values)
        res = cur.fetchall();
        cur.close()
        return tuple([x[0] for x in res])

    def randomLines(self, pages=None, num=1, predigested=False):
        cur = self.conn.cursor()
        if not pages:
            query = """SELECT * FROM iambic_lines_2 LIMIT %s"""
            values = (num,)
        else:
            query = ("""SELECT * FROM iambic_lines_2 WHERE"""
                    """ page_md5 IN %s LIMIT %s;"""
                    )
            if not predigested:
                pages = mapdigest(pages)
            values = (pages, num)
        cur.execute(query, values)
        res = cur.fetchall()
        cur.close()
        return res

    def linesRhymingWithLine(self, line, num=1):
        cur = self.conn.cursor()
        valueStr = (line[2], line[3], line[2], line[3], num)
        cur.execute("""SELECT * FROM iambic_lines_2 WHERE last_stressed_vowel = %s \
                    AND word != %s OFFSET random() * (SELECT count(*) FROM iambic_lines_2
                    WHERE last_stressed_vowel = %s AND word != %s) LIMIT %s""", valueStr)
        res = cur.fetchall();
        return res

    def continuationScoreForLine(self, firstLine, continuationLine):
        cur = self.conn.cursor()
        end = firstLine[8]
        start = continuationLine[7]
        pos = "|".join([end, start])
        query = """SELECT count FROM four_gram_pos_counts WHERE four_gram = %s"""
        cur.execute(query, (pos,))
        score = 0
        rows = cur.fetchall()
        if (len(rows) > 0):
            row = rows[0]
            score = row[0]
        return score

    def printTestContinuations(self, num=100):
        line = self.randomLines(1)[0]
        clines = self.randomLines(100)
        slines = sorted(clines, key=lambda sline: self.continuationScoreForLine(line, sline), reverse=True)
        for s in slines:
            print(line[3] + " " + s[3])

    def close(self):
        self.conn.close()
