import psycopg2
import hashlib
import math
import random
import mysql.connector
import iso8601
from benchmarking import Timer

def digest(text):
    m = hashlib.md5()
    m.update(text)
    return m.digest()

def mapdigest(page_url_tuple):
    plist = list(page_url_tuple)
    plist = map(lambda x: buffer(digest(x), 0, 16), plist)
    return tuple(plist)

def queryWithAddedWhere(query, phrase, valueList, appendedValue=None):
    continuing_where = False
    if query:
        continuing_where = """WHERE""" in query
    if continuing_where:
        query = query + """ AND"""
    else:
        query = query + """ WHERE"""
    valueList.append(appendedValue)
    return query + phrase

def queryWithAddedWhereAppend(query, phrase, valueList, appendedList=None):
    continuing_where = False
    if query:
        continuing_where = """WHERE""" in query
    if continuing_where:
        query = query + """ AND"""
    else:
        query = query + """ WHERE"""
    for v in appendedList:
        valueList.append(v)
    return query + phrase

class MySQLDatabaseConnection:
    def __init__(self, dbname, user, host, password):
        self.dbname = dbname;
        self.user = user;
        self.host = host;
        self.connection = mysql.connector.connect(user=user, password=password, host=host, database=dbname, charset='utf8', use_unicode=True)

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def randomIndexedPage(self):
        cur = self.connection.cursor()
        offset = random.randint(0, 4000000)
        query = """SELECT page_id FROM iambic_lines LIMIT 1 OFFSET %s;"""
        values = (offset,)
        cur.execute(query, values)
        res = cur.fetchall()
        cur.close()
        return res

    def storeTitleForPage(self, pageID, pagetitle, revision, datestring):
        cursor = self.connection.cursor()
        d = iso8601.parse_date(datestring)
        query = ("""INSERT INTO page_titles (page_id, page_title, revision_id, date)"""
                """ VALUES (%s, %s, %s, %s)"""
                """ ON DUPLICATE KEY UPDATE"""
                """ page_title=if(VALUES(date) > date, VALUES(page_title), page_title),"""
                """ revision_id=if(VALUES(date) > date, VALUES(revision_id), revision_id),"""
                """ date=if(VALUES(date) > date, VALUES(date), date);"""
                )
        values = (pageID, pagetitle, revision, d.strftime('%Y-%m-%d %H:%M:%S'))
        cursor.execute(query, values)
        self.connection.commit()
        cursor.close()

    def updateRhymeCounts(self):
        cursor=self.connection.cursor()
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
            self.connection.commit()
        cursor.close()

    def storeRedirectForPage(self, pageID, redirect_title, revision, datestring):
        cursor = self.connection.cursor()
        d = iso8601.parse_date(datestring)
        query = ("""INSERT INTO redirects (page_id, redirect_title, revision_id, date)"""
                """ VALUES (%s, %s, %s, %s)"""
                """ ON DUPLICATE KEY UPDATE"""
                """ redirect_title=if(VALUES(date) > date, VALUES(redirect_title), redirect_title),"""
                """ revision_id=if(VALUES(date) > date, VALUES(revision_id), revision_id),"""
                """ date=if(VALUES(date) > date, VALUES(date), date);"""
                )
        values = (pageID, redirect_title, revision, d.strftime('%Y-%m-%d %H:%M:%S'))
        cursor.execute(query, values)
        self.connection.commit()
        cursor.close()

    def removeOldLinksForPage(self, pageID, revision):
        cursor = self.connection.cursor()
        query = ("""DELETE FROM internal_links"""
                """ WHERE page_id = %s AND revision_id < %s"""
                )
        values = (pageID, revision)
        cursor.execute(query, values)
        self.connection.commit()
        cursor.close()

    def storeInternalLinksForPage(self, pageID, links, revision, datestring):
        cursor = self.connection.cursor()
        d = iso8601.parse_date(datestring)
        for link_title in links:
            query = ("""INSERT INTO internal_links (page_id, link_title, revision_id, date)"""
                    """ VALUES (%s, %s, %s, %s)"""
                    """ ON DUPLICATE KEY UPDATE"""
                    """ link_title=if(VALUES(date) > date, VALUES(link_title), link_title),"""
                    """ revision_id=if(VALUES(date) > date, VALUES(revision_id), revision_id),"""
                    """ date=if(VALUES(date) > date, VALUES(date), date);"""
                    )
            values = (pageID, link_title, revision, d.strftime('%Y-%m-%d %H:%M:%S'))
            cursor.execute(query, values)
        self.connection.commit()
        cursor.close()

    ## store one iambic line in the database
    def storePoemLine(self, pageID, word, text, pos, rhyme, options=None):
        cursor = self.connection.cursor()

        ## First check if the particular line-page compination already exists
        query = """SELECT * FROM iambic_lines WHERE page_id = %s AND line_sha1 = UNHEX(SHA1(%s))"""
        values = (pageID, text)
        cursor.execute(query, values)
        if not len(cursor.fetchall()) > 0 and len(pos) >= 2:
            valueList = [pageID, word, rhyme, text, text]
            query = """INSERT INTO iambic_lines (page_id, word, rhyme_part, line, line_sha1"""
            if options:
                for k in options:
                    query = query + """, """ + k
                    valueList.append(options[k])
            query = query + """) VALUES (%s, %s, %s, %s, UNHEX(SHA1(%s))"""
            if options:
                for k in options:
                    query = query + """, %s"""
            query = query + """)"""
            cursor.execute(query, tuple(valueList))
            self.connection.commit()
        cursor.close()

    def pageIDForPageTitle(self, page_title):
        cursor = self.connection.cursor()
        query = """SELECT page_id FROM page_titles WHERE page_title = %s"""
        values = (page_title,)
        cursor.execute(query, values)
        res = cursor.fetchall()
        if len(res) is not 0:
            pageID = res[0][0]
        else:
            pageID = None
        cursor.close()
        return pageID

    def pageTitleForPageID(self, pageID):
        cursor = self.connection.cursor()
        query = """SELECT page_title FROM page_titles WHERE page_id = %s"""
        values = (pageID,)
        cursor.execute(query, values)
        res = cursor.fetchall()
        if len(res) is not 0:
            page_title = res[0][0]
        else:
            page_title = None
        cursor.close()
        return page_title

    def followRedirectForPageID(self, pageID):
        cursor = self.connection.cursor()
        query = """SELECT redirect_title FROM redirects WHERE page_id = (%s)"""
        values = (pageID,)
        cursor.execute(query, values)
        res = cursor.fetchall();
        cursor.close()

        if len(res) is not 0:
            redirect_title = res[0][0]
            pageID = self.pageIDForPageTitle(redirect_title)
        return pageID

    def pagesLinkedFromPageID(self, pageID):
        pageID = self.followRedirectForPageID(pageID)
        cursor = self.connection.cursor()
        query = """SELECT link_title FROM internal_links WHERE page_id = %s"""
        values = (pageID,)
        cursor.execute(query, values)
        links = cursor.fetchall()
        cursor.close()
        for i in range(len(links)):
            links[i] = self.pageIDForPageTitle(links[i][0])
        links = filter(lambda x: x is not None, links)
        return links

    def rhymeCountForRhyme(self, word, rhyme):
        cursor = self.connection.cursor()
        query = """SELECT count FROM rhyme_counts WHERE word=%s"""
        values = (word,)
        cursor.execute(query, values)
        cnt = cursor.fetchall()
        if len(cnt)>0:
            cursor.close()
            return cnt[0][0]
        query = """SELECT COUNT(*) FROM iambic_lines WHERE word != %s AND rhyme_part = %s"""
        values = (word, rhyme)
        cursor.execute(query, values)
        cnt = cursor.fetchall()
        cursor.close()
        return cnt[0][0]

    def randomLines(self, pages=None, num=1, predigested=False, brandom=False, options=None):

        t = Timer()

        t.begin("prepare")
        cur = self.connection.cursor(dictionary=True) ## DictCursor is best
        query = """SELECT * FROM iambic_lines"""
        valueList = [];
        continuing_where = False

        excludedWord=None
        leadChunk=None
        excludedLines=None
        starts=None
        ends=None
        rhyme=None
        if options:
            for k in options:
                if k == "excludedWord":
                    excludedWord = options[k]
                if k == "excludedLines":
                    excludedLines = options[k]
                if k == "leadChunk":
                    leadChunk = options[k]
                if k == "starts":
                    starts = options[k]
                if k == "ends":
                    ends = options[k]
                if k == "rhyme":
                    rhyme = options[k]

        ### Building WHERE clause ###
        if (starts!=None):
            query = queryWithAddedWhere(query, """ starts = %s""", valueList, starts)
        if (ends!=None):
            query = queryWithAddedWhere(query, """ ends = %s""", valueList, ends)
        if rhyme:
            query = queryWithAddedWhere(query, """ rhyme_part = %s""", valueList, rhyme)
        if pages:
            if len(pages) is 1:
                query = queryWithAddedWhere(query, """ page_id = %s""", valueList, pages[0])
            else:
                format_strings = ','.join(['%s'] * len(pages))
                query = queryWithAddedWhereAppend(query, """ page_id IN (%s)""" % format_strings, valueList, list(pages))
        if leadChunk:
            query = queryWithAddedWhere(query, """ lead_chunk = %s""", valueList, leadChunk)
        if excludedWord:
            query = queryWithAddedWhere(query, """ word != %s""", valueList, excludedWord)
        if excludedLines:
            if len(excludedLines) is 1:
                query = queryWithAddedWhere(query, """ id != %s""", valueList, excludedLines[0])
            else:
                format_strings = ','.join(['%s'] * len(excludedLines))
                query = queryWithAddedWhereAppend(query, """ id NOT IN (%s)""" % format_strings, valueList, list(excludedLines))
        if "pos_0" in options:
            query = queryWithAddedWhere(query, """ pos_0 = %s""", valueList, options["pos_0"])
        if "pos_1" in options:
            query = queryWithAddedWhere(query, """ pos_0 = %s""", valueList, options["pos_1"])
        if "pos_m2" in options:
            query = queryWithAddedWhere(query, """ pos_m2 = %s""", valueList, options["pos_m2"])
        if "pos_m1" in options:
            query = queryWithAddedWhere(query, """ pos_m1 = %s""", valueList, options["pos_m1"])
        ### WHERE ###

        if brandom:
            query = query + """ ORDER BY RAND()"""

        query = query + """ LIMIT %s;"""
        valueList.append(num)
        values = tuple(valueList)
        t.end("prepare")

        t.begin("execute")
        cur.execute(query, values)
        # print cur.statement
        res = cur.fetchall()
        cur.close()
        t.end("execute")
        # t.printTime()
        return res
