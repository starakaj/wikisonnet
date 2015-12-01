import hashlib
import math
import random
import mysql.connector
import iso8601
import json
from benchmarking import Timer
import scraper.dbreformatting as dbreformatting
import os
import yaml

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
    def __init__(self, dbname, user, host, password, options=None):
        self.dbname = dbname
        self.user = user
        self.host = host
        self.connection = mysql.connector.connect(user=user, password=password, host=host, database=dbname, charset='utf8', use_unicode=True)
        self.statement = None
        self.execution_time = 0
        self.name_cache = {}

        if options is not None:
            for k in options:
                if k == "use_cache":
                    self.connection.cursor().execute("""SET SESSION query_cache_type=%s""", (options[k],))
                else:
                    print 'Ignoring unrecognized option {}'.format(k)

    @staticmethod
    def connectionWithConfiguration(config):
        filename = os.path.join(os.path.dirname(__file__), 'dbconfig.yml')
        f = open(filename, 'r')
        databases = yaml.load(f)
        dbconfig = databases[config]
        return MySQLDatabaseConnection(dbconfig['database'], dbconfig['user'], dbconfig['host'], dbconfig['password'])

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
        return res[0][0]

    def storeTitleForPage(self, pageID, pagetitle, doCommit=True):
        cursor = self.connection.cursor()
        query = ("""INSERT INTO page_names (page_id, name)"""
                """ VALUES (%s, %s)"""
                """ ON DUPLICATE KEY UPDATE"""
                """ name=name;"""
                )
        values = (pageID, pagetitle)
        cursor.execute(query, values)
        if doCommit:
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

    def storeRedirectForPage(self, pageID, redirectID, doCommit=True):
        cursor = self.connection.cursor()
        query = ("""INSERT INTO redirects (from_id, to_id)"""
                """ VALUES (%s, %s)"""
                """ ON DUPLICATE KEY UPDATE"""
                """ to_id=to_id;"""
                )
        values = (pageID, redirectID)
        cursor.execute(query, values)
        if doCommit:
            self.connection.commit()
        cursor.close()

    def storeCategoryForPage(self, pageID, categoryID, doCommit=True):
        cursor = self.connection.cursor()
        query = ("""INSERT INTO page_categories (id, category)"""
                """ VALUES (%s, %s)"""
                """ ON DUPLICATE KEY UPDATE"""
                """ category=category;"""
                )
        values = (pageID, categoryID)
        cursor.execute(query, values)
        if doCommit:
            self.connection.commit()
        cursor.close()

    def storeInternalLinksForPage(self, pageID, linkIDs):
        cursor = self.connection.cursor()
        for linkID in linkIDs:
            query = ("""INSERT IGNORE INTO page_links (from_id, to_id)"""
                    """ VALUES (%s, %s);"""
                    )
            values = (pageID, linkID)
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

    def incrementViewCountForPage(self, pageID, count, doCommit=True):
        cursor = self.connection.cursor()
        query = (
            """INSERT INTO view_counts (id, count) VALUES(%s, %s)"""
            """ ON DUPLICATE KEY UPDATE count=count+VALUES(count);"""
        )
        values = (pageID, count)
        cursor.execute(query, values)
        if doCommit:
            self.connection.commit()
        cursor.close()

    def pageIDForPageTitle(self, name, doCache=False):

        if name in self.name_cache:
            return self.name_cache[name]

        cursor = self.connection.cursor()
        query = """SELECT page_id FROM page_names WHERE name = %s"""
        values = (name,)
        cursor.execute(query, values)
        res = cursor.fetchall()
        cursor.close()
        if len(res) is not 0:
            pageID = res[0][0]
            if doCache:
                self.name_cache[name] = pageID
            return pageID
        else:
            return None
        # query = """SELECT redirect_title FROM redirects WHERE page_id=%s"""
        # values = (pageID,)
        # cursor.execute(query, values)
        # res = cursor.fetchall()
        # if len(res) is not 0:
        #     query = """SELECT page_id FROM page_names WHERE BINARY(name) = %s"""
        #     values = (res[0][0],)
        #     res = cursor.fetchall()
        #     if len(res) is not 0:
        #         pageID = res[0][0]
        #     else:
        #         return None
        # cursor.close()
        # return pageID

    def pageTitleForPageID(self, pageID):
        cursor = self.connection.cursor()
        query = """SELECT name FROM page_names WHERE page_id = %s"""
        values = (pageID,)
        cursor.execute(query, values)
        res = cursor.fetchall()
        if len(res) is not 0:
            name = res[0][0]
        else:
            name = None
        cursor.close()
        return name

    def followRedirectForPageID(self, pageID):
        cursor = self.connection.cursor()
        query = """SELECT to_id FROM redirects WHERE from_id = (%s)"""
        values = (pageID,)
        cursor.execute(query, values)
        res = cursor.fetchall();
        cursor.close()

        if len(res) is not 0:
            pageID = res[0][0]
        return pageID

    def pagesLinkedFromPageID(self, pageID):
        pageID = self.followRedirectForPageID(pageID)
        cursor = self.connection.cursor()
        query = """SELECT to_id FROM page_links WHERE from_id = %s"""
        values = (pageID,)
        cursor.execute(query, values)
        links = cursor.fetchall()
        cursor.close()
        links = [l[0] for l in links]
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

    def modifiedOptions(self, options):
        mod_options = {k:options[k] for k in options.keys()}
        pos_prev_options = ["pos_0", "pos_1", "pos_m1", "pos_m2"]
        pos_next_options = ["pos_len_m2", "pos_len_m1", "pos_len", "pos_len_p1"]
        hashed_leading_names = ["leading_2gram", "leading_3gram", "leading_4gram"]
        hashed_lagging_names = ["lagging_2gram", "lagging_3gram", "lagging_4gram"]
        pos_config_sets = [{"options":pos_prev_options, "names":hashed_leading_names},
                            {"options":pos_next_options, "names":hashed_lagging_names}]
        # pos_option_sets = [pos_prev_options, pos_next_options]

        for pos_config in pos_config_sets:

            pos_options = pos_config["options"]
            column_names = pos_config["names"]

            # Get the columns that we are going to combine into a precomputed hash for those columns
            pos_columns = filter(lambda x: x in pos_options, options.keys())

            # Check whether or not there are at least two parts of speech in question
            if len(pos_columns) >= 2:

                # Get a dictionary for which to compute a hash
                dict_to_hash = {col:options[col] for col in pos_columns}
                dict_as_sha = dbreformatting.columnsDictToSHA(dict_to_hash)

                # modify options to include the new keys
                hashed_column_name = column_names[len(pos_columns) - 2]
                for p in pos_columns:
                    mod_options.pop(p)
                mod_options[hashed_column_name] = dict_as_sha

        return mod_options

    def randomLines(self, pages=None, num=1, predigested=False, brandom=False, optimized=False, options=None):

        t = Timer()

        print options

        t.begin("prepare")
        cur = self.connection.cursor(dictionary=True) ## DictCursor is best
        query = """SELECT * FROM iambic_lines """
        valueList = [];
        continuing_where = False
        # if optimized:
        #     options = self.modifiedOptions(options)
        #     if any([p in options for p in ["leading_2gram", "leading_3gram", "leading_4gram", "lagging_2gram", "lagging_3gram", "lagging_4gram"]]):
        #         query = query + """ join pos_hashes on pos_hashes.line_id = iambic_lines.id"""

        excludedWord=None
        leadChunk=None
        excludedLines=None
        starts=None
        ends=None
        rhyme=None

        ### Building WHERE clause ###
        for key in options:
            if key == "excludedWord":
                query = queryWithAddedWhere(query, """ iambic_lines.word != %s""", valueList, options[key])
            elif key == "rhyme":
                query = queryWithAddedWhere(query, """ iambic_lines.rhyme_part = %s""", valueList, options[key])
            elif key == "excludedLines":
                excludedLines = options[key]
                if len(excludedLines) is 1:
                    query = queryWithAddedWhere(query, """ iambic_lines.id != %s""", valueList, excludedLines[0])
                elif len(excludedLines) is not 0:
                    format_strings = ','.join(['%s'] * len(excludedLines))
                    query = queryWithAddedWhereAppend(query, """ iambic_lines.id NOT IN (%s)""" % format_strings, valueList, list(excludedLines))
            else:
                query = queryWithAddedWhere(query, """ iambic_lines.""" + key + """ = %s""", valueList, options[key])

        if pages:
            if len(pages) is 1:
                query = queryWithAddedWhere(query, """ page_id = %s""", valueList, pages[0])
            else:
                format_strings = ','.join(['%s'] * len(pages))
                query = queryWithAddedWhereAppend(query, """ page_id IN (%s)""" % format_strings, valueList, list(pages))
        ### WHERE ###

        if brandom:
            query = query + """ ORDER BY RAND()"""

        query = query + """ LIMIT %s;"""
        valueList.append(num)
        values = tuple(valueList)
        t.end("prepare")

        t.begin("execute")

        execute_timer = Timer()
        cur.execute(query, values)
        self.statement = cur.statement
        print cur.statement
        res = cur.fetchall()
        cur.close()
        t.end("execute")
        self.execution_time = execute_timer.elapsed()
        t.printTime()
        return res
