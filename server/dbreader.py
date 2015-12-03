import dbhash
import random
from benchmarking import Timer
import scraper.dbreformatting as dbreformatting

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

def randomIndexedPage(dbconn):
    cur = dbconn.connection.cursor()
    offset = random.randint(0, 4000000)
    query = """SELECT page_id FROM iambic_lines LIMIT 1 OFFSET %s;"""
    values = (offset,)
    cur.execute(query, values)
    res = cur.fetchall()
    cur.close()
    return res[0][0]


def pageIDForPageTitle(dbconn, name, doCache=False):
    if name in dbconn.name_cache:
        return dbconn.name_cache[name]

    cursor = dbconn.connection.cursor()
    query = """SELECT page_id FROM page_names WHERE name = %s"""
    values = (name,)
    cursor.execute(query, values)
    res = cursor.fetchall()
    cursor.close()
    if len(res) is not 0:
        pageID = res[0][0]
        if doCache:
            dbconn.name_cache[name] = pageID
        return pageID
    else:
        return None

def pageTitleForPageID(dbconn, pageID):
    cursor = dbconn.connection.cursor()
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

def followRedirectForPageID(dbconn, pageID):
    cursor = sedbconnlf.connection.cursor()
    query = """SELECT to_id FROM redirects WHERE from_id = (%s)"""
    values = (pageID,)
    cursor.execute(query, values)
    res = cursor.fetchall();
    cursor.close()

    if len(res) is not 0:
        pageID = res[0][0]
    return pageID

def pagesLinkedFromPageID(dbconn, pageID):
    pageID = dbconn.followRedirectForPageID(pageID)
    cursor = dbconn.connection.cursor()
    query = """SELECT to_id FROM page_links WHERE from_id = %s"""
    values = (pageID,)
    cursor.execute(query, values)
    links = cursor.fetchall()
    cursor.close()
    links = [l[0] for l in links]
    return links

def rhymeCountForRhyme(dbconn, word, rhyme):
    cursor = dbconn.connection.cursor()
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

def modifiedOptions(options):
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
            dict_as_sha = dbhash.columnsDictToSHA(dict_to_hash)

            # modify options to include the new keys
            hashed_column_name = column_names[len(pos_columns) - 2]
            for p in pos_columns:
                mod_options.pop(p)
            mod_options[hashed_column_name] = dict_as_sha

    return mod_options

def randomLines(dbconn, pages=None, num=1, brandom=False, optimized=False, options=None):

    t = Timer()

    print options

    t.begin("prepare")
    cur = dbconn.connection.cursor(dictionary=True) ## DictCursor is best
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
    dbconn.statement = cur.statement
    print cur.statement
    res = cur.fetchall()
    cur.close()
    t.end("execute")
    dbconn.execution_time = execute_timer.elapsed()
    t.printTime()
    return res
