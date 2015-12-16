import dbhash
import random
from benchmarking import Timer

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
    cursor = dbconn.connection.cursor()
    query = """SELECT to_id FROM redirects WHERE from_id = (%s)"""
    values = (pageID,)
    cursor.execute(query, values)
    res = cursor.fetchall();
    cursor.close()

    if len(res) is not 0:
        pageID = res[0][0]
    return pageID

def pagesLinkedFromPageID(dbconn, pageID):
    cursor = dbconn.connection.cursor()
    query = """SELECT to_id FROM page_links WHERE from_id = %s"""
    values = (pageID,)
    cursor.execute(query, values)
    links = cursor.fetchall()
    cursor.close()
    links = [l[0] for l in links]
    return links

def categoryForPageID(dbconn, pageID, order='minor'):
    cursor = dbconn.connection.cursor()
    query = """SELECT minor_category, major_category FROM page_categories WHERE id=%s;"""
    values = (pageID,)
    cursor.execute(query, values)
    res = cursor.fetchall()
    if len(res)==0:
        return None
    elif order=='minor':
        return res[0][0]
    elif order=='major':
        return res[0][1]
    else:
        raise ValueError("order argument must be one of ['minor', 'major']")

def textForLineID(dbconn, lineID):
    cursor = dbconn.connection.cursor()
    query = """SELECT line FROM iambic_lines WHERE id=%s;"""
    values = (lineID,)
    cursor.execute(query, values)
    res = cursor.fetchall()
    if len(res)==0:
        return None
    return res[0][0]

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

pos_prev_constraints = ["pos_0", "pos_1", "pos_m1", "pos_m2"]
pos_next_constraints = ["pos_len_m2", "pos_len_m1", "pos_len", "pos_len_p1"]
hashed_leading_names = ["leading_2gram", "leading_3gram", "leading_4gram"]
hashed_lagging_names = ["lagging_2gram", "lagging_3gram", "lagging_4gram"]
pos_config_sets = [{"constraints":pos_prev_constraints, "names":hashed_leading_names},
                    {"constraints":pos_next_constraints, "names":hashed_lagging_names}]

## line must be a dictionary containing the keys for either a lagging 4gram or a leading 4gram
def posCountsForLine(dbconn, line, position='leading'):
    cursor = dbconn.connection.cursor()

    if position=='leading':
        dict_to_hash = {k:line[k] for k in line if k in pos_next_constraints}
        query = """SELECT COUNT(*) FROM leading_pos_counts WHERE leading_4gram=%s"""
        values = (dbhash.columnsDictToSHA(dict_to_hash), )
    elif position=='lagging':
        dict_to_hash = {k:line[k] for k in line if k in pos_prev_constraints}
        query = """SELECT COUNT(*) FROM lagging_pos_counts WHERE lagging_4gram=%s"""
        values = (dbhash.columnsDictToSHA(dict_to_hash), )
    else:
        raise ValueError("position must be one of ['leading', 'lagging']")

    cursor.execute(query, values)
    res = cursor.fetchall()
    if len(res) > 0:
        return res[0][0]
    return 0

def optimizedConstraints(constraints):
    mod_constraints = {k:constraints[k] for k in constraints}

    for pos_config in pos_config_sets:

        pos_constraints = pos_config["constraints"]
        column_names = pos_config["names"]

        # Get the columns that we are going to combine into a precomputed hash for those columns
        pos_columns = filter(lambda x: x in pos_constraints, constraints.keys())

        # Check whether or not there are at least two parts of speech in question
        if len(pos_columns) >= 2:

            # Get a dictionary for which to compute a hash
            dict_to_hash = {col:constraints[col] for col in pos_columns}
            dict_as_sha = dbhash.columnsDictToSHA(dict_to_hash)

            # modify options to include the new keys
            hashed_column_name = column_names[len(pos_columns) - 2]
            for p in pos_columns:
                mod_constraints.pop(p)
            mod_constraints[hashed_column_name] = dict_as_sha

    return mod_constraints

def searchForLines(dbconn, group=None, constraints=None, options=None):

    cur = dbconn.connection.cursor(dictionary=True)
    num = options.get('num', 1)
    brandom = options.get('random', False)
    optimized = options.get('optimized', False)
    query = """SELECT iambic_lines.id, word, rhyme_part, pos_m2, pos_m1, pos_0, pos_1, pos_len_m2, pos_len_m1, pos_len, pos_len_p1, word_len_m1, word_len FROM iambic_lines """
    valueList = [];
    continuing_where = False
    if optimized:
        constraints = optimizedConstraints(constraints)
        if any([p in constraints for p in hashed_leading_names+hashed_lagging_names]):
            query = query + """ join pos_hashes on pos_hashes.line_id = iambic_lines.id"""
    if group is not None and 'page_minor_category' in group or 'page_major_category' in group:
        query = query + """ join page_categories on page_categories.id = iambic_lines.page_id"""
    if group is not None and 'line_minor_category' in group or 'line_major_category' in group:
        query = query + """ join line_categories on line_categories.id = iambic_lines.id"""

    ### Building WHERE clause ###
    for key in constraints:
        if key == "excluded_word":
            query = queryWithAddedWhere(query, """ iambic_lines.word != %s""", valueList, constraints[key])
        elif key == "excluded_lines":
            excludedLines = constraints[key]
            if len(excludedLines) is 1:
                query = queryWithAddedWhere(query, """ iambic_lines.id != %s""", valueList, excludedLines[0])
            elif len(excludedLines) is not 0:
                format_strings = ','.join(['%s'] * len(excludedLines))
                query = queryWithAddedWhereAppend(query, """ iambic_lines.id NOT IN (%s)""" % format_strings, valueList, list(excludedLines))
        else:
            if key in hashed_leading_names + hashed_lagging_names:
                query = queryWithAddedWhere(query, """ pos_hashes.""" + key + """ = %s""", valueList, constraints[key])
            else:
                query = queryWithAddedWhere(query, """ iambic_lines.""" + key + """ = %s""", valueList, constraints[key])

    if group:
        if 'pageIDs' in group:
            pages = group['pageIDs']
            if len(pages) is 1:
                query = queryWithAddedWhere(query, """ page_id = %s""", valueList, pages[0])
            else:
                format_strings = ','.join(['%s'] * len(pages))
                query = queryWithAddedWhereAppend(query, """ page_id IN (%s)""" % format_strings, valueList, list(pages))
        if 'page_minor_category' in group:
            query = queryWithAddedWhere(query, """ minor_category = %s""", valueList, group['page_minor_category'])
        if 'page_major_category' in group:
            query = queryWithAddedWhere(query, """ major_category = %s""", valueList, group['page_major_category'])
    ### WHERE ###

    if brandom:
        query = query + """ ORDER BY RAND()"""

    query = query + """ LIMIT %s;"""
    valueList.append(num)
    values = tuple(valueList)
    try:
        cur.execute(query, values)
    except Exception as e:
        print cur.statement
        raise e
    if options.get('print_statement', False):
        print cur.statement
    res = cur.fetchall()
    cur.close()
    return res
