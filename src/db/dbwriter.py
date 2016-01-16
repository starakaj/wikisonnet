def storeTitleForPage(dbconn, pageID, pagetitle, doCommit=True):
    cursor = dbconn.connection.cursor()
    query = ("""INSERT INTO page_names (page_id, name)"""
            """ VALUES (%s, %s)"""
            """ ON DUPLICATE KEY UPDATE"""
            """ name=name;"""
            )
    values = (pageID, pagetitle)
    cursor.execute(query, values)
    if doCommit:
        dbconn.connection.commit()
    cursor.close()

def storeRedirectForPage(dbconn, pageID, redirectID, doCommit=True):
    cursor = dbconn.connection.cursor()
    query = ("""INSERT INTO redirects (from_id, to_id)"""
            """ VALUES (%s, %s)"""
            """ ON DUPLICATE KEY UPDATE"""
            """ to_id=to_id;"""
            )
    values = (pageID, redirectID)
    cursor.execute(query, values)
    if doCommit:
        dbconn.connection.commit()
    cursor.close()

def storeCategoryForPage(dbconn, pageID, categoryID, doCommit=True):
    cursor = dbconn.connection.cursor()
    query = ("""INSERT INTO page_categories (id, minor_category)"""
            """ VALUES (%s, %s)"""
            """ ON DUPLICATE KEY UPDATE"""
            """ minor_category=VALUES(minor_category);"""
            )
    values = (pageID, categoryID)
    cursor.execute(query, values)
    if doCommit:
        dbconn.connection.commit()
    cursor.close()

def storeInternalLinksForPage(dbconn, pageID, linkIDs):
    cursor = dbconn.connection.cursor()
    for linkID in linkIDs:
        query = ("""INSERT IGNORE INTO page_links (from_id, to_id)"""
                """ VALUES (%s, %s);"""
                )
        values = (pageID, linkID)
        cursor.execute(query, values)
    dbconn.connection.commit()
    cursor.close()

def storeRevisionForPage(dbconn, pageID, revision, doCommit=True):
    cursor = dbconn.connection.cursor()
    query = ("""INSERT INTO pages_revisions (page_id, revision)"""
            """ VALUES (%s, %s)"""
            """ ON DUPLICATE KEY UPDATE"""
            """ revision=VALUES(revision);"""
            )
    values = (pageID, revision)
    cursor.execute(query, values)
    if doCommit:
        dbconn.connection.commit()
    cursor.close()

## store one iambic line in the database
def storePoemLine(dbconn, pageID, word, text, pos, rhyme, options=None):
    cursor = dbconn.connection.cursor()

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
        dbconn.connection.commit()
    cursor.close()
