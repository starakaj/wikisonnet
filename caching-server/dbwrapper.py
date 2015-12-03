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
