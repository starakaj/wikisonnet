import gzip
import server.dbmanager

def parseLine(line, db):
    parts = line.split()
    if parts[0] == 'en':
        title = parts[1]
        pageID = db.pageIDForPageTitle(title, doCache=True)
        if pageID is not None:
            db.incrementViewCountForPage(pageID, parts[2], doCommit=False)

def countViews(filename, db):
    if filename.endswith(".gz"):
        f = gzip.open(filename, 'r')
    else:
        f = open(filename, 'r')
    commit_count=0
    commit_ceil=1000
    for line in f:
        parseLine(line, db)
        commit_count += 1
        if commit_count >= commit_ceil:
            commit_count=0
            db.connection.commit()
    f.close()
