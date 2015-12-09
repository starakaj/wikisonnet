import dbreader, dbconnect
import wikibard
import wikipedia
import random
from IPython import embed

# Change this if you'd like to use a local database or something
dbconfigname = 'local'

def removeTrailing(img_link):
    fmts = ['.jpg', '.png', '.tiff', '.gif', '.svg', '.bmp', '.jpeg']
    for fmt in fmts:
        if fmt in img_link:
            img_link = img_link[:img_link.find(fmt)+len(fmt)]
            break

def imagesForPageTitle(title):
    #just gotta sanitize the input
    title = title.replace("_", " ")
    page = wikipedia.page(title)
    if page:
        images = [x for x in page.images if not "Commons-logo" in x]
        images = [x for x in page.images if not ".svg" in x]
        # images = map(removeTrailing, images)
        images = sorted(images, key = lambda x: random.random())
        return images

def poemForPageTitle(title):
    dbconn = dbconnect.MySQLDatabaseConnection.connectionWithConfiguration(dbconfigname)
    dbconfig = dbconnect.MySQLDatabaseConnection.dbconfigForName(dbconfigname)
    pageID = dbreader.pageIDForPageTitle(dbconn, title)
    if pageID > 0:
        poem_lines = wikibard.poemForPageID(pageID, 'elizabethan', dbconfig)
        return wikibard.poemStringForPoemLines(dbconn, poem_lines)
    else:
        return ""
