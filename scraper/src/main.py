import sys
sys.path.append("../../server")

import dbmanager
import wikiutils
import sys
import scanner
import wikibard

print "Connecting to local database"
dbconn = dbmanager.MySQLDatabaseConnection('wikisonnet', 'william', 'localhost', 'sh4kespeare')

func = sys.argv[2]

if func == 'iambic':
    print "Extracting iambic lines from %s" % sys.argv[1]
    print "Parsing XML..."
    extractor = wikiutils.WikiTextExtractor(sys.argv[1])
    scanner.scanIambic(extractor, dbconn)
elif func == 'pos':
    print "Training parts of speech model on %s" % sys.argv[1]
    print "Parsing XML..."
    extractor = wikiutils.WikiTextExtractor(sys.argv[1])
    scanner.scanPOS(extractor, dbconn)
elif func == 'links':
    print "Extracting table links from %s" % sys.argv[1]
    print "Parsing XML..."
    extractor = wikiutils.WikiTextExtractor(sys.argv[1])
    scanner.scanLinks(extractor, dbconn)
elif func == 'ibard':
    print "Interactive poem test:"
    f = sys.argv[1]
    if sys.argv[1]=="random":
        f = dbconn.randomIndexedPage()[0][0]
    else:
        f = dbconn.pageIDForPageTitle(f)
    # wikibard.iPoem("en.wikipedia.org/wiki/" + f)
    wikibard.iPoem(f)
elif func == 'count':
    print "Counting pages"
    print "Parsing XML..."
    extractor = wikiutils.WikiTextExtractor(sys.argv[1])
    scanner.countPages(extractor)

dbconn.close()
