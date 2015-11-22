import server.dbmanager as dbmanager
import server.wikibard as wikibard
import scraper.wikiutils as wikiutils
import scraper.scanner as scanner
import sys
import codecs

print "Connecting to local database"
dbconfig = {'database':'wikisonnet',
            'user':'william',
            'host':'localhost',
            'password':'Sh4kespeare'}
# dbconn = dbmanager.MySQLDatabaseConnection(dbconfig["database"], dbconfig["user"], dbconfig["host"], dbconfig["password"])

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
        f = dbconn.randomIndexedPage()
    else:
        f = dbconn.pageIDForPageTitle(f)
    # wikibard.iPoem("en.wikipedia.org/wiki/" + f)
    wikibard.iPoem(f, dbconfig, debug_print=True)
elif func == 'count':
    print "Counting pages"
    print "Parsing XML..."
    extractor = wikiutils.WikiTextExtractor(sys.argv[1])
    scanner.countPages(extractor, 10000)
elif func == 'categorize':
    print "Displaying all categories"
    extractor = wikiutils.WikiTextExtractor(sys.argv[1])
    scanner.displayCategories(extractor)
elif func == 'topicPrep':
    print 'Formatting output for topic extraction'
    extractor = wikiutils.WikiTextExtractor(sys.argv[1])
    ofile  = codecs.open(sys.argv[3], 'w', 'utf-8')
    scanner.prepareInputsForTopicModelling(extractor, ofile)
    ofile.close()

# dbconn.close()
