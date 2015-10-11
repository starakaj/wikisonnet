import dbmanager
import wikiutils
import sys
import scanner

print "Extracting iambic lines from %s" % sys.argv[1]

print "Connecting to local database"
dbconn = dbmanager.DatabaseConnection('samtarakajian', 'samtarakajian', 'localhost', '')

print "Parsing XML..."
extractor = wikiutils.WikiTextExtractor(sys.argv[1])
func = sys.argv[2]

if func == 'iambic':
    scanner.scanIambic(extractor, dbconn)
elif func == 'pos':
    scanner.scanPOS(extractor, dbconn)
elif func == 'links':
    scanner.scanLinks(extractor, dbconn)

dbconn.close()
