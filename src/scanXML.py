import wikibard.wikibard as wikibard
import scraper.scanner as scanner
import sys
import codecs
import argparse
from multiprocessing import Process


def doScan(filename, methods, total, offset):
    scanner.scan(filename, methods, skipevery=total, offset=offset)

parser = argparse.ArgumentParser(description="Scan a wikipedia XML file for various data")
parser.add_argument('filename', type=str, help="the XML file to be scanned")
parser.add_argument('-n', '--names', action='store_true', default=False, help="Store the name of each page")
parser.add_argument('-i', '--iambic', action='store_true', default=False, help="Scan the pages for iambs")
parser.add_argument('-r', '--redirect', action='store_true', default=False, help="Store page-page redirects (names must already be saved)")
parser.add_argument('-l', '--links', action='store_true', default=False, help="Store page-page links (names must already be saved)")
parser.add_argument('-c', '--categories', action='store_true', default=False, help="Store categories (names must already be saved)")
parser.add_argument('-s', '--revisions', action='store_true', default=False, help="Store revisions")
parser.add_argument('--processes', action='store', type=int, default=1, help="Number of separate processes to run")
parser.add_argument('--skip', action='store', type=int, default=1, help="How many nodes to skip per process. --skip=2 will scan every other node")
parser.add_argument('--offset', action='store', type=int, default=0, help="Use with --skip to offset")

args = parser.parse_args()
methods = []
if args.names:
    methods.append('names')
if args.redirect:
    methods.append('redirects')
if args.links:
    methods.append('links')
if args.categories:
    methods.append('categories')
if args.revisions:
    methods.append('revisions')

pool = []
if args.processes>1:
    for i in range(args.processes):
        p = Process(target=doScan, args=(args.filename, methods, args.processes, i))
        pool.append(p)
        p.start()
    try:
        for p in pool:
            p.join()
    except Exception as e:
        print e
        for p in pool:
            p.terminate()
else:
    doScan(args.filename, methods, args.skip, args.offset)


# if func == 'iambic':
#     print "Extracting iambic lines from %s" % sys.argv[1]
#     print "Parsing XML..."
#     extractor = wikiutils.WikiTextExtractor(sys.argv[1])
#     scanner.scanIambic(extractor)
# elif func == 'pos':
#     print "Training parts of speech model on %s" % sys.argv[1]
#     print "Parsing XML..."
#     extractor = wikiutils.WikiTextExtractor(sys.argv[1])
#     scanner.scanPOS(extractor)
# elif func == 'links':
#     print "Extracting table links from %s" % sys.argv[1]
#     print "Parsing XML..."
#     extractor = wikiutils.WikiTextExtractor(sys.argv[1])
# elif func == 'count':
#     print "Counting pages"
#     print "Parsing XML..."
#     extractor = wikiutils.WikiTextExtractor(sys.argv[1])
#     scanner.countPages(extractor, 10000)
# elif func == 'categorize':
#     print "Displaying all categories"
#     extractor = wikiutils.WikiTextExtractor(sys.argv[1])
#     scanner.displayCategories(extractor)
