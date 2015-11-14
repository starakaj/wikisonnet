import server.dbmanager as dbmanager
from server.benchmarking import Timer
import wikiutils
import wordutils
import mwparserfromhell
import textblob
import time
import multiprocessing as mp
from pattern.en import parse

def findIambsForPages(ptext, pageID):
    iambic_runs = []
    paragraphs = ptext.split("\n")
    for paragraph in paragraphs:
        blob = textblob.TextBlob(paragraph)
        for sen in blob.sentences:

            ## Get the runs for each sentence
            ## Each sentence is just a raw, nasty string of the code-stripped sentence
            iambic_runs = iambic_runs + wordutils.extract_iambic_pentameter(sen.string)
    return iambic_runs

def scanIambic(extractor, dbconn):
    print "Beginning scan:"

    page_idx = 0
    page_count = 0 ## extractor.page_count()

    for ex in extractor:
        page_idx = page_idx+1
        if (page_idx < 10052905):
            continue
        if page_idx%3!=0:
            continue
        title = extractor.titleForCurrentPage()
        if title.split(":")[0] == "MediaWiki":
            continue
        if title.split(":")[0] == "File":
            continue
        print "\tScanning page %d of %d, %s" % (page_idx, page_count, extractor.titleForCurrentPage())

        ## Skip if redirect
        redirect = extractor.redirectTitleForCurrentPage()
        if redirect is not None:
            continue

        try:
            ptext = extractor.textForCurrentPage()
        except:
            print "Error parsing text"
            continue
        pageID = extractor.pageIDForCurrentPage()
        iambic_runs = findIambsForPages(ptext, pageID)
        for run in iambic_runs:
            words = run.text.split()
            if wordutils.make_safe(words[-1]) not in wordutils.banned_end_words:
                rhyme_part = wordutils.rhyming_part(wordutils.make_safe(words[-1]))
                rhyme_part = "".join(rhyme_part)
                try:
                    dbconn.storePoemLine(pageID, wordutils.make_safe(words[-1]), run.text, run.pos, rhyme_part, run.options)
                except:
                    print "store line error"

    print "Scan complete!"

def scanPOS(extractor, dbconn):
    print "Beginning scan:"
    for i, page in enumerate(extractor.pages):
        print "\tScanning page %d of %d, %s" % (i, len(extractor.pages), extractor.titleForCurrentPage())
        ptext = extractor.textForCurrentPage()
        for paragraph in ptext.split("\n"):
            blob = textblob.TextBlob(paragraph)
            for sen in blob.sentences:

                ## Train our part-of-speech n-gram model on this sentence
                pos = wordutils.pos_counts(sen.string)
                dbconn.updatePOSCounts(pos)

    print "Scan complete!"

def countPages(extractor):
    page_idx = 0
    printerval = 20000
    togo = printerval
    for ex in extractor:
        page_idx = page_idx + 1
        if togo<=0:
            togo = printerval
            print "Counted " + str(page_idx) + " pages"
        togo = togo - 1

def scanLinks(extractor, dbconn):
    page_idx = 0
    page_count = 0 ## extractor.page_count()

    for ex in extractor:
        try:
            page_idx = page_idx+1
            if (page_idx%3)!=2:
                continue
            page_id = extractor.pageIDForCurrentPage()
            page_title = extractor.titleForCurrentPage().replace(" ", "_")
            revision_id = extractor.revisionIDForCurrentPage()
            datestring = extractor.timestampForCurrentPage()
            print "\tScanning page %d of %d, %s" % (page_idx, page_count, extractor.titleForCurrentPage())

            ## 1. Put the page into the list of indexed pages, with its title
            dbconn.storeTitleForPage(page_id, page_title, revision_id, datestring)

            ## 2. Store the redirect title for the page, if any
            redirect = extractor.redirectTitleForCurrentPage()
            if redirect is not None:
                print "\t\tStoring redirect from %s to %s" % (extractor.titleForCurrentPage(), redirect)
                redirect = redirect.replace(" ", "_")
                dbconn.storeRedirectForPage(page_id, redirect, revision_id, datestring)

            ## 3. Clear up old internal links for the page
            dbconn.removeOldLinksForPage(page_id, revision_id)

            ## 4. Add outgoing links for the page to the outgoing links table
            dbconn.storeInternalLinksForPage(page_id, extractor.canonicalLinksForCurrentPage(), revision_id, datestring)
        except:
            print "Error"

    print "Scan complete!"
