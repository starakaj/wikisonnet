import dbmanager
import wikiutils
import wordutils
import mwparserfromhell
import textblob
from pattern.en import parse

def scanIambic(extractor, dbconn):
    print "Beginning scan:"
    for i, page in enumerate(extractor.pages):
        print "\tScanning page %d of %d, %s" % (i, len(extractor.pages), extractor.titleForPage(page))
        ptext = extractor.textForPage(page)
        pagelink = extractor.titleForPage(page).replace(" ", "_")
        for paragraph in ptext.split("\n"):
            blob = textblob.TextBlob(paragraph)
            for sen in blob.sentences:

                ## Get the runs for each sentence
                ## Each sentence is just a raw, nasty string of the code-stripped sentence
                iambic_runs = wordutils.extract_iambic_pentameter(sen.string)
                for run in iambic_runs:
                    words = run.text.split()
                    if wordutils.make_safe(words[-1]) not in wordutils.banned_end_words:
                        rhyme_part = wordutils.rhyming_part(wordutils.make_safe(words[-1]))
                        rhyme_part = "".join(rhyme_part)
                        dbconn.storePoemLine(pagelink, wordutils.make_safe(words[-1]), run.text, run.pos, rhyme_part, run.starts, run.ends)

    print "Scan complete!"

def scanPOS(extractor, dbconn):
    print "Beginning scan:"
    for i, page in enumerate(extractor.pages):
        print "\tScanning page %d of %d, %s" % (i, len(extractor.pages), extractor.titleForPage(page))
        ptext = extractor.textForPage(page)
        for paragraph in ptext.split("\n"):
            blob = textblob.TextBlob(paragraph)
            for sen in blob.sentences:

                ## Train our part-of-speech n-gram model on this sentence
                pos = wordutils.pos_counts(sen.string)
                dbconn.updatePOSCounts(pos)

    print "Scan complete!"

def scanLinks(extractor, dbconn):
    print "Beginning scan:"
    for i, page in enumerate(extractor.pages):
        pagelink = extractor.titleForPage(page).replace(" ", "_")
        print "\tScanning page %d of %d, %s" % (i, len(extractor.pages), extractor.titleForPage(page))

        ## 1. Put the page into the list of indexed pages, with the current timestamp
        dbconn.storeIndexForPage(pagelink, extractor.revisionIDForPage(page), extractor.timestampForPage(page))

        ## 2. Add outgoing links for the page to the outgoing links table
        dbconn.storeLinksForPage(pagelink, extractor.canonicalLinksForPage(page))

    print "Scan complete!"
