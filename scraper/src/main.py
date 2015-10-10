import dbmanager
import wikiutils

print "Loading NLTK resources..."
import wordutils
import mwparserfromhell
import sys
import textblob
from pattern.en import parse

print "Extracting iambic lines from %s" % sys.argv[1]

print "Connecting to local database"
dbconn = dbmanager.DatabaseConnection('samtarakajian', 'samtarakajian', 'localhost', '')

print "Parsing XML..."
extractor = wikiutils.WikiTextExtractor(sys.argv[1])

print "Beginning scan:"
for i, page in enumerate(extractor.pages):
    print "\tScanning page %d of %d, %s" % (i, len(extractor.pages), extractor.titleForPage(page))
    # for link in extractor.canonicalLinksForPage(page):
    #     print "\t\t%s" % link
    ptext = extractor.textForPage(page)
    for paragraph in ptext.split("\n"):
        blob = textblob.TextBlob(paragraph)
        for sen in blob.sentences:

            ## Train our part-of-speech n-gram model on this sentence
            pos = wordutils.pos_counts(sen.string)
            dbconn.updatePOSCounts(pos)

            ## Get the runs for each sentence
            ## Each sentence is just a raw, nasty string of the code-stripped sentence
            iambic_runs = wordutils.extract_iambic_pentameter(sen.string)
            for run in iambic_runs:
                words = run.text.split()
                if wordutils.make_safe(words[-1]) not in wordutils.banned_end_words:
                    rhyme_part = wordutils.rhyming_part(wordutils.make_safe(words[-1]))
                    rhyme_part = "".join(rhyme_part)
                    dbconn.storePoemLine("en.wikipedia.org", wordutils.make_safe(words[-1]), run.text, run.pos, rhyme_part, run.starts, run.ends)

print "Scan complete!"
dbconn.close()
