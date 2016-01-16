import db.dbconnect as dbconnect
import db.dbwriter as dbwriter
import db.dbreader as dbreader
from util.benchmarking import Timer
import wikiutils
import wordutils
import mwparserfromhell
import textblob
import time
import multiprocessing as mp
from pattern.en import parse
import re
import gensim
import logging
import os

class ScanContext:
    def __init__(self, extractor, dbconn, id2word, lda):
        self.extractor = extractor
        self.dbconn = dbconn
        self.id2word = id2word
        self.lda = lda

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

def scan(extractor_filename, methods=[], startIdx=0, skipevery=1, offset=0):
    logging.basicConfig(format = '%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    page_idx = 0
    dbconn = dbconnect.MySQLDatabaseConnection.connectionWithConfiguration('local')
    extractor = wikiutils.WikiTextExtractor(extractor_filename)
    commit_idx=0
    commit_ceil=1000
    ignore_namespaces = 'Wikipedia Category File Portal Template MediaWiki User Help Book Draft'.split()
    thisdir = "/".join(os.path.realpath(__file__).split("/")[:-1]) + "/"
    id2word = gensim.corpora.Dictionary.load_from_text(thisdir + 'lda/results_wordids.txt.bz2')
    lda = gensim.models.ldamodel.LdaModel.load(thisdir + 'lda/lda_1000')
    ctx = ScanContext(extractor, dbconn, id2word, lda)

    for ex in extractor:
        title = extractor.titleForCurrentPage()
        skip_scan = False
        skip_scan = skip_scan or (page_idx < startIdx)
        skip_scan = skip_scan or (page_idx+offset)%skipevery != 0
        skip_scan = skip_scan or any([title.startswith(ignore + ":") for ignore in ignore_namespaces])
        if not (skip_scan):

            try:
                for m in methods:
                    functionDict[m](ctx)
            except Exception as e:
                print e

        page_idx += 1
        commit_idx += 1
        if commit_idx >= commit_ceil:
            commit_idx=0
            dbconn.connection.commit()
            if offset==0:
                print "\tScanning page %d: %s" % (page_idx, extractor.titleForCurrentPage())
    dbconn.connection.commit()
    print "Scan complete!"

def scanIambic(ctx):
    extractor = ctx.extractor
    dbconn = ctx.dbconn

    ## Skip if redirect
    redirect = extractor.redirectTitleForCurrentPage()
    if redirect is not None:
        return

    try:
        ptext = extractor.textForCurrentPage()
    except:
        print "Error parsing text"
        return
    pageID = extractor.pageIDForCurrentPage()
    iambic_runs = findIambsForPages(ptext, pageID)
    for run in iambic_runs:
        words = run.text.split()
        if wordutils.make_safe(words[-1]) not in wordutils.banned_end_words:
            rhyme_part = wordutils.rhyming_part(wordutils.make_safe(words[-1]))
            rhyme_part = "".join(rhyme_part)
            try:
                dbwriter.storePoemLine(dbconn, pageID, wordutils.make_safe(words[-1]), run.text, run.pos, rhyme_part, run.options)
            except:
                print "store line error"

def countPages(extractor, limit=-1):
    page_idx = 0
    printerval = 1000
    togo = printerval
    t = Timer()
    for ex in extractor:
        page_idx = page_idx + 1
        if togo<=0:
            togo = printerval
            print "Counted " + str(page_idx) + " pages"
        togo = togo - 1
        if limit>0 and page_idx>limit:
            break
    t.printTime()

def scanNames(ctx):
    extractor = ctx.extractor
    dbconn = ctx.dbconn
    page_idx=0
    title = extractor.titleForCurrentPage().replace(' ', '_')
    dbwriter.storeTitleForPage(dbconn, extractor.pageIDForCurrentPage(), title, doCommit=False)

def scanRedirects(ctx):
    extractor = ctx.extractor
    dbconn = ctx.dbconn
    page_id = extractor.pageIDForCurrentPage()
    redirect = extractor.redirectTitleForCurrentPage()
    if redirect is not None:
        redirect = redirect.replace(" ", "_")
        redirectID = dbreader.pageIDForPageTitle(dbconn, redirect)
        if redirectID is not None:
            dbwriter.storeRedirectForPage(dbconn, page_id, redirectID, doCommit=True)

def scanCategories(ctx):
    extractor = ctx.extractor
    dbconn = ctx.dbconn
    page_id = extractor.pageIDForCurrentPage()
    redirect = extractor.redirectTitleForCurrentPage()
    if redirect is None:
        text = extractor.textForCurrentPage()
        bow = ctx.id2word.doc2bow(text.lower().split())
        cat_list = ctx.lda[bow]
        cat = None
        if len(cat_list) > 0:
            cat_list = sorted(cat_list, key=lambda x: x[1], reverse=True)
            cat = cat_list[0][0]
        dbwriter.storeCategoryForPage(dbconn, page_id, cat, doCommit=True)

def scanLinks(ctx):
    extractor = ctx.extractor
    dbconn = ctx.dbconn
    redirect = extractor.redirectTitleForCurrentPage()
    if redirect is None:
        page_id = extractor.pageIDForCurrentPage()

        ## 1. Get the page ID's for the outgoing links
        link_ids = map(lambda x: dbreader.pageIDForPageTitle(dbconn, x.replace(" ", "_"), doCache=True),  extractor.canonicalLinksForCurrentPage())
        link_ids = filter(lambda x: x is not None, link_ids)
        link_ids = list(set(link_ids))

        ## 2. Add outgoing links for the page to the outgoing links table
        dbwriter.storeInternalLinksForPage(dbconn, page_id, link_ids)

def scanRevisions(ctx):
    extractor = ctx.extractor
    dbconn = ctx.dbconn
    dbwriter.storeRevisionForPage(dbconn, extractor.pageIDForCurrentPage(), extractor.revisionForCurrentPage(), doCommit=False)

def displayCategories(extractor):
    foundModels = []
    foundFormats = []

    for ex in extractor:
        model = extractor.modelForCurrentPage()
        form = extractor.formatForCurrentPage()
        if model is not None and model not in foundModels:
            foundModels.append(model)
            name = extractor.titleForCurrentPage()
            print u"Page {} has model {}".format(name, model)
        if form is not None and form not in foundFormats:
            foundFormats.append(form)
            name = extractor.titleForCurrentPage()
            print u"Page {} has format {}".format(name, form)

    print "Scan complete!"

def prepareInputsForTopicModelling(extractor, ofile):
    page_idx = 0
    for _ in extractor:
        page_idx = page_idx+1
        wrote = 0

        ptext = extractor.textForCurrentPage()
        paragraphs = ptext.split("\n")
        for paragraph in paragraphs:
            paragraph = re.sub('\s+', ' ', paragraph)
            if len(paragraph.split(' ')) < 10:
                continue
            blob = textblob.TextBlob(paragraph)
            for sentence in blob.sentences:

                sen = " ".join(sentence.strip().split())
                if len(sen.split(' ')) < 3:
                    continue
                ofile.write(sen)
                ofile.write(" ")
                wrote=1

        if wrote:
            ofile.write('\n')

        if page_idx > 1000:
            break

functionDict = {'names':scanNames, 'links':scanLinks, 'redirects':scanRedirects, 'categories':scanCategories, 'revisions':scanRevisions}
