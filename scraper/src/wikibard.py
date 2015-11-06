import dbmanager
import wordutils
import urlparse
import pdb
import random
from benchmarking import Timer

debug_print = False

def continuePoem(dbconn, page, links, excludedWord=None, excludedLines=None, previousLine=None, nextLine=None, continues=False, ends=False, starts=False, rhyme=None):
    num = 10

    t = Timer()

    t.begin("setup")
    continues = (nextLine!=None) or (previousLine!=None)
    if continues:
        num = 10
    status = {}
    lines = []
    soft_constraints_previous = ["ends", "pos_m1", "pos_0", "pos_m2", "pos_1"]
    soft_constraints_next = ["pos_len_m1", "pos_len", "pos_len_m2", "pos_len_p1"]

    options = {"excludedWord":excludedWord, "excludedLines":excludedLines, "starts":starts, "ends":ends, "rhyme":rhyme}
    ## If we're continuing, then we need to look at the previous line to know
    ## what parts of speech are valid in our continuation.

    if previousLine:
        if previousLine["pos_len_m1"] is not None:
            options["pos_m1"] = previousLine["pos_len_m1"] ## close_pos1
        if previousLine["pos_len"] is not None:
            options["pos_0"] = previousLine["pos_len"] ## pos_p1
        if previousLine["pos_len_m2"] is not None:
            options["pos_m2"] = previousLine["pos_len_m2"] ## close_pos2
        if previousLine["pos_len_p1"] is not None:
            options["pos_1"] = previousLine["pos_len_p1"] ## pos_p2
    if nextLine:
        options["pos_len_m1"] = nextLine["pos_m1"] ## pos_m1
        options["pos_len"] = nextLine["pos_0"] ## open_pos1
        options["pos_len_m2"] = nextLine["pos_m2"] ## pos_m2
        options["pos_len_p1"] = nextLine["pos_1"] ## open_pos2

    if previousLine and nextLine:
        soft_constraints = [soft_constraints_next[i/2] if i%2 else soft_constraints_previous[i/2] for i in range(len(soft_constraints_next)*2)]
    elif previousLine:
        soft_constraints = soft_constraints_previous
    elif nextLine:
        soft_constraints = soft_constraints_next
    else:
        soft_constraints = []

    t.end("setup")

    for i in range(len(soft_constraints), -1, -1):
        t.begin("pos-search")
        lines = lines + dbconn.randomLines(pages=(page,), predigested=False, options=options, brandom=True, num=(num-len(lines)))
        if debug_print:
            print "Found " + str(len(lines)) + " lines on the original page"

        # If there are no good continuations on this page, look on pages connected to this page
        if len(lines) < num:
            more_lines = dbconn.randomLines(pages=links, predigested=True, options=options, brandom=False, num=(num-len(lines)))
            lines = lines + more_lines

        if debug_print:
            print "Found " + str(len(lines)) + " on links to that page"

        # Still no good continuations? Look on the whole corpus
        if len(lines)==0:
            if len(lines) < num:
                more_lines = dbconn.randomLines(pages=None, options=options, brandom=False, num=(num-len(lines)))
                lines = lines + more_lines

        if continues:
            status["continuation_quality"] = i
        t.end("pos-search")
        # Still nothing? Maybe we should relax our constraints
        if len(lines)<10:
            if i!=0:
                options.pop(soft_constraints[i-1], None)
        else:

            # Make sure your choices have a reasonable number of rhymes
            t.begin("rhyme-count")
            if rhyme is None:
                rhyme_counts = map(lambda x:(dbconn.rhymeCountForRhyme(x['word'], x['rhyme_part'])), lines)
                total_rhymes = sum(rhyme_counts)
                total_rhymes = total_rhymes / len(lines)
                lines = filter(lambda x: dbconn.rhymeCountForRhyme(x['word'], x['rhyme_part']) >= total_rhymes/2, lines)
                lines = sorted(lines, key = lambda x: random.random() )

                ## lines = sorted(lines, key=lambda x: dbconn.rhymeCountForRhyme(x['word'], x['rhyme_part']), reverse=True)
                if debug_print:
                    for l in lines:
                        print l['line'] + " " + str(dbconn.rhymeCountForRhyme(l['word'], l['rhyme_part']))
                    print(" ")
            t.end("rhyme-count")
            break

    # if continues:
    #     lines.sort(key=lambda x: dbconn.continuationScoreForLine(x, poem[nindex]), reverse=True)

    if len(lines)==0:
        pdb.set_trace()
    # t.printTime()
    return (lines, status)

def chooseContinuation(lines, count, poem):
    if len(lines)==0:
        return None
    # lines = sorted(lines, key = lambda x: random.random() )
    return lines[0]
    print(" ")
    printPoemSoFar(poem)
    for i in range(min(len(lines), count)):
        print(str(i+1) + ": " + lines[i][3])
    i = input("Choose continuation:")
    return lines[i-1]

def printPoemSoFar(poem, statuses):
    print(" ")
    for i,p in enumerate(poem):
        if p:
            if "continuation_quality" in statuses[i]:
                print p['line'] + '\t\t\t\t' + "continuation quality " + str(statuses[i]["continuation_quality"]) + " of 5"
            else:
                print p['line']
    print(" ")

def safe(str_or_none):
    if str_or_none is None:
        return ''
    return str_or_none

def iPoem(pageID):
    dbconn = dbmanager.MySQLDatabaseConnection('wikisonnet', 'william', 'localhost', 'sh4kespeare')
    startpage = dbconn.pageTitleForPageID(pageID)

    print("Welcome to WikiBard")
    # o = urlparse.urlparse(startpage)
    # startpage = o.path.split('/')[-1]
    print("Composing a sonnet starting on " + startpage)
    print(" ")

    links = dbconn.pagesLinkedFromPageID(pageID)
    poem = [None for x in range(14)]
    alter = [None for x in range(14)]
    statuses = [None for x in range(14)]
    excludedLinesList = []
    previousWasEnd = True
    previousLine = None

    ## Stanzas
    for i in range(3):

        # First line
        # pdb.set_trace()
        idx = i*4
        (lines,status) = continuePoem(dbconn, pageID, links, previousLine=previousLine, starts=previousWasEnd, excludedLines=tuple(excludedLinesList))
        previousWasEnd = lines[0]['ends']
        statuses[idx] = status
        poem[idx] = lines[0]
        alter[idx] = lines
        excludedLinesList.append(lines[0]['id'])
        if debug_print:
            print lines[0]['line']

        # Second line
        idx = i*4+1
        excludedWord = poem[idx-1]['word']
        previousLine = poem[idx-1] if not previousWasEnd else None
        (lines,status) = continuePoem(dbconn, pageID, links, starts=previousWasEnd, previousLine=previousLine, excludedWord=excludedWord, excludedLines=tuple(excludedLinesList))
        statuses[idx] = status
        if len(lines)==0:
            printPoemSoFar(poem, statuses)
        poem[idx] = lines[0]
        alter[idx] = lines
        previousWasEnd = lines[0]['ends']
        excludedLinesList.append(lines[0]['id'])
        if debug_print:
            print lines[0]['line']

        # Third line
        idx = i*4+2
        excludedWord = poem[idx-2]['word']
        previousLine = poem[idx-1] if not previousWasEnd else None
        (lines,status) = continuePoem(dbconn, pageID, links, starts=previousWasEnd, previousLine=previousLine, rhyme=poem[idx-2]['rhyme_part'], excludedWord=excludedWord, excludedLines=tuple(excludedLinesList))
        statuses[idx] = status
        if len(lines)==0:
            printPoemSoFar(poem, statuses)
        poem[idx] = lines[0]
        alter[idx] = lines
        previousWasEnd = lines[0]['ends']
        excludedLinesList.append(lines[0]['id'])
        if debug_print:
            print lines[0]['line']

        # Last line
        idx = i*4+3
        excludedWord = poem[idx-2]['word']
        previousLine = poem[idx-1] if not previousWasEnd else None
        (lines,status) = continuePoem(dbconn, pageID, links, starts=previousWasEnd, ends=True, previousLine=previousLine, rhyme=poem[idx-2]['rhyme_part'], excludedWord=excludedWord, excludedLines=tuple(excludedLinesList))
        statuses[idx] = status
        if len(lines)==0:
            printPoemSoFar(poem, statuses)
        poem[idx] = lines[0]
        alter[idx] = lines
        previousWasEnd = lines[0]['ends']
        previousLine = lines[0] if not previousWasEnd else None
        excludedLinesList.append(lines[0]['id'])
        if debug_print:
            print lines[0]['line']

    # Ultimate line
    idx=13
    (lines,status) = continuePoem(dbconn, pageID, links, ends=True, starts=False, excludedLines=tuple(excludedLinesList))
    statuses[idx] = status
    poem[idx] =lines[0]
    alter[idx] = lines
    excludedLinesList.append(lines[0]['id'])
    if debug_print:
        print lines[0]['line']

    # Penultimate line
    idx=12
    excludedWord = poem[idx+1]['word']
    (lines,status) = continuePoem(dbconn, pageID, links, previousLine=None, nextLine=poem[idx+1], starts=previousWasEnd, ends=False, rhyme=poem[idx+1]['rhyme_part'], excludedWord=excludedWord, excludedLines=tuple(excludedLinesList))
    statuses[idx] = status
    poem[idx] = lines[0]
    alter[idx] = lines
    printPoemSoFar(poem, statuses)

    k = 1
    while 1:
        k = raw_input("q to quit:")
        if k == 'q':
            break
        elif len(k.split("-")) > 1:
            idx = int(k.split("-")[0])
            idx2 = int(k.split("-")[1])
            poem[idx] = alter[idx][idx2]
            printPoemSoFar(poem, statuses)
        elif k == 'db':
            pdb.set_trace()
        elif int(k) is not None:
            k = int(k)
            print safe(poem[k]['word_m2']) + " " + safe(poem[k]['word_m1']) +" "+ poem[k]['line'] +" "+ safe(poem[k]['word_len']) +" "+ safe(poem[k]['word_len_p1'])


def printRandomPoem():
    dbconn = dbmanager.DatabaseConnection('samtarakajian', 'samtarakajian', 'localhost', '')
    lines = []
    for i in range(12):
        if i%4 < 2:
            lines.append(dbconn.randomLines()[0])
        else:
            lines.append(dbconn.linesRhymingWithLine(lines[i-2])[0])
    penultimate = dbconn.randomLines()[0]
    ultimate = dbconn.linesRhymingWithLine(penultimate)[0]
    lines.append(penultimate)
    lines.append(ultimate)
    for line in lines:
        print(line[1])

def printContinuationPoem():
    compcount = 40
    dbconn = dbmanager.DatabaseConnection('samtarakajian', 'samtarakajian', 'localhost', '')
    lines = []
    for i in range(3):
        line = dbconn.randomLines()[0]
        print(line[1])
        nextLines = dbconn.randomLines(compcount)
        newlist = sorted(nextLines[:compcount], key=lambda x: wordutils.get_continuation_probability(line[1], x[1]), reverse=True)
        lineB = newlist[0]
        print("%d continuation options", len(nextLines))
        print(lineB[1])
        nextLines = dbconn.linesRhymingWithLine(line, compcount)
        newlist = sorted(nextLines[:compcount], key=lambda x: wordutils.get_continuation_probability(lineB[1], x[1]), reverse=True)
        lineC = newlist[0]
        print("%d continuation options", len(nextLines))
        print(lineC[1])
        nextLines = dbconn.linesRhymingWithLine(lineB, compcount)
        newlist = sorted(nextLines[:compcount], key=lambda x: wordutils.get_continuation_probability(lineC[1], x[1]), reverse=True)
        lineD = newlist[0]
        print("%d continuation options", len(nextLines))
        print(lineD[1])
    line = dbconn.randomLines()[0]
    print(line[1])
    nextLines = dbconn.linesRhymingWithLine(line, compcount)
    newlist = sorted(nextLines[:compcount], key=lambda x: wordutils.get_continuation_probability(line[1], x[1]), reverse=True)
    print("%d continuation options", len(nextLines))
    print(newlist[0][1])

def printLineWithContinuation(compcount=10):
    dbconn = dbmanager.DatabaseConnection('samtarakajian', 'samtarakajian', 'localhost', '')
    line = dbconn.randomLines()[0]
    nextLines = dbconn.randomLines(compcount)
    print(line[1])
    print("Possible continuations:")
    newlist = sorted(nextLines[:compcount], key=lambda x: wordutils.get_continuation_probability(line[1], x[1]), reverse=True)
