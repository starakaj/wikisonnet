import dbmanager
import wordutils
import urlparse

def continuePoem(poem, dbconn, page, links, continues=False, ends=False, starts=False, rhymeidx=None):
    nindex = len(poem)-1

    num = 1
    if continues:
        num = 100
    rhyme = None
    if not rhymeidx==None:
        rhyme = poem[rhymeidx][2]

    lines = dbconn.randomLines(pages=(page,), predigested=False, starts=starts, ends=ends, brandom=True, num=num, rhyme=rhyme)
    if len(lines) < num:
        more_lines = dbconn.randomLines(pages=links, predigested=True, starts=starts, ends=ends, brandom=True, num=(num-len(lines)), rhyme=rhyme)
        lines = lines + more_lines
    if len(lines) < num:
        more_lines = dbconn.randomLines(pages=None, starts=starts, ends=ends, brandom=True, num=(num-len(lines)), rhyme=rhyme)
        lines = lines + more_lines
    if len(lines) < num:
        more_lines = dbconn.randomLines(pages=None, starts=starts, ends=False, brandom=True, num=(num-len(lines)), rhyme=rhyme)
        lines = lines + more_lines

    if continues:
        lines.sort(key=lambda x: dbconn.continuationScoreForLine(x, poem[nindex]), reverse=True)

    return lines

def chooseContinuation(lines, count):
    print(" ")
    for i in range(count):
        print(str(i+1) + ": " + lines[i][3])
    i = input("Choose continuation:")
    return lines[i-1]

def printPoemSoFar(poem):
    print(" ")
    for p in poem:
        if p:
            print p[3]
    print(" ")

def iPoem(startpage):
    print("Welcome to WikiBard")
    o = urlparse.urlparse(startpage)
    dest = o.path.split('/')[-1]
    print("Composing a sonnet starting on " + dest)
    print(" ")

    dbconn = dbmanager.DatabaseConnection('samtarakajian', 'samtarakajian', 'localhost', '')
    links = dbconn.pagesLinkedFromPage(startpage)
    poem = []

    ## Stanzas
    for i in range(3):
        lines = continuePoem(poem, dbconn, startpage, links, starts=True)
        poem.append(lines[0])

        lines = continuePoem(poem, dbconn, startpage, links, continues=True)
        # line = chooseContinuation(lines, 3)
        line = lines[0]
        poem.append(line)

        lines = continuePoem(poem, dbconn, startpage, links, continues=True, rhymeidx=(i*4))
        # line = chooseContinuation(lines, 3)
        line = lines[0]
        poem.append(line)

        lines = continuePoem(poem, dbconn, startpage, links, continues=True, ends=True, rhymeidx=(i*4+1))
        # line = chooseContinuation(lines, 3)
        line = lines[0]
        poem.append(line)

    lines = continuePoem(poem, dbconn, startpage, links, starts=True)
    poem.append(lines[0])
    lines = continuePoem(poem, dbconn, startpage, links, continues=True, ends=True, rhymeidx=12)
    poem.append(lines[0])
    printPoemSoFar(poem)

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
