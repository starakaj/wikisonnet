import dbmanager
import wordutils

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
