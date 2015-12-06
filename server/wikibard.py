import dbconnect, dbreader
import urlparse
import pdb
import random
from benchmarking import Timer

optimized = False

n2p = {
    "pos_len_m1":"pos_m1",
    "pos_len":"pos_0",
    "pos_len_m2":"pos_m2",
    "pos_len_p1":"pos_1"
    }

p2n = {n2p[k]:k for k in n2p}

def initializeOptionsAndConstraints(options, previousLine=None, nextLine=None):
    pconstraints = ["pos_1", "pos_m2", "pos_0", "pos_m1"]
    nconstraints = [p2n[k] for k in pconstraints]

    if previousLine:
        for k in pconstraints:
            if previousLine[p2n[k]] is not None:
                options[k] = previousLine[p2n[k]]
        return (options, pconstraints)

    if nextLine:
        for k in nconstraints:
            if nextLine[n2p[k]] is not None:
                options[k] = nextLine[n2p[k]]
        return (options, nconstraints)
    return (options, [])

def continuePoem(dbconn, page, links, excludedWord=None, excludedLines=None, previousLine=None, nextLine=None, ends=False, starts=False, rhyme=None, debug_print=False, sloppy=False):

    status = {}
    lines = []
    num = 10

    if sloppy:
        previousLine = None
        nextLine = None
        rhyme = None

    if previousLine is not None and nextLine is not None:
        print "Double constrained lines are not supported"
        return None

    options = {"excludedLines":excludedLines, "starts":starts, "ends":ends}
    if rhyme is not None:
        options["rhyme"] = rhyme
    if excludedWord is not None:
        options["excludedWord"] = excludedWord
    (options, soft_constraints) = initializeOptionsAndConstraints(options, previousLine, nextLine)

    total_runs = len(soft_constraints)+1
    for _ in range(total_runs):
        lines = lines + dbreader.randomLines(dbconn, pages=(page,), predigested=False, options=options, brandom=False, optimized=optimized, num=(num-len(lines)))
        print dbconn.statement

        # If there are no good continuations on this page, look on pages connected to this page
        if len(lines) < num:
            more_lines = dbreader.randomLines(dbconn, pages=links, predigested=True, options=options, brandom=False, optimized=optimized, num=(num-len(lines)))
            lines = lines + more_lines
            print dbconn.statement

        # Still no good continuations? Look on the whole corpus
        if len(lines) < num:
            more_lines = dbreader.randomLines(dbconn, pages=None, options=options, brandom=False, optimized=optimized, num=(num-len(lines)))
            lines = lines + more_lines
            print dbconn.statement

        # Still nothing? Maybe we should relax our constraints
        if len(lines)<num and len(soft_constraints) is not 0:
            options.pop(soft_constraints[0], None)
            soft_constraints = soft_constraints[1:]
        else:
            if previousLine is not None or nextLine is not None:
                status["continuation_quality"] = len(soft_constraints)
            break

    if len(lines)==0:
        return None

    # Make sure your choices have a reasonable number of rhymes
    if rhyme is None:
        rhyme_counts = map(lambda x:(dbreader.rhymeCountForRhyme(dbconn, x['word'], x['rhyme_part'])), lines)
        total_rhymes = sum(rhyme_counts)
        total_rhymes = total_rhymes / len(lines)
        lines = filter(lambda x: dbreader.rhymeCountForRhyme(dbconn, x['word'], x['rhyme_part']) >= total_rhymes/2, lines)
        lines = sorted(lines, key = lambda x: random.random() )

    return (lines, status)

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

def compute_possible_lines(hard_constraints, flexible_constraints, search_groups, composed_lines):

    for i in range(flexible_constraints):
        this_flexible_constraints = flexible_constraints[:len(flexible_constraints)-i]
        for group in search_groups:
            possible_lines.append(fetch_possible_lines(hard_constraints, this_flexible_constraints, group, composed_lines))

        if possible_lines.count >= REQUIRED_POSSIBILITY_COUNT:
            break

    return possible_lines

def poemForPageID(pageID, dbconfig):
    dbconn = dbmanager.MySQLDatabaseConnection(dbconfig["database"], dbconfig["user"], dbconfig["host"], dbconfig["password"])

    ## Get the groups associated with a given page (perhaps construct table views for speed?)
    links = dbreader.pagesLinkedFromPageID(dbconn, pageID)

    composed_lines = []

    for line_plan in lines_to_write:
        hard_constraints = line_plan.hard_constraints
        flexible_constraints = line_plan.flexible_constraints
        search_groups = line_plan.search_groups
        possible_lines = compute_possible_lines(hard_constraints, flexible_constraints, search_groups, composed_lines)
        next_line = get_best_line(possible_lines)
        composed_lines[next_idx] = next_line

    return composed_lines


def iPoem(pageID, db, debug_print=False, sloppy=False):
    dbconn = dbmanager.MySQLDatabaseConnection(db["database"], db["user"], db["host"], db["password"])
    startpage = dbreader.pageTitleForPageID(dbconn, pageID)

    t = Timer()
    links = dbreader.pagesLinkedFromPageID(dbconn, pageID)
    poem = [None for x in range(14)]
    alter = [None for x in range(14)]
    statuses = [None for x in range(14)]
    excludedLinesList = []
    previousWasEnd = True
    previousLine = None

    ## Stanzas
    for i in range(3):

        # First line
        idx = i*4
        (lines,status) = continuePoem(dbconn, pageID, links, previousLine=previousLine, starts=previousWasEnd, excludedLines=tuple(excludedLinesList), debug_print=debug_print, sloppy=sloppy)
        previousWasEnd = lines[0]['ends']
        statuses[idx] = status
        poem[idx] = lines[0]
        alter[idx] = lines
        excludedLinesList.append(lines[0]['id'])
        if debug_print:
            print lines[0]['line']
            print " "

        # Second line
        idx = i*4+1
        excludedWord = poem[idx-1]['word']
        previousLine = poem[idx-1] if not previousWasEnd else None
        (lines,status) = continuePoem(dbconn, pageID, links, starts=previousWasEnd, previousLine=previousLine, excludedWord=excludedWord, excludedLines=tuple(excludedLinesList), debug_print=debug_print, sloppy=sloppy)
        statuses[idx] = status
        if len(lines)==0:
            printPoemSoFar(poem, statuses)
        poem[idx] = lines[0]
        alter[idx] = lines
        previousWasEnd = lines[0]['ends']
        excludedLinesList.append(lines[0]['id'])
        if debug_print:
            print lines[0]['line']
            print " "

        # Third line
        idx = i*4+2
        excludedWord = poem[idx-2]['word']
        previousLine = poem[idx-1] if not previousWasEnd else None
        (lines,status) = continuePoem(dbconn, pageID, links, starts=previousWasEnd, previousLine=previousLine, rhyme=poem[idx-2]['rhyme_part'], excludedWord=excludedWord, excludedLines=tuple(excludedLinesList), debug_print=debug_print, sloppy=sloppy)
        statuses[idx] = status
        if len(lines)==0:
            printPoemSoFar(poem, statuses)
        poem[idx] = lines[0]
        alter[idx] = lines
        previousWasEnd = lines[0]['ends']
        excludedLinesList.append(lines[0]['id'])
        if debug_print:
            print lines[0]['line']
            print " "

        # Last line
        idx = i*4+3
        excludedWord = poem[idx-2]['word']
        previousLine = poem[idx-1] if not previousWasEnd else None
        (lines,status) = continuePoem(dbconn, pageID, links, starts=previousWasEnd, ends=True, previousLine=previousLine, rhyme=poem[idx-2]['rhyme_part'], excludedWord=excludedWord, excludedLines=tuple(excludedLinesList), debug_print=debug_print, sloppy=sloppy)
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
            print " "

    # Ultimate line
    idx=13
    (lines,status) = continuePoem(dbconn, pageID, links, ends=True, starts=False, excludedLines=tuple(excludedLinesList), debug_print=debug_print, sloppy=sloppy)
    statuses[idx] = status
    poem[idx] =lines[0]
    alter[idx] = lines
    excludedLinesList.append(lines[0]['id'])
    if debug_print:
        print lines[0]['line']
        print " "

    # Penultimate line
    idx=12
    excludedWord = poem[idx+1]['word']
    (lines,status) = continuePoem(dbconn, pageID, links, previousLine=None, nextLine=poem[idx+1], starts=previousWasEnd, ends=False, rhyme=poem[idx+1]['rhyme_part'], excludedWord=excludedWord, excludedLines=tuple(excludedLinesList), debug_print=debug_print, sloppy=sloppy)
    statuses[idx] = status
    poem[idx] = lines[0]
    alter[idx] = lines
    if debug_print:
        print lines[0]['line']
        print " "


    if debug_print:
        print " "
        print "A sonnet on: " + startpage
        print " "
        print "Total runtime: " + str(t.elapsed())
        print " "
        for l in poem:
            print l['line']
    rlines = [[y['line'] for y in x] for x in alter]
    return rlines
    # return reduce(lambda a="", b="":a + "<p>" + b + "</p>", [x['line'] for x in poem])
