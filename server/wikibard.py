import dbconnect, dbreader
import urlparse
import pdb
import random
import poemform
from benchmarking import Timer

optimized = False
REQUIRED_POSSIBILITY_COUNT = 10

n2p = {
    "pos_len_m1":"pos_m1",
    "pos_len":"pos_0",
    "pos_len_m2":"pos_m2",
    "pos_len_p1":"pos_1"
    }

p2n = {n2p[k]:k for k in n2p}

pconstraints = ["pos_1", "pos_m2", "pos_0", "pos_m1"]
nconstraints = [p2n[k] for k in pconstraints]

def initializeOptionsAndConstraints(options, previousLine=None, nextLine=None):
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

def hardConstraints(line_index, poem_form, composed_lines):
    hc = []
    hc.append({'starts':poem_form.lines[line_index].starts})
    hc.append({'ends':poem_form.lines[line_index].ends})
    rhyming_indexes = poem_form.indexesOfRhymingLinesForIndex(line_index)
    if rhyming_indexes:
        if composed_lines[rhyming_indexes[0]] is not None:
            hc.append({'rhyme_part':composed_lines[rhyming_indexes[0]]['rhyme_part']})
            hc.append({'excluded_word':composed_lines[rhyming_indexes[0]]['word']})
    return hc

def flexibleConstraints(line_index, poem_form, completed_lines):
    fc = []
    if not poem_form.lines[line_index].starts:
        if completed_lines[line_index-1] is not None:
            pline = completed_lines[line_index-1]
            for c in pconstraints:
                fc.append({c:pline[p2n[c]]})
    if not poem_form.lines[line_index].ends:
        if completed_lines[line_index+1] is not None:
            nline = completed_lines[line_index+1]
            for c in nconstraints:
                fc.append({c:nline[n2p[c]]})
    return fc

def fetchPossibleLines(dbconn, search_constraints, group, composed_lines, num=REQUIRED_POSSIBILITY_COUNT):
    options={"num":num, "random":False, "optimized":True, "print_statement":True}
    return dbreader.searchForLines(dbconn, group, search_constraints, options)

def computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, composed_lines):

    possible_lines = []

    for i in range(len(flexible_constraints)+1):

        ## Get constraints dict for the current round
        this_flexible_constraints = flexible_constraints[i:]
        search_constraints = {}
        for d in hard_constraints+this_flexible_constraints:
            for k in d:
                search_constraints[k] = d[k]

        for group in search_groups:
            num = REQUIRED_POSSIBILITY_COUNT - len(possible_lines)
            possible_lines += fetchPossibleLines(dbconn, search_constraints, group, composed_lines, num)
            if len(possible_lines) >= REQUIRED_POSSIBILITY_COUNT:
                break
            print possible_lines
            print " "
        if len(possible_lines) >= REQUIRED_POSSIBILITY_COUNT:
            break

    return possible_lines

def getBestLine(dbconn, hard_constraints, lines):
    if 'rhyme_part' not in hard_constraints:
        rhyme_counts = map(lambda x:(dbreader.rhymeCountForRhyme(dbconn, x['word'], x['rhyme_part'])), lines)
        total_rhymes = sum(rhyme_counts)
        total_rhymes = total_rhymes / len(lines)
        lines = filter(lambda x: dbreader.rhymeCountForRhyme(dbconn, x['word'], x['rhyme_part']) >= total_rhymes/2, lines)
        lines = sorted(lines, key = lambda x: random.random() )
    return lines[0]

def poemForPageID(pageID, sonnet_form_name, dbconfig):
    dbconn = dbconnect.MySQLDatabaseConnection(dbconfig["database"], dbconfig["user"], dbconfig["host"], dbconfig["password"])

    ## Decide what kind of poem you're going to write
    poem_form = poemform.PoemForm.NamedPoemForm(sonnet_form_name)

    ## Follow the redirect, if you need to
    pageID = dbreader.followRedirectForPageID(dbconn, pageID)

    ## Get the groups associated with a given page (perhaps construct table views for speed?)
    search_groups = [{'pageIDs':[pageID]},
                    {'pageIDs':dbreader.pagesLinkedFromPageID(dbconn, pageID)},
                    {'minor_category':dbreader.categoryForPageID(dbconn, pageID, 'minor')},
                    {'major_category':dbreader.categoryForPageID(dbconn, pageID, 'major')},
                    {}] ## This 'none' group will search through the entire corpus

    composed_lines = [None for _ in poem_form.lines]

    for i,_ in enumerate(poem_form.lines):
        idx = poem_form.order[i]
        hard_constraints = hardConstraints(idx, poem_form, composed_lines)
        flexible_constraints = flexibleConstraints(idx, poem_form, composed_lines)
        possible_lines = computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, composed_lines)
        next_line = getBestLine(dbconn, hard_constraints, possible_lines)
        composed_lines[idx] = next_line

    return composed_lines

def poemStringForPoemLines(dbconn, lines):
    texts = []
    for line in lines:
        line_text = dbreader.textForLineID(dbconn, line['id'])
        texts.append(line_text)
    return "\n".join(texts)
