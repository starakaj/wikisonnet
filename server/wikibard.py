import dbconnect, dbreader
import urlparse
import pdb
import random
import poemform
import copy
from multiprocessing import Pool
from benchmarking import Timer
from tabulate import tabulate

optimized = True
REQUIRED_POSSIBILITY_COUNT = 20

n2p = {
    "pos_len_m1":"pos_m1",
    "pos_len":"pos_0",
    "pos_len_m2":"pos_m2",
    "pos_len_p1":"pos_1"
    }

p2n = {n2p[k]:k for k in n2p}

pconstraints = ["pos_1", "pos_m2", "pos_0", "pos_m1"]
nconstraints = [p2n[k] for k in pconstraints]

def makeStanzas(parallel_starts, parallel_ends, poem_form):
    ret_stanzas = []
    for l in parallel_starts:
        indexes = []
        while True:
            indexes.append(l.index)
            if l.ends:
                break
            l = poem_form.lines[l.index+1]
        ret_stanzas.append(indexes)
    for l in parallel_ends:
        indexes = []
        while True:
            indexes.append(l.index)
            if l.starts:
                break
            l = poem_form.lines[l.index-1]
        ret_stanzas.append(indexes)
    return ret_stanzas

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
    is_random = True or 'pageIDs' in group
    options={"num":num, "random":is_random, "optimized":optimized, "print_statement":False}
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
            new_lines = fetchPossibleLines(dbconn, search_constraints, group, composed_lines, num)
            for nl in new_lines:
                nl['group_level'] = search_groups.index(group)
                nl['constraint_fraction'] = float(i) / (len(flexible_constraints)+1)
            possible_lines += new_lines
            if len(possible_lines) >= REQUIRED_POSSIBILITY_COUNT:
                break
        if len(possible_lines) >= REQUIRED_POSSIBILITY_COUNT:
            break

    return possible_lines

def getBestLines(dbconn, hard_constraints, lines, poem_form, line_index, count=1):
    if 'rhyme_part' not in hard_constraints:
        rhyme_counts = map(lambda x:(dbreader.rhymeCountForRhyme(dbconn, x['word'], x['rhyme_part'])), lines)
        total_rhymes = sum(rhyme_counts)
        total_rhymes = total_rhymes / len(lines)
        lines = filter(lambda x: dbreader.rhymeCountForRhyme(dbconn, x['word'], x['rhyme_part']) >= total_rhymes/2, lines)

    ## Sort based on how you're likely to continue
    if (not poem_form.lines[line_index].starts and
        poem_form.order[line_index] < poem_form.order[line_index-1]
        ):
        lines = sorted(lines, key = lambda x: dbreader.posCountsForLine(dbconn, x, 'leading'), reverse=True )
    elif (not poem_form.lines[line_index].ends and
        poem_form.order[line_index] < poem_form.order[line_index+1]
        ):
        lines = sorted(lines, key = lambda x: dbreader.posCountsForLine(dbconn, x, 'lagging'), reverse=True )
    else:
        lines = sorted(lines, key = lambda x: random.random())
    return lines[:count]

def composeLinesAtIndexes(pageID, poem_form, dbconfig, search_groups, composed_lines, indexes):
    dbconn = dbconnect.MySQLDatabaseConnection(dbconfig["database"], dbconfig["user"], dbconfig["host"], dbconfig["password"])
    ret_composed_lines = copy.deepcopy(composed_lines)
    for idx in indexes:
        if ret_composed_lines[idx] is None:
            hard_constraints = hardConstraints(idx, poem_form, ret_composed_lines)
            flexible_constraints = flexibleConstraints(idx, poem_form, ret_composed_lines)
            possible_lines = computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, ret_composed_lines)
            next_lines = getBestLines(dbconn, hard_constraints, possible_lines, poem_form, idx, 1)
            ret_composed_lines[idx] = next_lines[0]
    dbconn.close()
    return ret_composed_lines

def poemForPageID(pageID, sonnet_form_name, dbconfig, multi=False):
    dbconn = dbconnect.MySQLDatabaseConnection(dbconfig["database"], dbconfig["user"], dbconfig["host"], dbconfig["password"])

    ## Decide what kind of poem you're going to write
    poem_form = poemform.PoemForm.NamedPoemForm(sonnet_form_name)

    ## Follow the redirect, if you need to
    pageID = dbreader.followRedirectForPageID(dbconn, pageID)

    ## Get the groups associated with a given page (perhaps construct table views for speed?)
    search_groups = [{'pageIDs':[pageID]},
                    # {'pageIDs':dbreader.pagesLinkedFromPageID(dbconn, pageID)},
                    {'page_minor_category':dbreader.categoryForPageID(dbconn, pageID, 'minor')},
                    {'page_major_category':dbreader.categoryForPageID(dbconn, pageID, 'major')},
                    {}] ## This 'none' group will search through the entire corpus

    composed_lines = [None for _ in poem_form.lines]

    ## For parallelization, separate out each stanza
    starting_lines = [x for x in poem_form.lines if x.starts]
    ending_lines = [x for x in poem_form.lines if x.ends]
    parallel_starts = [x for i,x in enumerate(starting_lines) if poem_form.order[ending_lines[i].index] > poem_form.order[x.index]]
    parallel_ends = [x for i,x in enumerate(ending_lines) if poem_form.order[starting_lines[i].index] > poem_form.order[x.index]]

    ## Compose all the parallelizable starting lines at once
    if parallel_starts:
        idx = parallel_starts[0].index
        hard_constraints = hardConstraints(idx, poem_form, composed_lines)
        flexible_constraints = flexibleConstraints(idx, poem_form, composed_lines)
        possible_lines = computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, composed_lines)
        starting_lines = getBestLines(dbconn, hard_constraints, possible_lines, poem_form, idx, len(parallel_starts))
        for i,l in enumerate(starting_lines):
            composed_lines[parallel_starts[i].index] = l

    ## Do the same for all of the parallelizable ending lines
    if parallel_ends:
        idx = parallel_ends[0].index
        hard_constraints = hardConstraints(idx, poem_form, composed_lines)
        flexible_constraints = flexibleConstraints(idx, poem_form, composed_lines)
        possible_lines = computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, composed_lines)
        ending_lines = getBestLines(dbconn, hard_constraints, possible_lines, poem_form, idx, len(parallel_ends))
        for i,l in enumerate(ending_lines):
            composed_lines[parallel_ends[i].index] = l

    ## Now compose each stanza in parallel
    stanzas = makeStanzas(parallel_starts, parallel_ends, poem_form)

    if multi:
        pool = Pool(processes=4)
        pp = [pool.apply_async(composeLinesAtIndexes, args=(pageID, poem_form, dbconfig, search_groups, composed_lines, x)) for x in stanzas]
        poem_pieces = [p.get() for p in pp];
    else:
        poem_pieces = [composeLinesAtIndexes(pageID, poem_form, dbconfig, search_groups, composed_lines, x) for x in stanzas]

    ## Piece the results back together
    for i,l in enumerate(composed_lines):
        if l is None:
            for p in poem_pieces:
                if p[i] is not None:
                    composed_lines[i] = p[i]
                    break
    dbconn.close()
    return composed_lines

def addTextToLines(dbconn, lines):
    for line in lines:
        line_text = dbreader.textForLineID(dbconn, line['id'])
        line['text'] = line_text

def printPoemLinesTable(lines, keys=["id", "text"]):
    line_dicts = [{k:line[k] for k in keys} for line in lines]
    print tabulate(line_dicts)

def posStringForPoemLines(lines):
    pos_per_line = []
    ordered_pos_keys = ['pos_m2', 'pos_m1', 'pos_0', 'pos_1', 'pos_len_m2', 'pos_len_m1', 'pos_len', 'pos_len_p1']
    for line in lines:
        line_pos = [line[x] for x in ordered_pos_keys]
        line_pos = ['None' if x is None else x for x in line_pos]
        pos_per_line.append("\t".join(line_pos))
    return '\n'.join(pos_per_line)
