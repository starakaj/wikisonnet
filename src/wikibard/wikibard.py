from util.benchmarking import Timer
from db import dbconnect, dbreader
import urlparse
import pdb
import random
import poemform
import copy
from multiprocessing import Pool, Manager
from tabulate import tabulate
import os

optimized = True
use_model = False
if use_model:
    from nltk import word_tokenize
    import gensim
    model_path = '/Users/samtarakajian/Documents/wikisonnet/word2vec/wiki-latest.en.model'
REQUIRED_POSSIBILITY_COUNT = 16

n2p = {
    "pos_len_m1":"pos_m1",
    "pos_len":"pos_0",
    "pos_len_m2":"pos_m2",
    "pos_len_p1":"pos_1"
    }

p2n = {n2p[k]:k for k in n2p}

pconstraints = ["pos_1", "pos_m2", "pos_0", "pos_m1"]
nconstraints = [p2n[k] for k in pconstraints]
rname = None

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
    xcl_lines = [l['id'] for l in composed_lines if l is not None]
    if xcl_lines:
        hc.append({'excluded_lines':xcl_lines})
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
            if pline['pos_len'] == 'IN':
                fc.insert(2, {'word_0':pline['word_len']})
            if pline['pos_len_m1'] == 'IN':
                fc.insert(2, {'word_m1':pline['word_len_m1']})

    if not poem_form.lines[line_index].ends:
        if completed_lines[line_index+1] is not None:
            nline = completed_lines[line_index+1]
            for c in nconstraints:
                fc.append({c:nline[n2p[c]]})
    return fc

def fetchPossibleLines(dbconn, search_constraints, group, composed_lines, view_constraints=None, temp_view_name='tv', num=REQUIRED_POSSIBILITY_COUNT):
    is_random = True or 'pageIDs' in group or 'minor_category' in group or 'major_category' in group
    options={"num":num, "random":is_random, "optimized":optimized, "print_statement":False, "subquery":False}
    if view_constraints:
        options['view_name'] = temp_view_name;
        options['view_constraints'] = view_constraints;
    return dbreader.searchForLines(dbconn, group, search_constraints, options)

def computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, composed_lines, possibility_count=REQUIRED_POSSIBILITY_COUNT):

    possible_lines = []

    ## Create a view for this round of flexible constraints
    tv_name = "tv_{}".format(int(random.getrandbits(128)))
    view_constraints = {}
    for d in hard_constraints: ## + this_flexible_constraints:
        for k in d:
            view_constraints[k] = d[k]
    dbreader.createViewForConstraints(dbconn, view_constraints, tv_name)

    for i in range(len(flexible_constraints)+1):
        ## Get constraints dict for the current round
        this_flexible_constraints = flexible_constraints[i:]
        search_constraints = {}
        for d in hard_constraints+this_flexible_constraints:
            for k in d:
                search_constraints[k] = d[k]

        for group in search_groups:
            num = possibility_count - len(possible_lines)
            new_lines = fetchPossibleLines(dbconn, search_constraints, group, composed_lines, view_constraints, tv_name, num)
            for nl in new_lines:
                nl['group_level'] = search_groups.index(group)
                nl['constraint_fraction'] = float(i) / (len(flexible_constraints)+1)
            possible_lines += new_lines
            if len(possible_lines) >= possibility_count:
                break

        if len(possible_lines) >= possibility_count:
            break

    dbreader.clearView(dbconn, tv_name)

    return possible_lines

def getBestLines(dbconn, hard_constraints, lines, poem_form, line_index, previous_line=None, count=1):
    if len(lines) == 0:
        return []
    if 'rhyme_part' not in hard_constraints:
        rhyme_counts = map(lambda x:(dbreader.rhymeCountForRhyme(dbconn, x['word'], x['rhyme_part'])), lines)
        total_rhymes = sum(rhyme_counts)
        total_rhymes = total_rhymes / len(lines)
        lines = filter(lambda x: dbreader.rhymeCountForRhyme(dbconn, x['word'], x['rhyme_part']) >= total_rhymes/2, lines)

    ## Sort based on how you're likely to continue
    if (not poem_form.lines[line_index].starts and
        poem_form.order[line_index] < poem_form.order[line_index-1] and
        count is 1
        ):
        lines = sorted(lines, key = lambda x: dbreader.posCountsForLine(dbconn, x, 'leading'), reverse=True )
    elif (not poem_form.lines[line_index].ends and
        poem_form.order[line_index] < poem_form.order[line_index+1] and
        count is 1
        ):
        lines = sorted(lines, key = lambda x: dbreader.posCountsForLine(dbconn, x, 'lagging'), reverse=True )
    else:
        lines = sorted(lines, key = lambda x: random.random())

    ## Finally, sort within the sort by how close to the original page the lines are
    lines = sorted(lines, key = lambda x: x['group_level'])

    return lines[:count]

def composeLinesAtIndexes(pageID, poem_form, dbconfig, search_groups, composed_lines, indexes, callback=None, user_info=None):
    dbconn = dbconnect.MySQLDatabaseConnection(dbconfig["database"], dbconfig["user"], dbconfig["host"], dbconfig["password"])
    ret_composed_lines = copy.deepcopy(composed_lines)
    for idx in indexes:
        if ret_composed_lines[idx] is None:
            hard_constraints = hardConstraints(idx, poem_form, composed_lines)

            # print ("pid: " + str(os.getpid()))
            # for l in composed_lines:
            #     print ("\t" + str(l))
            # print ("\n")

            flexible_constraints = flexibleConstraints(idx, poem_form, ret_composed_lines)
            possible_lines = computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, ret_composed_lines)
            previous_line = None
            if not poem_form.lines[idx].starts and ret_composed_lines[idx-1]:
                previous_line = dbreader.textForLineID(dbconn, ret_composed_lines[idx-1]['id'])
            next_lines = getBestLines(dbconn, hard_constraints, possible_lines, poem_form, idx, previous_line=previous_line, count=1)
            if len(next_lines) > 0:
                ret_composed_lines[idx] = next_lines[0]
                composed_lines[idx] = ret_composed_lines[idx]
            if callback is not None:
                callback(ret_composed_lines, user_info)
    dbconn.close()
    return ret_composed_lines

def poemForPageID(pageID, sonnet_form_name, dbconfig, multi=False, output_queue=None, callback=None, user_info=None):
    dbconn = dbconnect.MySQLDatabaseConnection(dbconfig["database"], dbconfig["user"], dbconfig["host"], dbconfig["password"])

    ## Decide what kind of poem you're going to write
    poem_form = poemform.PoemForm.NamedPoemForm(sonnet_form_name)
    # poem_form.scrambleOrder()

    ## Follow the redirect, if you need to
    pageID = dbreader.followRedirectForPageID(dbconn, pageID)

    ## Get the groups associated with a given page (perhaps construct table views for speed?)
    search_groups = [{'pageIDs':[pageID]},
                    {'pageIDs':dbreader.pagesLinkedFromPageID(dbconn, pageID), 'page_major_category':dbreader.categoryForPageID(dbconn, pageID, 'major')},
                    # {'line_minor_category':dbreader.categoryForPageID(dbconn, pageID, 'minor')},
                    # {'line_major_category':dbreader.categoryForPageID(dbconn, pageID, 'major')},
                    {'page_minor_category':dbreader.categoryForPageID(dbconn, pageID, 'minor')},
                    {'page_major_category':dbreader.categoryForPageID(dbconn, pageID, 'major')},
                    {}] ## This 'none' group will search through the entire corpus

    composed_lines = [None for _ in poem_form.lines]

    ## First, get random lines from the starting page itself (if any exist)
    hard_constraints = []
    flexible_constraints = []
    possible_lines = computePossibleLines(dbconn, hard_constraints, flexible_constraints, [{'pageIDs':[pageID]}], composed_lines, possibility_count=20)
    filled_stanzas = []
    if (possible_lines):
        source_lines = getBestLines(dbconn, hard_constraints, possible_lines, poem_form, 0, count=10)
        source_lines = filter(lambda x: dbreader.rhymeCountForRhyme(dbconn, x['word'], x['rhyme_part']) > 0, source_lines)
        order = range(len(poem_form.stanzas))
        random.shuffle(order)
        for o in order:
            s = poem_form.stanzas[o]
            if len(s) > 2:
                valid_indexes = range(len(source_lines))
            else:
                valid_indexes = [i for i in range(len(source_lines)) if source_lines[i]['starts'] or source_lines[i]['ends']]
            if valid_indexes:
                random.shuffle(valid_indexes)
                line = source_lines.pop(valid_indexes[0])
                if line['starts']:
                    r = min(s)
                    composed_lines[min(s)] = line
                elif line['ends']:
                    r = max(s)
                    composed_lines[max(s)] = line
                else:
                    r = random.randint(min(s)+1,max(s)-1)
                    composed_lines[r] = line
                poem_form.setStanzaStart(o, r-min(s))
                filled_stanzas.append(o)

    ## For parallelization, separate out each stanza
    unfilled_stanzas = [poem_form.stanzas[i] for i in range(len(poem_form.stanzas)) if i not in filled_stanzas]
    starting_lines = [poem_form.lines[i[0]] for i in unfilled_stanzas]
    parallel_starts = [s for s in starting_lines if s.starts]
    parallel_ends = [s for s in starting_lines if s.ends]
    parallel_mids = [s for s in starting_lines if not s.ends and not s.starts]

    ## Compose all the parallelizable starting lines at once
    if parallel_starts:
        idx = parallel_starts[0].index
        hard_constraints = hardConstraints(idx, poem_form, composed_lines)
        flexible_constraints = flexibleConstraints(idx, poem_form, composed_lines)
        possible_lines = computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, composed_lines, possibility_count=20)
        starting_lines = getBestLines(dbconn, hard_constraints, possible_lines, poem_form, idx, count=10)
        starting_lines = random.sample(starting_lines, len(parallel_starts))
        for i,l in enumerate(starting_lines):
            composed_lines[parallel_starts[i].index] = l

    ## Do the same for all of the parallelizable ending lines
    if parallel_ends:
        idx = parallel_ends[0].index
        hard_constraints = hardConstraints(idx, poem_form, composed_lines)
        flexible_constraints = flexibleConstraints(idx, poem_form, composed_lines)
        possible_lines = computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, composed_lines, possibility_count=10)
        ending_lines = getBestLines(dbconn, hard_constraints, possible_lines, poem_form, idx, count=10)
        ending_lines = random.sample(ending_lines, len(parallel_ends))
        for i,l in enumerate(ending_lines):
            composed_lines[parallel_ends[i].index] = l

    ## Finally, same for all the parallelizable mid lines
    if parallel_mids:
        idx = parallel_mids[0].index
        hard_constraints = hardConstraints(idx, poem_form, composed_lines)
        flexible_constraints = flexibleConstraints(idx, poem_form, composed_lines)
        possible_lines = computePossibleLines(dbconn, hard_constraints, flexible_constraints, search_groups, composed_lines, possibility_count=10)
        midding_lines = getBestLines(dbconn, hard_constraints, possible_lines, poem_form, idx, count=10)
        midding_lines = random.sample(midding_lines, len(parallel_mids))
        for i,l in enumerate(midding_lines):
            composed_lines[parallel_mids[i].index] = l

    ## If there's a callback, call it
    if callback is not None:
        callback(composed_lines, user_info)

    ## Now compose each stanza in parallel
    stanzas = poem_form.stanzas

    dbconn.close()

    if output_queue is not None:
        manager = Manager()
        managed_composed_lines = manager.list(composed_lines)
        for x in stanzas:
            output_queue.put((composeLinesAtIndexes, (pageID, poem_form, dbconfig, search_groups, managed_composed_lines, x, callback, user_info), callback, user_info))
        return

    if multi:
        manager = Manager()
        managed_composed_lines = manager.list(composed_lines)
        pool = Pool(processes=4)
        pp = [pool.apply_async(composeLinesAtIndexes, args=(pageID, poem_form, dbconfig, search_groups, managed_composed_lines, x)) for x in stanzas]
        poem_pieces = [p.get() for p in pp];
        pool.close()
        pool.join()
    else:
        poem_pieces = [composeLinesAtIndexes(pageID, poem_form, dbconfig, search_groups, composed_lines, x) for x in stanzas]

    ## Piece the results back together
    for i,l in enumerate(composed_lines):
        if l is None:
            for p in poem_pieces:
                if p[i] is not None:
                    composed_lines[i] = p[i]
                    break
    return composed_lines

def addTextToLines(dbconn, lines):
    for line in lines:
        line_text = dbreader.textForLineID(dbconn, line['id'])
        line['text'] = line_text

def printPoemLinesTable(lines, keys=["id", "text"]):
    table_rows = [[line[k] for k in keys] for line in lines]
    print tabulate(table_rows, headers=keys)

def posStringForPoemLines(lines):
    pos_per_line = []
    ordered_pos_keys = ['pos_m2', 'pos_m1', 'pos_0', 'pos_1', 'pos_len_m2', 'pos_len_m1', 'pos_len', 'pos_len_p1']
    for line in lines:
        line_pos = [line[x] for x in ordered_pos_keys]
        line_pos = ['None' if x is None else x for x in line_pos]
        pos_per_line.append("\t".join(line_pos))
    return '\n'.join(pos_per_line)
