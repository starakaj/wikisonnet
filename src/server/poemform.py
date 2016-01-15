from random import randint, shuffle

poem_form_dicts = {
    'petrarchan' : {'scheme':'abba.abba.cde.cde',
                    'order':[0,1,2,3,4,5,6,7,8,9,10,11,12,13]},
    'elizabethan' : {'scheme':'abab.cdcd.efef.gg',
                    'order':[0,1,2,3,4,5,6,7,8,9,10,11,13,12]},
    'spenserian' : {'scheme':'abab.bcbc.cdcd.ee',
                    'order':[0,1,2,3,4,5,6,7,8,9,10,11,13,12]},
    'bespoke-1' : {'scheme':'aa.bb.cc.dd.efef.gg',
                    'order':[0,1,3,2,4,5,6,7,8,9,10,11,13,12]},
    'free': {'scheme':'abcd.efgh.ijkl.mn',
            'order':[0,1,2,3,4,5,6,7,8,9,10,11,13,12]}
}


class PoemFormLine:
    def __init__(self, index, rhyme, starts=False, ends=False):
        self.rhyme = rhyme
        self.starts = starts
        self.ends = ends
        self.index = index

class PoemForm:
    @staticmethod
    def NamedPoemForm(name):
        form_dict = poem_form_dicts[name]
        return PoemForm(form_dict['scheme'], form_dict['order'])

    def __init__(self, form_string, write_order):
        self.order = write_order
        self.lines = []
        idx=0
        for i, char in enumerate(form_string):
            if char == '.':
                continue
            starts = i==0 or form_string[i-1] == '.'
            ends = i==(len(form_string)-1) or form_string[i+1] == '.'
            self.lines.append(PoemFormLine(idx, char, starts, ends))
            idx += 1
        self.makeStanzas()

    def makeStanzas(self):
        ## Make start-end pairs
        starts = [i for (i, x) in enumerate(self.lines) if x.starts]
        ends = [i for (i, x) in enumerate(self.lines) if x.ends]

        ## Compute each stanza
        out_stanzas = []
        for i in range(len(starts)):
            s = starts[i]
            e = ends[i]
            stanza = [self.order[j] for j in range(s, e+1)]
            out_stanzas.append(stanza)

        self.stanzas = out_stanzas

    def setStanzaStart(self, stanza_idx, start_idx):
        stanza = self.stanzas[stanza_idx]
        s = min(stanza)
        e = max(stanza)
        r = s + start_idx
        out_stanza = []
        out_stanza.append(r)
        asc = iter(range(r+1, e+1))
        desc = iter(range(r-1, s-1, -1))
        poll = [0 for _ in range(r+1, e+1)] + [1 for _ in range(r-1, s-1, -1)]
        shuffle(poll)
        for p in poll:
            if p is 0:
                out_stanza.append(asc.next())
            else:
                out_stanza.append(desc.next())
        self.stanzas[stanza_idx] = out_stanza
        self.order = reduce(lambda x,y: x+y, self.stanzas)

    def scrambleOrder(self):
        for i in range(len(self.stanzas)):
            r = randint(0, len(self.stanzas[i])-1)
            self.setStanzaStart(i, r)

    def indexesOfRhymingLinesForIndex(self, line_index):
        rhyme_char = self.lines[line_index].rhyme
        return map(lambda x:x.index, filter(lambda l:l.rhyme == rhyme_char and l.index != line_index, self.lines))
