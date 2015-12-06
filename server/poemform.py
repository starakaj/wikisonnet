poem_form_dicts = {
    'petrarchan' : {'scheme':'abba.abba.cde.cde',
                    'order':[0,1,2,3,4,5,6,7,8,9,10,11,12,13]},
    'elizabethan' : {'scheme':'abab.cdcd.efef.gg',
                    'order':[0,1,2,3,4,5,6,7,8,9,10,11,13,12]},
    'spenserian' : {'scheme':'abab.bcbc.cdcd.ee',
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

    def indexesOfRhymingLinesForIndex(self, line_index):
        rhyme_char = self.lines[line_index].rhyme
        return map(lambda x:x.index, filter(lambda l:l.rhyme == rhyme_char and l.index != line_index, self.lines))
