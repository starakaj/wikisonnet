
poem_form_strings = {
    'elizabethan' : {'scheme':'abab.cdcd.efef.gg',
                    'order'=[0,1,2,3,7,6,5,4,8,9,10,11,13,12]}
}

class PoemFormLine:
    def __init__(self, index, rhyme, starts=False, ends=False):
        self.rhyme = rhyme
        self.starts = starts
        self.ends = ends

class PoemForm:
    @staticmethod
    def IambicPoemForm():
        return PoemForm(poem_form_strings['elizabethan'])

    def __init__(self, form_string, write_order):
        self.order = write_order
        self.lines = []
        for i, char in enumerate(form_string):
            if char == '.':
                continue
            starts = i==0 or form_string[i-1] == '.'
            ends = i==(len(form_string)-1) or form_string[i+1] == '.'
            self.lines.append(PoemFormLine(i, char, starts, ends))

    def indexesOfRhymingLinesForIndex(line_index):
        rhyme_char = self.lines[line_index].rhyme
        return map(lambda x:x.index, filter(lambda l:l.rhyme = rhyme_char, self.lines))
