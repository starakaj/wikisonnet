import random
import re
import nltk
import operator
from nltk.util import ngrams
from nltk.corpus import cmudict
from nltk.probability import LidstoneProbDist

e = cmudict.entries()
d = cmudict.dict()

banned_end_words = ['the', 'a', 'an', 'at', 'been', 'in', 'of', 'to', 'by', 'my',
                    'too', 'not', 'and', 'but', 'or', 'than', 'then', 'no', 'o',
                    'for', 'so', 'which', 'their', 'on', 'your', 'as', 'has',
                    'what', 'is', 'nor', 'i']

## blocks = re.split('\n+', testtext)

def sylcount(s):
	try:
		d[s]
	except KeyError:
		return None
	else:
		if len(d[s]) <= 1:
			sj = ''.join(d[s][0])
			sl = re.split('0|1|2', sj)
			return len(sl) - 1
		else:
			sj0 = ''.join(d[s][0])
			sl0 = re.split('0|1|2', sj0)
			sj1 = ''.join(d[s][1])
			sl1 = re.split('0|1|2', sj1)
			if len(sl1) < len(sl0):
				return len(sl1) - 1
			else:
				return len(sl0) - 1


def line_sylcount(line):
	count = 0
	for word in line:
		count += sylcount(word)
	return count


def meter(word):
	pron = d[word]
	m1 = []
	m2 = []
	mx = []
	if len(pron) == 1:
		for i in pron[0]:
			if '0' in i:
				m1.append(0)
			elif '1' in i:
				m1.append(1)
			elif '2' in i:
				m1.append(2)
			else:
				pass
		mx = [m1]
	elif len(pron) >= 2:
		for i in pron[0]:
			if '0' in i:
				m1.append(0)
			elif '1' in i:
				m1.append(1)
			elif '2' in i:
				m1.append(2)
			else:
				pass
		for i in pron[1]:
			if '0' in i:
				m2.append(0)
			elif '1' in i:
				m2.append(1)
			elif '2' in i:
				m2.append(2)
			else:
				pass
		mx = [m1, m2]
	m = []
	if len(mx) == 1:
		w0 = reduce(operator.mul, mx[0], 1)
		if w0 >= 2:
			for i in mx[0]:
				if i == 1:
					m.append('u')
				elif i == 2:
					m.append('s')
		elif w0 == 1:
			for i in mx[0]:
				m.append('s')
		elif w0 == 0:
			for i in mx[0]:
				if i == 0:
					m.append('u')
				elif i == 1 or i == 2:
					m.append('s')
	elif len(mx) == 2:
		w0 = reduce(operator.mul, mx[0], 1)
		w1 = reduce(operator.mul, mx[1], 1)
		if w0 >= 2 and w1 >= 2:
			for (i, j) in zip(mx[0], mx[1]):
				if i * j == 1:
					m.append('u')
				elif i * j == 4:
					m.append('s')
				elif i * j == 2:
					m.append('x')
		elif w0 == 1 and w1 == 1:
			for (i, j) in zip(mx[0], mx[1]):
				m.append('s')
		elif w0 == 0 and w1 == 0:
			for (i, j) in zip(mx[0], mx[1]):
				if i == j and i * j >= 1:
					m.append('s')
				elif i != j and i * j == 0:
					m.append('x')
				elif i == j and i * j == 0:
					m.append('u')
		elif w0 >= 2 and w1 == 0:
			for (i, j) in zip(mx[0], mx[1]):
				if i == 1 and j == 0:
					m.append('u')
				elif i == 2 and j == 0:
					m.append('x')
				elif i == 1 and j == 1:
					m.append('x')
				elif i == 1 and j == 2:
					m.append('x')
				elif i == 2 and j == 1:
					m.append('s')
				elif i == 2 and j == 2:
					m.append('s')
		elif w0 == 0 and w1 >= 2:
			for (i, j) in zip(mx[0], mx[1]):
				if i == 0 and j == 1:
					m.append('u')
				elif i == 0 and j == 2:
					m.append('x')
				elif i == 1 and j == 1:
					m.append('x')
				elif i == 2 and j == 1:
					m.append('x')
				elif i == 1 and j == 2:
					m.append('s')
				elif i == 2 and j == 2:
					m.append('s')
		elif w0 == 1 and w1 >= 2:
			for (i, j) in zip(mx[0], mx[1]):
				if j == 1:
					m.append('x')
				elif j == 2:
					m.append('s')
		elif w0 >= 2 and w1 == 1:
			for (i, j) in zip(mx[0], mx[1]):
				if i == 1:
					m.append('x')
				elif i == 2:
					m.append('s')
		elif w0 == 1 and w1 == 0:
			for (i, j) in zip(mx[0], mx[1]):
				if j == 0:
					m.append('x')
				elif j == 1:
					m.append('s')
				elif j == 2:
					m.append('s')
		elif w0 == 0 and w1 == 1:
			for (i, j) in zip(mx[0], mx[1]):
				if i == 0:
					m.append('x')
				if i == 1:
					m.append('s')
				if i == 2:
					m.append('s')
	return m


def strip_numbers(x):
	xj = '.'.join(x)
	xl = re.split('0|1|2', xj)
	xjx = ''.join(xl)
	xlx = xjx.split('.')
	return xlx


def last_stressed_vowel(word):
	if len(d[word]) <= 1:
		pron = d[word][0]
	else:
		p0 = d[word][0]
		p1 = d[word][1]
		sj0 = ''.join(p0)
		sl0 = re.split('0|1|2', sj0)
		sj1 = ''.join(p1)
		sl1 = re.split('0|1|2', sj1)
		if len(sl1) < len(sl0):
			pron = p1
		else:
			pron = p0
	mtr = meter(word)
	vowel_index = []
	if len(mtr) == 1:
		lsv = -1
	elif mtr[-1] == 's' or mtr[-1] == 'x':
		lsv = -1
	elif mtr[-2] == 's' or mtr[-3] == 'x':
		lsv = -2
	elif mtr[-3] == 's' or mtr[-3] == 'x':
		lsv = -3
	elif mtr[-4] == 's' or mtr[-4] == 'x':
		lsv = -4
	elif mtr[-5] == 's' or mtr[-5] == 'x':
		lsv = -5
	elif mtr[-6] == 's' or mtr[-6] == 'x':
		lsv = -6
	elif mtr[-7] == 's' or mtr[-7] == 'x':
		lsv = -7
	elif mtr[-8] == 's' or mtr[-8] == 'x':
		lsv = -8
	elif mtr[-9] == 's' or mtr[-9] == 'x':
		lsv = -9
	elif mtr[-10] == 's' or mtr[-10] == 'x':
		lsv = -10
	else:
		lsv = -1
	for i in pron:
		if '0' in i or '1' in i or '2' in i:
			vowel_index.append(pron.index(i))
		else:
			continue
	return vowel_index[lsv]


def rhyme_finder(word):
	rhyming_words = []
	if len(d[word]) <= 1:
		pron = d[word][0]
	else:
		p0 = d[word][0]
		p1 = d[word][1]
		sj0 = ''.join(p0)
		sl0 = re.split('0|1|2', sj0)
		sj1 = ''.join(p1)
		sl1 = re.split('0|1|2', sj1)
		if len(sl1) < len(sl0):
			pron = p1
		else:
			pron = p0
	pron = strip_numbers(pron)
	lsv = last_stressed_vowel(word)
	rhyme_part = pron[lsv:]
	lrp = len(rhyme_part) * -1
	for (x, y) in word_list_u:
		ps = strip_numbers(y)
		if ps[lrp:] == rhyme_part and ps[lrp-1:] != pron[lsv-1:]:
			rhyming_words.append(x)
		else:
			pass
	rw = [i for i in rhyming_words if not i == word]
	rw2 = [j for j in rw if not j in banned_end_words]
	return rw2

def make_safe(raw_word):
    return re.sub(r'\W+', '', raw_word.lower())

## All words must be valid or bad things will happen
def is_iambic(words):
    if len(words) == 0:
        return True
    words = map(lambda wrd: re.sub(r'\W+', '', wrd), words)
    m_words_stack = map(meter, words)
    m_words = [item for sublist in m_words_stack for item in sublist]
    m_fits = [syl == 'x' or (syl == 'u' and (i%2 == 0)) or (syl == 's' and (i%2 == 1)) for i, syl in enumerate(m_words)]
    return reduce(lambda x,y: x and y, m_fits)

def extract_iambic_pentameter(sentence):
    iambic_runs = []
    raw_previous_words = []
    for w in range(len(sentence)):
        raw_word = sentence[w]
        safe_word = make_safe(raw_word)
        if safe_word in d:
            raw_previous_words.append(raw_word)
            if is_iambic(map(make_safe, raw_previous_words)):
                if line_sylcount(map(make_safe, raw_previous_words)) >= 10:
                    if line_sylcount(map(make_safe, raw_previous_words)) == 10:
                        iambic_runs.append(" ".join(raw_previous_words))
                    raw_previous_words = raw_previous_words[1:]
                    while len(raw_previous_words) > 0:
                        if is_iambic(map(make_safe, raw_previous_words)):
                            break
                        raw_previous_words = raw_previous_words[1:]
            else:
                raw_previous_words = []
        else:
            raw_previous_words = []
    return iambic_runs
