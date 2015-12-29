import server.dbconnect as dbconnect
import server.dbreader as dbreader
import argparse
import codecs
import random
import nltk
import gensim

from nltk.tokenize import sent_tokenize, word_tokenize

parser = argparse.ArgumentParser(description="Experiment with word2vec")
parser.add_argument('model', type=str, help="path to the word2vec model to load")
parser.add_argument('--output', action='store', type=str, default='poems.txt', help="Number of separate processes to run")

args = parser.parse_args()

if __name__ == "__main__":
    conn = dbconnect.MySQLDatabaseConnection.connectionWithConfiguration('local')

    ## Start by getting a random starting line
    lines = dbreader.searchForLines(conn, constraints={'starts':1}, options={'num':1, 'random':True})

    pos_m2 = lines[0]['pos_len_m2']
    pos_m1 = lines[0]['pos_len_m1']
    pos_0 = lines[0]['pos_len']
    pos_1 = lines[0]['pos_len_p1']

    ## Now find 1000 continuations for that line
    continuations = dbreader.searchForLines(conn, constraints={'starts':0, 'ends':0, 'pos_m1':pos_m1, 'pos_0':pos_0}, options={'num':10000, 'random':True})

    ## Load the model
    model = gensim.models.Word2Vec.load(args.model)

    ## Get the text for each of the lines
    start_text = dbreader.textForLineID(conn, lines[0]['id'])
    cont_text = [dbreader.textForLineID(conn, cnt['id']) for cnt in continuations]

    ## Sort the continuations
    sen1 = (u" ").join(word_tokenize(start_text))
    con_sens = [u" ".join(word_tokenize(cnt)) for cnt in cont_text]
    sorted_continuations = sorted(con_sens, key=lambda x: model.score([(sen1 + " " + x).split()])[0], reverse=True)

    print "Best 10"
    for s in sorted_continuations[:10]:
        print sen1 + " " + s

    print "Worst 10"
    for s in sorted_continuations[-10:]:
        print sen1 + " " + s

    conn.close()
