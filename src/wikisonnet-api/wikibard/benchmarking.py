import time
import random
import mysql.connector

class Timer:
    def __init__(self):
        self.start_time = time.time()
        self.interval_starts = {}
        self.interval_ends = {}
        self.intervals = {}

    def begin(self, key):
        self.interval_starts[key] = time.time()

    def end(self, key):
        self.interval_ends[key] = time.time()
        if key in self.intervals:
            self.intervals[key] = self.intervals[key] + self.interval_ends[key] - self.interval_starts[key]
        else:
            self.intervals[key] = self.interval_ends[key] - self.interval_starts[key]

    def printTime(self):
        total_time = time.time() - self.start_time
        print("Total time: " + str(total_time))
        for key in self.intervals:
            interval_time = self.intervals[key]
            print("\t" + str(key) + ": " + str(interval_time) + " " + str(100.0 * interval_time / total_time) + "%%")

    def elapsed(self):
        return time.time() - self.start_time

pos_types = ["NN", "TO", "DT", "VB", "NNS", "CC", "IN", "JJ", "VBG", "VBD", "NNP", "MD", "VBZ", "NNP-LOC", "VBN", "NNPS", "RB", "PRP", "VBP", "WDT", "PRP$", "CD", "WRB", "NNP-ORG", "NNP-PERS", "WP", "FW", "JJS", "POS", "RP", "JJR", "UH", "WP$", "RBS", "EX", "PDT", "SYM", "RBR", "NNPS-LOC", "LS"]

def generateRandomQueryForRhyme(rhyme="IYP", database="iambic_lines_mini"):
    random_pos = [pos_types[random.randint(0, len(pos_types)-1)] for _ in range(4)]
    query = """SELECT * FROM """ + database;
    query = query + (
            """ WHERE rhyme_part=%s"""
            """ AND pos_m2=%s"""
            """ AND pos_m1=%s"""
            """ AND pos_0=%s"""
            """ AND pos_1=%s"""
            """ LIMIT 1000;"""
            )
    values = (rhyme, random_pos[0], random_pos[1], random_pos[2], random_pos[3])
    return (query, values)

def generateSHAQueryFromQuery(query, values, database="iambic_lines_mini"):
    key = str(values[0])+str(values[1])+str(values[2])+str(values[3])+str(values[4])
    query = """SELECT id FROM """ + database
    query = query + """ WHERE prevkey4=UNHEX(SHA1(%s))"""
    values = (key, )
    return (query, values)

def runQueryBenchmarks(num=1000):
    conn = mysql.connector.connect(user="william", password="sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)

    t = Timer()
    cursor = conn.cursor()
    t.begin("fast")
    for i in range(num):
        (query, values) = generateRandomQueryForRhyme(database="iambic_lines_metal")
        (query, values) = generateSHAQueryFromQuery(query, values, database="iambic_lines_metal")
        cursor.execute(query, values)
        cursor.fetchall()
    t.end("fast")
    t.begin("slow")
    for i in range(num):
        (query, values) = generateRandomQueryForRhyme(database="iambic_lines_mini")
        # (query, values) = generateSHAQueryFromQuery(query, values, database="iambic_lines_essential")
        cursor.execute(query, values)
        cursor.fetchall()
    t.end("slow")
    cursor.close()
    t.printTime()
    conn.close()

def computeSHA():
    conn = mysql.connector.connect(user="william", password="sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    cursor = conn.cursor()
    query = """SELECT id, rhyme_part, pos_m2, pos_m1, pos_0, pos_1 FROM iambic_lines_essential"""
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    for row in rows:
        key = str(row[1])+str(row[2])+str(row[3])+str(row[4])+str(row[5])
        cursor2 = conn.cursor()
        query = """UPDATE iambic_lines_essential SET prevkey4=UNHEX(SHA1(%s)) WHERE id=%s"""
        values = (key, row[0])
        cursor2.execute(query, values)
        cursor2.close()
    conn.commit()
    conn.close()

def continuePoemComparison():
    import wikibard, dbconnect
    conn = dbconnect.MySQLDatabaseConnection(user="william", password="Sh4kespeare", host="localhost", dbname="wikisonnet", options={"use_cache":0})
    hard_conn = mysql.connector.connect(user="william", password="Sh4kespeare", host="localhost", database="wikisonnet", charset='utf8', use_unicode=True)
    page = 25041
    links = [27047837,2219,5468825,4054,6901847,6886386,8686599,485507,177644,1041974,2965165,36623,146351,198201,15908735,319558,341957,187662,102837,1481067,2395137,205392,454475,265516,654151,32087,778814,307054,6190562,198199,2419084,1666399,32927]
    excludedWord = "reshaped"
    excludedLines = [58814,66973,425329,494873,58801,26556,1719,999778,269663,45562,51466,3967249,32599]
    rhyme = "EYPT"
    previousLine = {"page_id":25041, "id":16, "rhyme_part":"UW", "pos_0":"IN", "pos_1":"DT", "pos_len_m2":"NNS", "pos_len_m1":"DT", "pos_len":"TO", "pos_len_p1":"VB"}

    t = Timer()

    t.begin("Baseline")
    wikibard.continuePoem(conn, page, links, excludedWord=excludedWord, excludedLines=excludedLines, previousLine=previousLine, starts=False, ends=False, rhyme=rhyme)
    t.end("Baseline")

    t.printTime()
