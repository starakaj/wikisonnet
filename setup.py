import urllib
import bz2

dl_link = "https://dl.dropboxusercontent.com/u/15166642/wikisonnet/data/enwiki-latest-pages-articles3.xml-p000025001p000055000.bz2"
filename = "./data/enwiki-latest-pages-articles3.xml-p000025001p000055000.bz2"

print "Downloading data"
testfile = urllib.URLopener()
testfile.retrieve(dl_link, filename)

print "Unzipping data"
newfilename = ".".join(filename.split('.')[:-1]);
with open(newfilename, 'wb') as new_file, bz2.BZ2File(filename, 'rb') as file:
        for data in iter(lambda : file.read(100 * 1024), b''):
            new_file.write(data)

print "All done! Have fun writing sonnets"
