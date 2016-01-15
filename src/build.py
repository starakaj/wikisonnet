import sys
import os
from pynt import task
from tqdm import tqdm
import requests
import shutil

sys.path.append(".")

def rmdir_nonempty(dirname):
    for root, dirs, files in os.walk(dirname, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    try:
        os.rmdir(dirname)
    except OSError:
        print "Could not delete " + dirname

@task()
def download_wikidump():
    url = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
    f = url.split("/")[-1]
    dest = "scraper/data"
    print "Downloading wikipedia dump to scraper/data"
    print "Downloading from {}".format(url)
    response = requests.get(url, stream=True)

    with open("{}/{}".format(dest, f), "wb") as handle:
        for data in tqdm(response.iter_content()):
            handle.write(data)

@task()
def start_server():
    destdir = "wikisonnet_api/bard/"

    # Copy the poem writing files
    rmdir_nonempty(destdir)
    os.mkdir(destdir)
    open(destdir + "__init__.py", 'a').close()
    shutil.copytree("db", destdir + "db")
    shutil.copytree("util", destdir + "util")
    shutil.copytree("wikibard", destdir + "wikibard")

    # Run the server
    from wikisonnet_api import application
    application.run()
