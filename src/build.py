import sys
import os
from pynt import task
from tqdm import tqdm
import requests
import shutil


srcdir = "/".join(os.path.realpath(__file__).split("/")[:-1]) + "/"
sys.path.append(srcdir)

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
    destdir = srcdir + "wikisonnet_api/bard/"

    # Copy the poem writing files
    rmdir_nonempty(destdir)
    os.mkdir(destdir)
    open(destdir + "__init__.py", 'a').close()
    shutil.copytree(srcdir + "db", destdir + "db")
    shutil.copytree(srcdir + "util", destdir + "util")
    shutil.copytree(srcdir + "wikibard", destdir + "wikibard")

    # Start newrelic
    import newrelic.agent
    newrelic_path = "/".join(os.path.realpath(__file__).split("/")[:-1]) + "/wikisonnet_api/newrelic.ini"
    newrelic.agent.initialize(newrelic_path)

    # Run the server
    from wikisonnet_api import application
    application.run()
