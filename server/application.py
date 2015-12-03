from flask import Flask, abort, jsonify, request
import dbconnect, dbreader
import wikibard
from  werkzeug.debug import get_current_traceback
import yaml
from flask.ext.cors import CORS

try:
    f = open('dbconfig.yml', 'r')
    databases = yaml.load(f)
except Exception, e:
    print "Could not load dbconfig.yml, did you member to add it?"
    print str(e)

# Change this if you'd like to use a local database or something
dbconfig = 'amazon'

def random_page():
    db = dbconnect.connectionWithConfiguration(dbconfig)
    pageID = dbreader.randomIndexedPage(db)
    return dbreader.pageTitleForPageID(db, pageID)

def say_hello(username = "World"):
    return '<p>Hello %s!</p>\n' % username

# some bits of text for the page.
header_text = '''
    <html>\n<head> <title>Wikisonnets Flask Test</title> </head>\n<body>'''
instructions = '''
    <p><em>Hint</em>: This is a RESTful web service! Try hitting /api/v1/random
    to get an indexed page, then /api/v1/compose/<page> on that page.</p>\n'''
home_link = '<p><a href="/">Back</a></p>\n'
footer_text = '</body>\n</html>'

# EB looks for an 'application' callable by default.
application = Flask(__name__)
cors = CORS(application, resources={r"/api/*": {"origins": "*"}})

# add a rule for the index page.
application.add_url_rule('/', 'index', (lambda: header_text +
    say_hello() + instructions + footer_text))

# add a rule that actually talks to the database
application.add_url_rule('/api/v1/random', 'random', (lambda:
    jsonify(title=random_page())))

# poem writing rule
def poem_page(wiki, sloppy=False):
    try:
        db = dbconnect.connectionWithConfiguration(dbconfig)
        pageID = dbreader.pageIDForPageTitle(db, wiki)
        if pageID is None:
            return {"error":"No page with name " + wiki}
        return wikibard.iPoem(pageID, dbconfig, sloppy=sloppy)
    except Exception, e:
        track = get_current_traceback(False, skip=1, show_hidden_frames=True)
        track.log()
        return str(track.render_summary())

@application.route('/api/v1/compose/<wiki>', methods=['GET'])
def compose(wiki):
    if 'sloppy' in request.args:
        poem = poem_page(wiki, sloppy=request.args['sloppy'])
    else:
        poem = poem_page(wiki)
    return jsonify(poem=poem)

# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.testing = True
    application.run()
