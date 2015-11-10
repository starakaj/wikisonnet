from flask import Flask, abort, jsonify
import dbmanager
import wikibard
from  werkzeug.debug import get_current_traceback
import yaml

try:
    f = open('dbconfig.yml', 'r')
    databases = yaml.load(f)
except Exception, e:
    print "Could not load dbconfig.yml, did you member to add it?"
    print str(e)

# Change this if you'd like to use a local database or something
dbconfig = databases['amazon']

# print a nice greeting.
def say_hello(username = "World"):
    return '<p>Hello %s!</p>\n' % username

def random_page():

    db = dbmanager.MySQLDatabaseConnection(dbconfig["database"], dbconfig["user"], dbconfig["host"], dbconfig["password"])
    pageID = db.randomIndexedPage()
    return '<h1>%s</h1>\n' % db.pageTitleForPageID(pageID)

# some bits of text for the page.
header_text = '''
    <html>\n<head> <title>EB Flask Test</title> </head>\n<body>'''
instructions = '''
    <p><em>Hint</em>: This is a RESTful web service! Append a username
    to the URL (for example: <code>/Thelonious</code>) to say hello to
    someone specific.</p>\n'''
home_link = '<p><a href="/">Back</a></p>\n'
footer_text = '</body>\n</html>'

# EB looks for an 'application' callable by default.
application = Flask(__name__)

# add a rule for the index page.
application.add_url_rule('/', 'index', (lambda: header_text +
    say_hello() + instructions + footer_text))

# add a rule that actually talks to the database
application.add_url_rule('/random', 'random', (lambda:
    header_text + random_page() + footer_text))

# poem writing rule
def poem_page(wiki):
    try:
        db = dbmanager.MySQLDatabaseConnection(dbconfig["database"], dbconfig["user"], dbconfig["host"], dbconfig["password"])
        pageID = db.pageIDForPageTitle(wiki)
        if pageID is None:
            return {"error":"No page with name " + wiki}
        return wikibard.iPoem(pageID, dbconfig)
    except Exception, e:
        track = get_current_traceback(False, skip=1, show_hidden_frames=True)
        track.log()
        return str(track.render_summary())

application.add_url_rule('/api/v1/compose/<wiki>', 'compose', (lambda wiki:
    jsonify(poem=poem_page(wiki))))

# add a rule when the page is accessed with a name appended to the site
# URL.
application.add_url_rule('/<username>', 'hello', (lambda username:
    header_text + say_hello(username) + home_link + footer_text))

# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.testing = True
    application.run()
