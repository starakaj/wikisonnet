from flask import Flask, abort, jsonify, request, render_template
import wikibard, wikiserver
from  werkzeug.debug import get_current_traceback
import yaml
from flask.ext.cors import CORS

# EB looks for an 'application' callable by default.
application = Flask(__name__)
cors = CORS(application, resources={r"/api/*": {"origins": "*"}})

@application.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@application.route('/compose/<title>', methods=['GET'])
def compose(title):
    poem = wikiserver.poemForPageTitle(title)
    poem_lines = poem.split('\n')
    images = wikiserver.imagesForPageTitle(title)
    if images:
        image = images[0]
        return render_template('poem.html', title=title, poem_lines=poem_lines, image=image)
    else:
        return render_template('poem.html', title=title, poem_lines=poem_lines)

# run the app.
if __name__ == "__main__":
    application.debug = False
    application.testing = True
    application.run()
