from flask import Flask, abort, jsonify, request
from pybard import poem_page
import task

application = Flask(__name__)

application.add_url_rule('/', 'index', (lambda: "Holy shit you did it congratulations text"))

@application.route('/api/v2/compose/<wiki>', methods=['GET'])
def compose(wiki):
    poem = poem_page(wiki)
    return jsonify(poem=poem)

# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    # application.debug = True
    application.testing = True
    application.run()
