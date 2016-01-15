from flask import Flask, abort, jsonify, request, render_template, redirect, url_for, make_response
import wikibard, wikiserver
from  werkzeug.debug import get_current_traceback
import yaml
import dotmatrix
from flask.ext.cors import CORS
import wikipedia
import requests

print_to_dot_matrix = False

# EB looks for an 'application' callable by default.
application = Flask(__name__)
cors = CORS(application, resources={r"/api/*": {"origins": "*"}})

@application.route('/', methods=['GET', 'POST'])
def index():
    error = request.args.get("error")
    if error is not None:
        return render_template('index.html', error=error)
    else:
        return render_template('index.html')

@application.route('/search', methods=['GET'])
def search():
    query = request.args.get('q')
    query = query.replace("_", " ")
    print("Searching!")
    print(query)
    payload = {'action': 'opensearch', 'limit': '10', 'format': 'json', 'search': query}
    response = requests.get("https://en.wikipedia.org/w/api.php", payload)
    results = response.json()[1]
    print(results)
    updated_results = []
    for result in results:
        updated_results.append(result.replace(" ", "_"))
    return jsonify(list=updated_results)


@application.route('/compose', methods=['POST'])
def compose():
    title = request.form.get("query")
    try:
        page = wikipedia.page(title.replace("_", " "))
    except wikipedia.exceptions.DisambiguationError:
        error = "{} is a disambiguation page, try something else".format(title.replace("_", " "))
        response = make_response(redirect(url_for('index', error=error)))
        return response

    except wikipedia.exceptions.PageError:
        error = "{} is not a real page, try something else".format(title.replace("_", " "))
        response = make_response(redirect(url_for('index', error=error)))
        return response

    if not page:
        error = "{} is not a valid page, try something else".format(title.replace("_", " "))
        response = make_response(redirect(url_for('index', error=error)))
        return redirect(url_for('index'))
    print("Composing poem for " + title)
    poem = wikiserver.poemForPageTitle(title)
    poem_lines = poem.split('\n')
    title = title.replace("_", " ")
    images = wikiserver.imagesForPageTitle(title)

    if print_to_dot_matrix:
        dotmatrix.printPoem(title, poem_lines)

    if images:
        image = images[0]
        return render_template('poem.html', title=title, poem_lines=poem_lines, image=image)
    else:
        return render_template('poem.html', title=title, poem_lines=poem_lines)

# run the app.
if __name__ == "__main__":
    application.debug = False
    application.testing = True
    application.run(threaded=True)
