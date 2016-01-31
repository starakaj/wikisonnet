# Copyright 2015. Amazon Web Services, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import json
from datetime import datetime, timedelta

import flask
from flask import request, Response, jsonify, session
import wikiconnector
from wikierrors import InvalidAPIUsage
from multiprocessing import Manager, Queue, cpu_count, Process, Condition
from flask.ext.cors import CORS
from models import articles, lauds, poems, sessions, tasks
# from IPython import embed

import dotmatrix

print_to_dotmatrix = False

# Default config vals
THEME = 'default' if os.environ.get('THEME') is None else os.environ.get('THEME')
FLASK_DEBUG = 'false' if os.environ.get('FLASK_DEBUG') is None else os.environ.get('FLASK_DEBUG')

# Create the Flask app
application = flask.Flask(__name__)
cors = CORS(application, resources={r"/api/*": {"origins": ["http://localhost:3000", "http://159.203.110.230:3000", "http://wikison.net"], "supports_credentials": True}})

# Load config values specified above
application.config.from_object(__name__)

application.secret_key = 'this is a test secret key'

# Load configuration vals from a file
application.config.from_pyfile('application.config', silent=True)
HOST_IP = 'localhost'if application.config.get('HOST_IP') is None else application.config.get('HOST_IP')
DB_CONFIG = 'local'if application.config.get('DB_CONFIG') is None else application.config.get('DB_CONFIG')
PROCESS_COUNT = 1 if application.config.get('PROCESS_COUNT') is None else application.config.get('PROCESS_COUNT')
RATE_LIMIT = 40 if application.config.get('RATE_LIMIT') is None else application.config.get('RATE_LIMIT')
dbconfig = wikiconnector.dbconfigForName(DB_CONFIG)

# Only enable Flask debugging if an env var is set to true
application.debug = application.config['FLASK_DEBUG'] in ['true', 'True']

# Pool of worker processes
process_count = 4
try:
    process_count = cpu_count();
except:
    print "Could not determine cpu_count--defaulting to {} processes".format(process_count)
task_process = None
task_condition = None

@application.route('/')
def welcome():
    theme = application.config['THEME']
    page_name = articles.getRandomArticleTitle(dbconfig)
    return flask.render_template('index.html', theme=theme, page_name=page_name)

# @application.route('/api/v2/pages/<page_id>/poems', methods=['GET', 'POST'])
# def compose(page_id):
#     if not session.get('id'):
#         session_id = wikiconnector.createSession(dbconfig)
#         session['id'] = session_id
#     poem_dict = wikiconnector.getCachedPoemForPage(dbconfig, page_id)
#     if poem_dict is None:
#         poem_dict = wikiconnector.getCachedPoemForPage(dbconfig, page_id, complete=False)
#     if poem_dict is None:
#         poem_dict = wikiconnector.writeNewPoemForArticle(dbconfig, page_id)
#     return jsonify(poem_dict)

@application.route('/api/v2/poems', methods=['POST'])
def compose():
    if not session.get('id'):
        session_id = sessions.createSession(dbconfig)
        session['id'] = session_id
    title = request.form.get("poemTitle", None)
    if title is None:
        raise InvalidAPIUsage("No article title provided", "You must provide a Wikipedia article title to get a poem")
    twitterHandle = request.form.get("twitterHandle", None, type=str)
    page_id = articles.getArticleIdForTitle(dbconfig, title)
    if page_id is None:
        raise InvalidAPIUsage("Article not found", "Could not find a Wikipedia article with title {}".format(title))
    poem_dict = poems.getCachedPoemForArticle(dbconfig, page_id, True, session['id'])
    if poem_dict is not None:
        if 'id' in session:
            sessions.addPoemToSession(dbconfig, poem_dict['id'], session['id'])
        print_poem(page_id, poem_dict)
    if poem_dict is None:
        poem_dict = poems.getCachedPoemForArticle(dbconfig, page_id, False, session['id'])
    if poem_dict is None:
        userdata = {}
        if twitterHandle is not None:
            userdata = {"source":"twitter", "session":session["id"], "twitter_handle":twitterHandle}
            task_count = tasks.getTaskCountForTwitterHandle(dbconfig, twitterHandle, datetime.now()-timedelta(hours=1))
        elif session.get('id'):
            userdata = {"source":"website", "session":session["id"]}
            task_count = tasks.getTaskCountForSession(dbconfig, session['id'], datetime.now()-timedelta(hours=1))
        if not userdata:
            raise InvalidAPIUsage("Sessions inactive", "The Wikisonnet API requires that sessions be active to work")
        if task_count >= RATE_LIMIT:
            raise InvalidAPIUsage("Rate limit exceeded", "Because Wikisonnet is run by struggling artists, we can only support {} poems per user per hour".format(RATE_LIMIT), status_code=429)
        poem_dict = poems.writeNewPoemForArticle(dbconfig, page_id, task_condition, userdata)
    return jsonify(poem_dict)

@application.route('/api/v2/poems/<int:poem_id>', methods=['GET'])
def lookup(poem_id):
    if not session.get('id'):
        session_id = sessions.createSession(dbconfig)
        session['id'] = session_id
    poem_dict = poems.getSpecificPoem(dbconfig, poem_id, session['id'])
    if poem_dict is not None and poem_dict['complete']:
        if 'id' in session:
            sessions.addPoemToSession(dbconfig, poem_dict['id'], session['id'])
        print_poem(poem_dict['starting_page'], poem_dict)
    elif poem_dict is None:
        poem_dict = {}
    return jsonify(poem_dict)

@application.route("/api/v2/tasks", methods=['GET'])
def getTasks():
    offset = request.args.get('offset', 0, type=int)
    limit = request.args.get('limit', 0, type=int)
    incomplete_tasks = tasks.getIncompleteTasks(dbconfig, offset, limit)
    return jsonify({"tasks":incomplete_tasks})

@application.route("/api/v2/poems", methods=['GET'])
def get_poems():
    offset = request.args.get('offset', 0, type=int)
    limit = request.args.get('limit', 0, type=int)
    sortby  = request.args.get('sortby', None, type=str)
    before = request.args.get('before', None, type=str)
    after = request.args.get('after', None, type=str)
    options = {}
    if sortby:
        options['sortby'] = sortby
    if after:
        options['after'] = after
    if before:
        options['before'] = before
    prewritten_poems = poems.getPoems(dbconfig, offset, limit, session.get('id', 0), options)
    return jsonify({"poems":prewritten_poems})

@application.route("/api/v2/poems/<int:poem_id>/lauds", methods=["POST", "DELETE"])
def put_laud(poem_id):
    if not session.get('id'):
        session_id = sessions.createSession(dbconfig)
        session['id'] = session_id
    if poem_id is not None:
        if request.method == "POST":
            status = lauds.putLaudForPoemAndSession(dbconfig, poem_id, session['id'])
        elif request.method == "DELETE":
            status = lauds.deleteLaudForPoemAndSession(dbconfig, poem_id, session['id'])
        (laud_count, session_laud_count) = lauds.laudCountForPoem(dbconfig, poem_id, session['id'])
        return jsonify({"success":status, "lauds":laud_count, "lauded_by_session":session_laud_count, "poem_id":poem_id})
    else:
        return jsonify({"success":0})

@application.errorhandler(InvalidAPIUsage)
def handle_invalid_usage(err):
    response = jsonify(err.to_dict())
    response.status_code = err.status_code
    return response

def print_poem(page_id, poem_dict):
    if print_to_dotmatrix:
        title = articles.getArticleTitleForId(dbconfig, page_id)
        lines = [r["text"] for r in poem_dict["lines"]]
        dotmatrix.printPoem(title, lines)

def run():
    global task_process
    global task_condition
    task_condition = Condition()
    task_process = Process(target=wikiconnector.task_processor, args=(dbconfig, task_condition)).start()
    application.run(host='0.0.0.0', port=8000)

if __name__ == '__main__':
    run()
