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

import flask
from flask import request, Response, jsonify
import wikiconnector

# Default config vals
THEME = 'default' if os.environ.get('THEME') is None else os.environ.get('THEME')
FLASK_DEBUG = 'false' if os.environ.get('FLASK_DEBUG') is None else os.environ.get('FLASK_DEBUG')

# Create the Flask app
application = flask.Flask(__name__)

# Load config values specified above
application.config.from_object(__name__)

# Load configuration vals from a file
application.config.from_pyfile('application.config', silent=True)
HOST_IP = 'localhost'if application.config.get('HOST_IP') is None else application.config.get('HOST_IP')
DB_CONFIG = 'local'if application.config.get('DB_CONFIG') is None else application.config.get('DB_CONFIG')
dbconfig = wikiconnector.dbconfigForName()

# Only enable Flask debugging if an env var is set to true
application.debug = application.config['FLASK_DEBUG'] in ['true', 'True']


@application.route('/')
def welcome():
    theme = application.config['THEME']
    page_name = wikiconnector.getRandomPoemTitle(dbconfig)
    return flask.render_template('index.html', theme=theme, page_name=page_name)

@application.route('/api/v2/pages/<page_id>/poems', methods=['GET', 'POST'])
def compose(page_id):
    poem_dict = wikiconnector.getCachedPoemForPage(dbconfig, page_id)
    if poem_dict is None:
        poem_dict = wikiconnector.getCachedPoemForPage(dbconfig, page_id, complete=False)
    if poem_dict is None:
        print "About to write a new poem"
        poem_dict = wikiconnector.writeNewPoemForPage(dbconfig, page_id)
    return jsonify(poem_dict)

@application.route('/api/v2/poems/<poem_id>', methods=['GET'])
def lookup(poem_id):
    poem_dict = wikiconnector.getSpecificPoem(dbconfig, poem_id)
    return jsonify(poem_dict)

if __name__ == '__main__':
    application.run(host='0.0.0.0', port=80)
