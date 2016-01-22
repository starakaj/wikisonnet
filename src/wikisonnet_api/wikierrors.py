from flask import jsonify

class InvalidAPIUsage(Exception):
    status_code = 400
    problem_uri_root = "http://www.wikison.net/probs/"

    def __init__(self, title, detail=None, status_code=None, payload=None):
        Exception.__init__(self)
        self.title = title
        self.detail = detail
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['problemType'] = self.problem_uri_root + "invalid_usage"
        rv['title'] = self.title
        if self.detail is not None:
            rv['detail'] = self.detail
        return rv
