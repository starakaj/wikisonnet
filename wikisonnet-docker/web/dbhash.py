import hashlib
import json

def columnsDictToSHA(columns):
    return hashlib.sha1(json.dumps(columns, sort_keys=True)).digest()
