def laudRandomly(host, laudcount):
    import json, httplib
    conn = httplib.HTTPConnection(host)

    for i in range(laudcount):

        ## Get a random poem ID
        conn.request("GET", "/api/v2/poems/random")
        random_poem = conn.getresponse().read()
        random_poem_json = json.loads(random_poem)
        random_poem_id = random_poem_json['id']

        ## Laud it
        conn.request("POST", "/api/v2/poems/{}/lauds".format(random_poem_id))
        conn.getresponse().read()

    conn.close()
