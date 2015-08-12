from HTMLParser import HTMLParser
from htmlentitydefs import name2codepoint

class MyHTMLParser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        print "Start tag:", tag
        for attr in attrs:
            print "     attr:", attr
    def handle_endtag(self, tag):
        print "End tag  :", tag
    def handle_data(self, data):
        print "Data     :", data

def wikipediaLinkHook(parser_env, namespace, body):
    article.strip().capitalize().replace(' ', '_')
    text = (text or article).strip()
    return '<a href="http://en.wikipedia.org/wiki/%s">%s</a>' % (href, text)
