from lxml import etree
import mwparserfromhell
import re

def make_canonical(link):
    title = link.title.strip_code()
    # print title
    # print type(title)
    title.replace(" ", "_")
    return title

class WikiTextExtractor:
    def __init__(self, filename):
        self.filename = filename
        self.tree = etree.parse(filename)
        self.root = self.tree.getroot()
        self.nsmap = self.root.nsmap
        if None in self.nsmap:
            self.nsmap['default'] = self.nsmap[None]
        self.pages = self.root.findall('default:page', self.nsmap)

    def textForPage(self, page):
        rawMarkupText = page.find('default:revision', self.nsmap).find('default:text', self.nsmap).text
        parsedText = mwparserfromhell.parse(rawMarkupText)
        return parsedText.strip_code()

    def canonicalLinksForPage(self, page):
        rawMarkupText = page.find('default:revision', self.nsmap).find('default:text', self.nsmap).text
        parsedText = mwparserfromhell.parse(rawMarkupText)
        wikilinks = parsedText.filter_wikilinks(False)
        wikilinks = map(make_canonical, wikilinks)
        return wikilinks

    def titleForPage(self, page):
        return page.find("default:title", self.nsmap).text
