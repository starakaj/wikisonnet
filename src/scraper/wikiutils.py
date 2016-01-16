import lxml.etree as etree
import mwparserfromhell
import re

def make_canonical(link):
    title = link.title.strip_code()
    title = title.replace(" ", "_")
    return title

class WikiTextExtractor:
    def __init__(self, filename):
        self.filename = filename
        self.nsmap = {}
        self.initializeDefaultNamespace()
        self.tree_iterator = etree.iterparse(self.filename, events=('end',), tag=("{" + self.nsmap['default'] + "}" + 'page'))
        self.current_page = None
        self.pages_to_clear = []

    def __iter__(self):
        return self

    # Scans until it finds the empty namespace, then stops
    def initializeDefaultNamespace(self):
        # f = open(self.filename, 'r')
        # print (f.readline())
        # f.close()
        # print (self.filename)
        it = etree.iterparse(self.filename, events=('start-ns',))
        for _, elem in it:
            key = 'default' if elem[0] == '' else elem[0]
            self.nsmap[key] = elem[1]
            if key=='default':
                break

    def next(self):
        if self.current_page is not None:
            self.current_page.clear()
        for _, elem in self.tree_iterator:
            self.current_page = elem
            return self
        raise StopIteration()

    def page_count(self):
        pages = 0
        tree_iterator = etree.iterparse(self.filename, events=('end',))
        for event, elem in tree_iterator:
            if elem.tag[-4:] == 'page':
                pages = pages + 1
                if pages%20000 == 0:
                    print ("Counted " + str(pages) + " pages")
            elem.clear()
        return pages

    def pageIDForCurrentPage(self):
        page = self.current_page
        return int(page.find('default:id', self.nsmap).text)

    def redirectTitleForCurrentPage(self):
        page = self.current_page
        p = page.find('default:redirect', self.nsmap)
        if p is not None:
            return p.attrib['title']
        return None

    def revisionForCurrentPage(self):
        page = self.current_page
        return int(page.find('default:revision', self.nsmap).find('default:id', self.nsmap).text)

    def timestampForCurrentPage(self):
        page = self.current_page
        return page.find('default:revision', self.nsmap).find('default:timestamp', self.nsmap).text

    def textForCurrentPage(self):
        page = self.current_page
        rawMarkupText = page.find('default:revision', self.nsmap).find('default:text', self.nsmap).text
        parsedText = mwparserfromhell.parse(rawMarkupText)
        return parsedText.strip_code()

    def canonicalLinksForCurrentPage(self):
        page = self.current_page
        rawMarkupText = page.find('default:revision', self.nsmap).find('default:text', self.nsmap).text
        parsedText = mwparserfromhell.parse(rawMarkupText)
        wikilinks = parsedText.filter_wikilinks(False)
        wikilinks = map(make_canonical, wikilinks)
        wikilinks = filter(lambda x: not bool(re.match('file:', x, re.I)), wikilinks)
        wikilinks = filter(lambda x: not bool(re.match('media:', x, re.I)), wikilinks)
        wikilinks = filter(lambda x: not bool(re.match('image:', x, re.I)), wikilinks)
        wikilinks = map(lambda x: x[0:x.find('#')] if '#' in x else x, wikilinks)
        return wikilinks

    def titleForCurrentPage(self):
        page = self.current_page
        return page.find("default:title", self.nsmap).text

    def modelForCurrentPage(self):
        page = self.current_page
        return page.find('default:revision', self.nsmap).find('default:model', self.nsmap).text

    def formatForCurrentPage(self):
        page = self.current_page
        return page.find('default:revision', self.nsmap).find('default:format', self.nsmap).text

    def contributorNameForCurrentPage(self):
        page = self.current_page
        contributor = page.find('default:revision', self.nsmap).find('default:contributor', self.nsmap)
        if contributor is not None:
            return contributor.find('default:username', self.nsmap).text
        return None

    def contributorIDForCurrentPage(self):
        page = self.current_page
        contributor = page.find('default:revision', self.nsmap).find('default:contributor', self.nsmap)
        if contributor is not None:
            return contributor.find('default:id', self.nsmap).text
        return None
