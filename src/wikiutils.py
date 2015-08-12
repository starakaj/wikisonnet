from lxml import etree
import mwparserfromhell

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
