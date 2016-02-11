from scraper import wikiutils

def textForPageWithID(xml_filename, pageid):
    text = None
    extractor = wikiutils.WikiTextExtractor(xml_filename)
    for page in extractor:
        if extractor.pageIDForCurrentPage() == pageid:
            text = extractor.textForCurrentPage()
            break
        if extractor.pageIDForCurrentPage() > pageid:
            break
    return text
