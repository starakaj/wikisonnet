def page_exists(wiki_page_name):
    return True

def page_suggestions(wiki_page_name):
    return ["Fuck all"]

def poem_page(wiki_page_name):
    ## Check if the page exists
    if not page_exists(wiki_page_name):
        return page_suggestions(wiki_page_name)

    ## Check if there's a cached version available
