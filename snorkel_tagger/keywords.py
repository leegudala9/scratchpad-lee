import json
from feature_generation import get_coordinates_of_keywords, clean_string


def get_keywords(doctype):
    with open("../config/config.json") as f:
        conf_json = json.load(f)
    return [
        clean_string(tok)
        for tok in conf_json["keywords_by_doctype"][doctype]
        if len(clean_string(tok))
    ]


def get_all_pages_keywords(ocr, doctype):
    keywords = get_keywords(doctype)
    keyword_coordinates = {}
    for page_num, page in ocr.items():
        keyword_coordinates[page_num] = get_coordinates_of_keywords(page, keywords)
    return keyword_coordinates
