import json
import logging
import math
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
from functools import partial
import re
from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

service_version = "2020-06-16"


def send_response(code: int, message):
    response = {"statusCode": code, "body": json.dumps(message)}
    logger.info(response)
    return response


def clean_string(s):
    if type(s) != str:
        return ""
    return re.sub("[^a-z ]", "", s.lower()).strip()


def handler(event, context):
    logger.info(event)
    if event.get("httpMethod") == "OPTIONS":
        return send_response(200, {})
    req = json.loads(event["body"])
    if len(req) == 0:
        return send_response(200, {"version": service_version})
    with open("config/config.json", "r") as r:
        config = json.load(r)
    entity_map = config["entity_map"]
    entity_map[None] = 0
    doctype_map = config["doctype_map"]
    keywords = config["keywords_by_doctype"][req.get("doctype", "BANK STATEMENT")]
    features = generate_feature_set(req, keywords, entity_map, doctype_map)
    features["version"] = service_version
    return send_response(200, features)


def get_coordinates_of_keywords(page_ocr_json, keywords):
    page_size = {
        "x": page_ocr_json["page_size"]["width"],
        "y": page_ocr_json["page_size"]["length"],
    }

    res = {"page_size": page_size}
    keyword_coordinates = defaultdict(list)
    for kw in keywords:
        for _, word in page_ocr_json["words"].items():
            clean_word = clean_string(word["text"])
            if clean_word == kw:
                keyword_coordinates[kw].append(word["coordinates"])
    res["keyword_coordinates"] = keyword_coordinates
    return res


def calculate_distance_and_angle(src, dest, x_normal, y_normal):
    res = {}
    src_x = (src["x0"] + src["x1"]) / (2.0)
    src_y = (src["y0"] + src["y1"]) / (2.0)
    dest_x = (dest["x0"] + dest["x1"]) / (2.0)
    dest_y = (dest["y0"] + dest["y1"]) / (2.0)
    res["distance"] = math.sqrt(
        (dest_x - src_x) ** 2 + (dest_y - src_y) ** 2
    ) / math.sqrt(x_normal ** 2 + y_normal ** 2)
    res["angle"] = math.atan2((dest_y - src_y) * 1.0, (dest_x - src_x) * 1.0)
    return res


def coerce_currency(value, entity_type):
    if entity_type != "CURRENCY":
        return None

    try:
        curr = float(
            value.strip("$%, ").replace(", ", "").replace("  ", "").replace(" ", "")
        )
        if (curr < 1000000) and (curr > -100000):
            return curr
        else:
            return None
    except:
        return None


def calculate_currency_zscores(currs):
    currencies = np.array(currs)

    def zscore(curr):
        try:
            currs = [curr for curr in currencies if curr is not None]
            std = np.nanstd(currs)
            if std == 0:
                return None
            return (curr - np.nanmean(currs)) / std
        except:
            return None

    return [zscore(curr) for curr in currencies]


def try_date_pattern(target, pattern):
    """try_date_pattern

    :param target:
    :type target: str
    :param pattern:
    :type pattern: str
    :rtype: datetime
    """
    try:
        return datetime.strptime(target, pattern)
    except:
        return None


common_date_patterns = [
    "%m/%d/%Y",
    "%m/%d/%y",
    "%B %d, %Y",
    "%b %d, %Y",
    "%B %d, %y",
    "%b %d, %y",
    "%B %d, %Y" "%B %d, %y",
    "%b %d, %Y",
    "%b %d, %y",
    "%B %d. %Y",
    "%b %d. %Y",
    "%B %d. %y",
    "%b %d. %y",
    "%B %d, %Y %I:%M:%S %p",
    "%b %d, %Y %I:%M:%S %p",
    "%b %d, %y %I:%M:%S %p",
    "%B %d, %y %I:%M:%S %p",
    "%m-%d-%Y",
    "%Y-%m-%d",
]


def coerce_date(string, patterns):
    try:
        clean_string = string.replace(" ", "")
    except:
        clean_string = string

    partial_string = partial(try_date_pattern, string)
    partial_clean = partial(try_date_pattern, clean_string)

    coercions = list(map(partial_string, patterns)) + list(map(partial_clean, patterns))

    try:
        return [coercion for coercion in coercions if coercion is not None][0]
    except:
        return None


def dateable(string, patterns, entity_type):
    if entity_type != "DATE":
        return None
    coerced = coerce_date(string, patterns)

    if coerced is None:
        return None

    return string


def date_diff(date1, date2):
    if date1 is None or date2 is None:
        return None

    return int((date2 - date1).days)


def filter_date(date, threshold):
    if date is None:
        return False

    return date >= threshold


def first_date(dates: list):
    current_date = date.today()
    lower_date_threshold = datetime(
        current_date.year - 7, *current_date.timetuple()[1:-2]
    )

    dates = [dt for dt in dates if filter_date(dt, lower_date_threshold)]

    try:
        return sorted([dt for dt in dates if dt is not None])[0]
    except:
        return None


def day_of_week(date):
    if date is None:
        return None
    return date.weekday()


def day_of_month(date):
    if date is None:
        return None
    return date.day


def generate_feature_set(event, keywords, entity_map, doctype_map):
    logger.info(f'ocr = \n {event["ocr"]}\n\nentities = \n {event["entities"]}')
    features, keyword_coordinates, feature_set = {}, {}, []
    for page_num, page in event["ocr"].items():
        keyword_coordinates[page_num] = get_coordinates_of_keywords(page, keywords)
    for entity_type, entities_extracted in event["entities"].items():
        if entity_type in entity_map:
            for entity in entities_extracted:
                page_num, row = entity["PAGE"], get_empty_row(keywords)
                keywords_on_the_page = keyword_coordinates[f"page_{page_num}"]
                x_normal, y_normal = (
                    keywords_on_the_page["page_size"]["x"],
                    keywords_on_the_page["page_size"]["y"],
                )
                center_coordinates = {"x0": 0, "y0": 0, "x1": x_normal, "y1": y_normal}
                row["datapoint"], row["entity_id"] = (
                    entity["CONTENT"]["CONTENT"]["value"],
                    entity["ENTITY_ID"],
                )
                row["page_number"], row["entity_type"] = float(page_num), entity_type
                row["doctype"] = doctype_map[
                    event["doctype"] if "doctype" in event else "BANK STATEMENT"
                ]
                row["currency"] = coerce_currency(
                    entity["CONTENT"]["CONTENT"]["value"], entity_type
                )
                row["date"] = dateable(
                    entity["CONTENT"]["CONTENT"]["value"],
                    common_date_patterns,
                    entity_type,
                )
                datapoint_coordinates = entity["CONTENT"]["CONTENT"]["coordinates"]
                distance_af_center = calculate_distance_and_angle(
                    center_coordinates, datapoint_coordinates, x_normal, y_normal
                )
                row["df_center"], row["af_center"] = (
                    distance_af_center["distance"],
                    distance_af_center["angle"],
                )
                for keyword_value, keyword_coordinates_lst in keywords_on_the_page[
                    "keyword_coordinates"
                ].items():
                    vector_keyword = find_closest_coordinates(
                        keyword_coordinates_lst,
                        datapoint_coordinates,
                        x_normal,
                        y_normal,
                    )
                    row[f"dfk_{keyword_value}"], row[f"afk_{keyword_value}"] = (
                        vector_keyword["distance"],
                        vector_keyword["angle"],
                    )
                feature_set.append(row)
    logger.info(feature_set)
    fsdf = pd.DataFrame.from_dict(feature_set, orient="columns")
    fsdf.drop(["currency", "date"], axis=1, inplace=True)

    features["features"] = [row.dropna().to_dict() for index, row in fsdf.iterrows()]
    logger.info(features)
    return features


def get_empty_row(keywords):
    columnss = [
        "datapoint",
        "af_center",
        "df_center",
        "doctype",
        "entity_type",
        "page_number",
    ]
    empty_row = {}

    for col in columnss:
        empty_row[col] = None

    for kw in keywords:
        empty_row[f"afk_{kw}"] = None
        empty_row[f"dfk_{kw}"] = None

    return empty_row


def find_closest_coordinates(
    keyword_coordinates_lst, datapoint_coordinates, x_normal, y_normal
):
    res = {
        calculate_distance_and_angle(co, datapoint_coordinates, x_normal, y_normal)[
            "distance"
        ]: co
        for co in keyword_coordinates_lst
    }
    mn = min(res.keys())
    return calculate_distance_and_angle(
        res[mn], datapoint_coordinates, x_normal, y_normal
    )
