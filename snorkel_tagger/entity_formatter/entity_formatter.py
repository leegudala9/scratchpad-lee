import json
import logging
import dateutil.parser
import re
from datetime import datetime
from itertools import groupby
import preprocess


logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter_version = "2020-10-22-12"


def format(text, entity_type):
    try:
        if entity_type == "DATE":
            return dateutil.parser.parse(text).strftime("%m/%d/%Y")

        if entity_type == "QUANTITY":
            return (
                text.replace(",", "").replace("$", "").replace("%", "").replace(" ", "")
            )
    except Exception as e:
        msg = f"failed to format {e}"
        logger.error(msg)
        return text
    return text


def group_entities(entities):
    groupped_entities = {}
    for entity in entities.values():
        entity_type = get_entity_type(entity["entity_type"], entity)
        if entity_type in groupped_entities:
            groupped_entities[entity_type].append(entity)
        else:
            groupped_entities[entity_type] = [entity]
    return groupped_entities


def get_entity_type(entity_type, entity):
    if entity_type not in ["OTHER", "QUANTITY", "DATE"]:
        return entity_type
    elif entity_type == "DATE":
        txt = entity["text"].replace(",", "").replace(" ", "")
        return "YEAR" if len(txt) == 4 and isYear(txt) else "DATE"
    else:
        try:
            float(
                entity["text"]
                .strip("$%,")
                .replace(",", "")
                .replace("  ", "")
                .replace(" ", "")
            )
            return "QUANTITY"
        except:
            return "OTHER"


def translate_entities(entities, page):
    translated_entities, translation_key = (
        {},
        {
            "YEAR": "YEAR",
            "PERSON": "PERSON NAME",
            "LOCATION": "ADDRESS",
            "ORGANIZATION": "COMPANY NAME",
            "COMMERCIAL_ITEM": "COMMERCIAL_ITEM",
            "EVENT": "EVENT",
            "TITLE": "TITLE",
            "DATE": "DATE",
            "QUANTITY": "CURRENCY",
            "OTHER": "OTHER",
            "ZIPCODE": "ZIPCODE",
            "STATE": "STATE",
            "ROUTE": "ROUTE",
            "CITY": "CITY",
        },
    )
    for entity_type, entity_list in entities.items():
        translated_entity_list = []
        for entity in entity_list:
            # THE MVP PATH DUE TO UI DEPENDENCY OF STRUCTURE
            translated_entity = {}
            translated_entity["ENTITY_ID"] = entity["entity_id"]
            translated_entity["PAGE"] = page
            translated_entity["CONFIDENCE"] = entity["entity_score"]
            translated_entity["CONTENT"] = {
                "CONTENT": {
                    "value": format(entity["text"], entity_type),
                    "raw_text_unformatted": entity["text"],
                    "coordinates": {
                        "x0": entity["bounding_box"][0],
                        "y0": entity["bounding_box"][1],
                        "x1": entity["bounding_box"][2],
                        "y1": entity["bounding_box"][3],
                    },
                    "word_ids": entity["word_ids"],
                }
            }
            if filter_entities(translated_entity, entity_type):
                translated_entity_list.append(translated_entity)
        translated_entities[translation_key[entity_type]] = translated_entity_list
    return translated_entities


def filter_entities(entity, entity_type):
    try:
        value = entity["CONTENT"]["CONTENT"]["value"]
        raw_text_unformatted = entity["CONTENT"]["CONTENT"]["raw_text_unformatted"]
        if entity_type == "QUANTITY" and (re.search("[a-zA-Z]+", value) is not None):
            return False
        if entity_type == "DATE" and value == raw_text_unformatted:
            datetime.strptime(value, "%m/%d/%Y")
    except Exception as e:
        msg = f"failed to format {e}"
        logger.error(msg)
        return False
    return True


def isYear(text):
    try:
        datetime.strptime(text, "%Y")
        return True
    except:
        return False


def extract_full_name_attributes(vals):
    confidence = 0
    value = ""
    raw_text_unformatted = ""
    y0 = []
    y1 = []
    word_ids = []
    for each in vals:
        confidence += each["CONFIDENCE"]
        value = value + each["CONTENT"]["CONTENT"]["value"] + " "
        raw_text_unformatted = (
            raw_text_unformatted + each["CONTENT"]["CONTENT"]["raw_text_unformatted"]
        )
        y0.append(each["CONTENT"]["CONTENT"]["coordinates"]["y0"])
        y1.append(each["CONTENT"]["CONTENT"]["coordinates"]["y1"])
        word_ids += each["CONTENT"]["CONTENT"]["word_ids"]
    return confidence, value, raw_text_unformatted, y0, y1, word_ids


def add_full_name_entities(t_entities):
    names = t_entities["PERSON NAME"]
    names_sorted = sorted(names, key=lambda k: k["ENTITY_ID"])
    for k, v in groupby(names_sorted, key=lambda x: (x["ENTITY_ID"].split("_")[0])):
        vals = list(v)
        if len(vals) > 1:
            (
                confidence,
                value,
                raw_text_unformatted,
                y0,
                y1,
                word_ids,
            ) = extract_full_name_attributes(vals)
            new_full_name = {}
            new_full_name["ENTITY_ID"] = k
            new_full_name["PAGE"] = vals[0]["PAGE"]
            new_full_name["CONFIDENCE"] = confidence / len(vals)
            new_full_name["CONTENT"] = {
                "CONTENT": {
                    "value": value.strip(),
                    "raw_text_unformatted": raw_text_unformatted,
                }
            }
            new_full_name["CONTENT"]["CONTENT"]["coordinates"] = {
                "x0": vals[0]["CONTENT"]["CONTENT"]["coordinates"]["x0"],
                "x1": vals[-1]["CONTENT"]["CONTENT"]["coordinates"]["x1"],
                "y0": min(y0),
                "y1": max(y1),
            }
            new_full_name["CONTENT"]["CONTENT"]["word_ids"] = word_ids
            t_entities["PERSON NAME"].append(new_full_name)
    return t_entities


def format_entities(entities, page):
    washed_entities = preprocess.entity_cleaning(entities)
    entities_with_page_number = preprocess.replace_page_number(washed_entities, page)
    mapped_entities = preprocess.map_entities(entities_with_page_number)
    reduced_entities = preprocess.reduce_entities(mapped_entities)
    groupped_entities_by_type = group_entities(reduced_entities)
    translated_entities = translate_entities(groupped_entities_by_type, page)
    if "PERSON NAME" in translated_entities.keys():
        translated_entities = add_full_name_entities(translated_entities)
    logger.info(f"Translated Entities = {translated_entities}")
    return {"statusCode": 200, "body": translated_entities}


def handler(event, context):
    output = {}
    req = json.loads(event["body"])
    logger.info(f"Input= {event}")
    try:
        entities = req["entities"]
        page = req["page"]
        if len(entities) == 0:
            return {"body": json.dumps({"statusCode": 200, "body": {}})}
    except Exception as e:
        msg = f"error parsing request: {e}"
        logger.error(msg)
        output["statusCode"] = 400
        output["body"] = {"code": 400, "message": msg}
    try:
        output = format_entities(entities, page)
        output["version"] = formatter_version
    except Exception as e:
        msg = f"error parsing request: {e}"
        logger.error(msg)
        output["statusCode"] = 400
        output["body"] = {"code": 400, "message": msg}
    return {"body": json.dumps(output)}
