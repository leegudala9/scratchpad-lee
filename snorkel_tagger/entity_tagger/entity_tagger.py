from typing import List
import json
import logging
import boto3
import uuid
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)

comprehend = boto3.client("comprehend")
tagger_version = "2019-10-22-11"

def parse_words(words):
    #print(f"Unsorted: {words}")
    parsed_words = []
    string_index = 0
    words = sorted(words, key=lambda k: int(k['id'].split('_')[-1]))
    #print(f"Sorted: {words}")
    for word in words:
        pos = word["pos"]
        token = "" if word.get("text") is None else word.get("text")
        token = re.sub('[()]', '', token)
        parsed_words.append(
            {
                "word_id": word.get("id"),
                "text": token,
                "string_index": string_index,
                "bounding_box": [pos["x0"], pos["y0"], pos["x1"], pos["y1"]],
                "confidence": float(word["score"]) / 100,
            }
        )

        string_index += len(token) + 1

    return parsed_words


def make_blob(words: List[dict]) -> str:
    return (
        " ".join([word["text"] for word in words])
        .encode("utf-8", "ignore")[:5000]
        .decode("utf-8", "ignore")
    )


def parse_comprehend_output(output: dict, uid: str) -> list:
    begin = output["BeginOffset"]
    tokens = output["Text"].split()

    parsed_output = []

    for token in tokens:
        parsed_output.append(
            {
                "entity_id": uid,
                "text": token,
                "entity_score": output["Score"],
                "entity_type": output["Type"],
                "string_index": begin,
            }
        )

        begin += len(token) + 1

    return parsed_output


def comprehend_entities(text: str) -> list:
    entities = comprehend.detect_entities(Text=text, LanguageCode="en")["Entities"]
    appendedEntities = append_ssn_formats(text,entities)
    output: list = []
    for entity in appendedEntities:
        uid = str(uuid.uuid4())
        output += parse_comprehend_output(entity, uid)
    return output


def zip_words_entities(words: List[dict], entities: List[dict]) -> List[dict]:

    zipped = [
        {
            **ent,
            **next(
                iter(
                    [
                        word
                        for word in words
                        if word["string_index"] == ent["string_index"]
                    ]
                ),
                {},
            ),
        }
        for ent in entities
    ]

    zipped = [z for z in zipped if "word_id" in z]

    return {
        "entities": [
            {k: v for k, v in z.items() if k != "string_index"} for z in zipped
        ]
    }


def handler(event, context):
    output = {}
    logger.info(event)
    try:
        req = json.loads(event["body"])
        hocr = req["Words"]

        if len(hocr) == 0:

            return {"statusCode": 200, "body": json.dumps({"entities": []})}

    except Exception as e:
        msg = f"error loading hocr: {e}"
        logger.error(msg)

        output["statusCode"] = 400
        output["body"] = json.dumps({"code": 400, "message": msg})

        return output

    try:
        words = parse_words(hocr)
    except Exception as e:
        msg = f"error parsing hocr: {e}"
        logger.error(msg)

        output["statusCode"] = 400
        output["body"] = json.dumps({"code": 400, "message": msg})

        return output

    try:
        blob = make_blob(words)
    except Exception as e:
        msg = f"error creating blob-of-text string: {e}"
        logger.error(msg)

        output["statusCode"] = 400
        output["body"] = json.dumps({"code": 400, "message": msg})

        return output

    try:
        if len(blob) == 0:
            return {"statusCode": 200, "body": {"entities": []}}
        entities = comprehend_entities(blob)
        #print("=====================ENTITIES INFO========================")
        #print(entities)
        #print("=====================ENDED========================")
    except Exception as e:
        msg = f"error retreiving entities: {e}"
        logger.error(msg)

        output["statusCode"] = 400
        output["body"] = json.dumps({"code": 400, "message": msg})

        return output

    try:
        zipped = zip_words_entities(words, entities)
        zipped["version"] = tagger_version

        #print(f"successfully tagged entities: {zipped}")

        return {"statusCode": 200, "body": json.dumps(zipped)}

    except Exception as e:
        msg = f"error combining words and entities: {e}"
        logger.error(msg)

        output["statusCode"] = 400
        output["body"] = json.dumps({"code": 400, "message": msg})

        return output


def append_ssn_formats(text,entities):
    matches = []
    pattern = re.compile("(\d{3}-\d{2}|\*{3}-\*{2}|\#{3}-\#{2}|x{3}-x{2}|Xx{2}-x{2})-\d{4}")
    try:
        words = text.split()
        entityTexts = [d['Text'] for d in entities]
        for each in words:
            if bool(pattern.match(each)) & (each not in entityTexts):
                print("Match: ",  each)
                matchIndex = text.find(each)
                entities.append({"Score": 1.0 , "Type": "OTHER", "Text": each, "BeginOffset": matchIndex, "EndOffset": matchIndex+len(each)})          
    except Exception as e:
        msg = f"error: SSN regex matching failed: {e}"
        logger.error(msg)
        return entities
    return entities