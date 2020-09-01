import json
import usaddress
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def replace_page_number(entities, page):
    entities_string = json.dumps(entities)
    return json.loads(entities_string.replace("word_1", "word_" + str(page)))


def map_entities(entities):
    mapped_entities = {}
    for entity in entities:
        entity_id = entity["entity_id"]
        if entity_id in mapped_entities:
            mapped_entities[entity_id].append(entity)
        else:
            mapped_entities[entity_id] = [entity]
    return mapped_entities


def reduce_address_entities(entity_id, entity):
    rules = {
        "PERSON": ["Recipient"],
        "ROUTE": [
            "AddressNumber",
            "StreetNamePreDirectional",
            "StreetName",
            "StreetNamePostType",
        ],
        "CITY": ["PlaceName"],
        "STATE": ["StateName"],
        "ZIPCODE": ["ZipCode"],
    }
    indices = {"ROUTE": [], "CITY": [], "STATE": [], "ZIPCODE": [], "PERSON": []}
    combined_address = (" ".join([word["text"] for word in entity])).lstrip()
    parsed_address = usaddress.parse(combined_address)
    split_entities = {}
    for i in range(0, len(entity)):
        for subtype in rules.keys():
            try:
                if parsed_address[i][1] in rules[subtype]:
                    entity[i]["entity_type"] = subtype
                    indices[subtype].append(i)
            except:
                logger.error(
                    f"{parsed_address} from entity ---> {entity} at Index of the entity -->  {i} failed to parse"
                )
    for subtype in indices.keys():
        if len(indices[subtype]) > 0:
            split_entities[f"{entity_id}-{subtype}"] = entity[
                indices[subtype][0] : indices[subtype][-1] + 1
            ]
    return split_entities


def reduce_entities(entities):
    splitted, reduced_entities = {}, {}
    for entity_id, entity in entities.items():
        if entity[0]["entity_type"] == "LOCATION":
            splitted.update(reduce_address_entities(entity_id, entity))
        else:
            splitted[entity_id] = entity
    for entity_id, entity in splitted.items():
        reduced_entity = entity[0]
        reduced_entities = update_reduced_entities(
            entity_id, entity, reduced_entity, reduced_entities
        )
    return reduced_entities


def update_reduced_entities(entity_id, entity, reduced_entity, reduced_entities):
    word_ids, text, confidence = [], "", 0.0
    x0, y0, x1, y1 = [], [], [], []
    if reduced_entity["entity_type"] == "PERSON":
        count = 0
        for word in entity:
            count += 1
            uuid_string = f"{entity_id}_{str(count)}"
            word["entity_id"] = uuid_string
            word["word_ids"] = [word["word_id"]]
            reduced_entities[uuid_string] = word
    else:
        for word in entity:
            bounding_box = word["bounding_box"]
            text = f'{text} {word["text"]}'
            word_ids.append(word["word_id"])
            confidence = confidence + word["confidence"]
            x0.append(bounding_box[0])
            y0.append(bounding_box[1])
            x1.append(bounding_box[2])
            y1.append(bounding_box[3])
        reduced_entity.update(
            {
                "text": text.lstrip(),
                "word_ids": word_ids,
                "confidence": confidence / len(entity),
                "bounding_box": [min(x0), min(y0), max(x1), max(y1)],
                "entity_id": entity_id,
            }
        )
        reduced_entities[entity_id] = reduced_entity
    return reduced_entities


def entity_cleaning(entities):
    for entity in entities:
        if entity["entity_type"] in ("OTHER", "QUANTITY"):
            try:
                float(
                    entity["text"]
                    .strip("$%,")
                    .replace(",", "")
                    .replace("  ", "")
                    .replace(" ", "")
                )
                entity["entity_type"] = "QUANTITY"

            except:
                entity["entity_type"] = "TO_BE_REMOVED"

    cleaned = [i for i in entities if not (i["entity_type"] == "TO_BE_REMOVED")]
    return cleaned
