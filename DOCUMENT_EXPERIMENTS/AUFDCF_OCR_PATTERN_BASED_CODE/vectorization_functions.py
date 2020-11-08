import pandas as pd
from overlaps import overlap_coordinates
from distances import full_distance
import usaddress
from utilities import get_attr_list
from keywords import get_keywords
from feature_generation import (
    coerce_currency,
    common_date_patterns,
    dateable,
    calculate_currency_zscores,
    coerce_date,
    first_date,
    date_diff,
    day_of_week,
    day_of_month,
    calculate_distance_and_angle,
    find_closest_coordinates,
    get_empty_row,
)


def remove_currency_junk(el):
    if type(el) != str:
        return el
    return el.replace("$", "").replace(",", "")


def found_entity_match(entity, content, threshold):
    fe_content = entity["CONTENT"]["CONTENT"]["raw_text_unformatted"]
    dist = full_distance(
        remove_currency_junk(fe_content), remove_currency_junk(content)
    )
    return dist >= threshold


def remove_found_entity_matches(found_entities, content_list, threshold):
    for content in content_list:
        for fe in found_entities:
            if found_entity_match(fe, content, threshold):
                print(
                    "I AM REMOVING ",
                    content,
                    fe["ENTITY_TYPE"],
                    fe["CONTENT"]["CONTENT"]["value"],
                )
        found_entities = [
            fe
            for fe in found_entities
            if not found_entity_match(fe, content, threshold)
        ]
    return found_entities


def remaining_ec_data(ec_data, last_rows_df):
    tagged_labels = last_rows_df.datapoint.tolist()
    ec_data = {k: v for k, v in ec_data.items() if k not in tagged_labels}
    return ec_data


def get_coord_ent_tups(found_entities):
    # Returns tups like (ent_id, coordinates, entity_content)
    return [
        (
            ent["ENTITY_ID"],
            get_attr_list(["CONTENT", "CONTENT", "coordinates"], ent),
            remove_currency_junk(
                get_attr_list(["CONTENT", "CONTENT", "raw_text_unformatted"], ent)
            ),
            get_attr_list(["ENTITY_TYPE"], ent),
        )
        for ent in found_entities
    ]


def get_coord_ocr_tups(ocr):
    # Returns tups like (ocr_token, coordinates, ocr_word)
    # inside a dict like {'1' : [(), ()]}
    ocr_tups = {}
    for page, page_ocr in ocr.items():
        page_num = page.replace("page_", "")
        ocr_tups[page_num] = [
            (word_id, word_dict["coordinates"], word_dict["text"])
            for word_id, word_dict in page_ocr["words"].items()
        ]
    return ocr_tups


def overlaps_with_ent_coordinates(coor, content, ent_type, ent_tups_lst):
    return [
        (ent_id, found_co, found_content, found_entity_type)
        for ent_id, found_co, found_content, found_entity_type in ent_tups_lst
        if overlap_coordinates(coor, found_co)
    ]


def overlaps_with_ocr_coordinates(coor, content, ent_type, ocr_tups_lst):
    return [
        (word_id, word_co, word_text)
        for word_id, word_co, word_text in ocr_tups_lst
        if overlap_coordinates(coor, word_co)
    ]


def get_default_page_num(val, key_list):
    try:
        key_str = str(int(val))
        if key_str in key_list:
            return key_str
        else:
            return key_list[0]
    except:
        return key_list[0]


def get_ocr_matches(rid, coordinate_rows, ocr):
    results = []
    coordinate_ocr = get_coord_ocr_tups(ocr)
    for _, row in coordinate_rows.iterrows():
        row_page_num = get_default_page_num(
            row.page_number, list(coordinate_ocr.keys())
        )
        row_coord = row.coordinates
        row_cont = row.content
        row_ent_type = row.entity_type
        row_id = row.id
        for tup in overlaps_with_ocr_coordinates(
            row_coord, row_cont, row_ent_type, coordinate_ocr.get(row_page_num)
        ):
            word_id, word_co, word_text = tup
            dist = full_distance(
                remove_currency_junk(row_cont), remove_currency_junk(word_text)
            )
            results.append(
                [
                    rid,
                    row_id,
                    word_id,
                    row_coord,
                    word_co,
                    row.datapoint,
                    row.entity_type,
                    "ocr",
                    row_cont,
                    word_text,
                    dist,
                ]
            )
    # Wrap up all the results
    if len(results) == 0:
        return None
    df = pd.DataFrame(results)
    df.columns = [
        "rid",
        "row_id",
        "entity_id",
        "coord",
        "ent_coord",
        "label",
        "ote_entity_type",
        "found_entity_type",
        "content",
        "ent_content",
        "distance",
    ]
    return df


def get_entity_matches(rid, coordinate_rows, found_entities):
    results = []
    coordinate_ents = get_coord_ent_tups(found_entities)
    for _, row in coordinate_rows.iterrows():
        row_coord = row.coordinates
        row_cont = row.content
        row_ent_type = row.entity_type
        row_id = row.id
        for tup in overlaps_with_ent_coordinates(
            row_coord, row_cont, row_ent_type, coordinate_ents
        ):
            ent_id, found_co, found_content, found_entity_type = tup
            dist = full_distance(
                remove_currency_junk(row_cont), remove_currency_junk(found_content)
            )
            if dist is None:
                dist = 0
            results.append(
                [
                    rid,
                    row_id,
                    ent_id,
                    row_coord,
                    found_co,
                    row.datapoint,
                    row.entity_type,
                    found_entity_type,
                    row_cont,
                    found_content,
                    dist,
                ]
            )
    # Wrap up all the results
    if len(results) == 0:
        return None
    df = pd.DataFrame(results)
    df.columns = [
        "rid",
        "row_id",
        "entity_id",
        "coord",
        "ent_coord",
        "label",
        "ote_entity_type",
        "found_entity_type",
        "content",
        "ent_content",
        "distance",
    ]
    return df


def get_ec_found_item(ec_item):
    return {
        "ENTITY_ID": ec_item.get("entity_id"),
        "PAGE": ec_item.get("page"),
        "CONTENT": {
            "CONTENT": {
                "word_ids": [],
                "coordinates": ec_item.get("coordinates"),
                "raw_text_unformatted": "",
                "value": "",
            }
        },
        "CONFIDENCE": 1,
        "ENTITY_TYPE": "",
    }


def produce_found_vector_row(rid, entity, doctype, keyword_coordinates, label=0):
    addr_mapper = {
        "AddressNumber": "ROUTE",
        "StreetName": "ROUTE",
        "StreetNamePostType": "ROUTE",
        "StreetNamePreDirectional": "ROUTE",
        "PlaceName": "CITY",
        "StateName": "STATE",
        "ZipCode": "ZIP",
    }
    datapoint = entity["CONTENT"]["CONTENT"]["value"]
    entity_type = entity["ENTITY_TYPE"]
    if entity_type == "ADDRESS":
        parsed_address = usaddress.parse(datapoint)
        ent_type_set = list(
            set(addr_mapper.get(tup[1], "ADDRESS") for tup in parsed_address)
        )
        if len(ent_type_set) == 1:
            entity_type = ent_type_set[0]
    if entity_type == "ADDRESS":
        print("# ", datapoint)

    keywords = get_keywords(doctype)
    row = get_empty_row(keywords)
    page_num = entity["PAGE"]

    page_name = f"page_{page_num}"
    if page_name not in keyword_coordinates:
        page_name = list(keyword_coordinates.keys())[0]

    keywords_on_the_page = keyword_coordinates[page_name]
    x_normal, y_normal = (
        keywords_on_the_page["page_size"]["x"],
        keywords_on_the_page["page_size"]["y"],
    )

    center_coordinates = {"x0": 0, "y0": 0, "x1": x_normal, "y1": y_normal}

    row["rid"] = rid
    row["label"] = label
    row["entity_id"] = entity["ENTITY_ID"]
    row["page_number"] = float(page_num)
    row["entity_type"] = entity_type
    row["doctype"] = doctype
    row["currency"] = coerce_currency(datapoint, entity_type)
    row["date"] = dateable(
        entity["CONTENT"]["CONTENT"]["value"], common_date_patterns, entity_type
    )

    # Try to match up with tagged entities
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
            keyword_coordinates_lst, datapoint_coordinates, x_normal, y_normal,
        )
        row[f"dfk_{keyword_value}"], row[f"afk_{keyword_value}"] = (
            vector_keyword["distance"],
            vector_keyword["angle"],
        )
    return row


def get_default_page_name(val, key_list):
    try:
        key_str = str(int(val))
        page_name = f"page_{key_str}"
        if page_name in key_list:
            return page_name
        else:
            return key_list[0]

    except:
        return key_list[0]


def produce_tagged_vector_row(rid, last_row, doctype, keyword_coordinates):
    page_name = get_default_page_name(
        last_row["page_number"], list(keyword_coordinates.keys())
    )
    keywords = get_keywords(doctype)
    row = get_empty_row(keywords)

    if page_name not in keyword_coordinates:
        page_name = list(keyword_coordinates.keys())[0]

    keywords_on_the_page = keyword_coordinates[page_name]
    x_normal, y_normal = (
        keywords_on_the_page["page_size"]["x"],
        keywords_on_the_page["page_size"]["y"],
    )
    center_coordinates = {"x0": 0, "y0": 0, "x1": x_normal, "y1": y_normal}

    datapoint = last_row["content"]

    row["rid"] = rid
    row["label"] = last_row["datapoint"]
    row["entity_id"] = last_row["id"]
    row["page_number"] = float(page_name.replace("page_", ""))
    row["doctype"] = doctype
    row["entity_type"] = last_row["entity_type"]

    row["currency"] = coerce_currency(last_row["content"], row["entity_type"])
    row["date"] = dateable(
        last_row["content"], common_date_patterns, row["entity_type"]
    )

    if type(last_row["coordinates"]) != str:
        return False

    x0, y0, x1, y1 = tuple(float(x) for x in last_row["coordinates"].split(" "))

    datapoint_coordinates = {"y0": y0, "x0": x0, "y1": y1, "x1": x1}

    distance_af_center = calculate_distance_and_angle(
        center_coordinates, datapoint_coordinates, x_normal, y_normal
    )

    row["currency"] = coerce_currency(datapoint, row["entity_type"])
    row["date"] = dateable(
        last_row["content"], common_date_patterns, row["entity_type"]
    )

    distance_af_center = calculate_distance_and_angle(
        center_coordinates, datapoint_coordinates, x_normal, y_normal
    )

    row["df_center"] = distance_af_center["distance"]
    row["af_center"] = distance_af_center["angle"]

    for keyword_value, keyword_coordinates_lst in keywords_on_the_page[
        "keyword_coordinates"
    ].items():
        vector_keyword = find_closest_coordinates(
            keyword_coordinates_lst, datapoint_coordinates, x_normal, y_normal,
        )
        row[f"dfk_{keyword_value}"] = vector_keyword["distance"]
        row[f"afk_{keyword_value}"] = vector_keyword["angle"]

    return row


def finalize_vectors(fsdf):
    try:
        fsdf["currency_zscore"] = calculate_currency_zscores(fsdf["currency"].tolist())

        dates = [coerce_date(dt, common_date_patterns) for dt in fsdf["date"]]
        fd = first_date(dates)

        fsdf["date_diff"] = [date_diff(fd, dt) for dt in dates]
        fsdf["day_of_week"] = [day_of_week(dt) for dt in dates]
        fsdf["day_of_month"] = [day_of_month(dt) for dt in dates]

        fsdf.drop(["date"], axis=1, inplace=True)
    except:
        pass

    return fsdf


def get_fe_tuples(found_entities):
    get_word_id_set = lambda fe: set(
        get_attr_list(["CONTENT", "CONTENT", "word_ids"], fe)
    )
    return [
        (
            get_word_id_set(fe),
            get_attr_list(["CONTENT", "CONTENT", "raw_text_unformatted"], fe),
            fe["ENTITY_ID"],
            fe["ENTITY_TYPE"],
        )
        for fe in found_entities
    ]


def get_entity_type_by_id(fe_tups, entity_id):
    id_lst = [tup for tup in fe_tups if tup[2] == entity_id]
    if len(id_lst) > 0:
        return id_lst[0][3]
    return None


def get_intersections(word_ids, fe_tups):
    return [fe for fe in fe_tups if len(fe[0].intersection(word_ids)) > 0]


def resolve_entity_type(result, found_entities):
    entity_type = None
    row = Row(result)
    # word_id_set, text, entity_id, entity_type
    fe_tuples = get_fe_tuples(found_entities)
    intersection_fe = get_intersections(row.ocr_words, fe_tuples)
    intersection_types = set(tup[3] for tup in intersection_fe)

    # Do we have an entity_id?
    if row.entity_id is not None:
        entity_type = get_entity_type_by_id(fe_tuples, row.entity_id)
        if entity_type is not None:
            return entity_type

    # Do we have intersection with ocr words?
    if len(intersection_fe) == 0:
        return None

    elif len(intersection_types) == 1:
        return intersection_types.pop()

    else:
        if "ADDRESS" in intersection_types and "COMPANY NAME" in intersection_types:
            return "COMPANY NAME"
        elif "CURRENCY" in intersection_types and "COMPANY NAME" in intersection_types:
            return "ADDRESS"

    return row.entity_type


def produce_v2_vector_row(rid, result, doctype, keyword_coordinates, found_entities):

    if "ENTITY TYPE" not in result:
        return None

    found_entity_type = resolve_entity_type(result, found_entities)
    entity_type = result.get("CHILD ENTITY TYPE", result.get("ENTITY TYPE"))

    keywords = get_keywords(doctype)
    row = get_empty_row(keywords)

    datapoint = result["value"]["value"]

    row["rid"] = rid
    row["label"] = result["NAME"]
    page_num = get_attr_list(["selection_input", "page", "document_index"], result)
    page_num += 1
    page_name = f"page_{page_num}"

    if page_name not in keyword_coordinates:
        page_name = list(keyword_coordinates.keys())[0]

    keywords_on_the_page = keyword_coordinates[page_name]
    x_normal, y_normal = (
        keywords_on_the_page["page_size"]["x"],
        keywords_on_the_page["page_size"]["y"],
    )

    center_coordinates = {"x0": 0, "y0": 0, "x1": x_normal, "y1": y_normal}

    row["entity_id"] = None
    row["page_number"] = float(page_num)
    row["entity_type"] = entity_type
    row["doctype"] = doctype

    row["currency"] = coerce_currency(datapoint, row["entity_type"])

    row["date"] = dateable(row["datapoint"], common_date_patterns, row["entity_type"])

    datapoint_coordinates = get_attr_list(["selection_input", "pos_original"], result)

    # No coordinates? No vector
    if datapoint_coordinates is None:
        return None

    distance_af_center = calculate_distance_and_angle(
        center_coordinates, datapoint_coordinates, x_normal, y_normal
    )

    row["df_center"] = distance_af_center["distance"]
    row["af_center"] = distance_af_center["angle"]

    for keyword_value, keyword_coordinates_lst in keywords_on_the_page[
        "keyword_coordinates"
    ].items():
        vector_keyword = find_closest_coordinates(
            keyword_coordinates_lst, datapoint_coordinates, x_normal, y_normal,
        )
        row[f"dfk_{keyword_value}"] = vector_keyword["distance"]
        row[f"afk_{keyword_value}"] = vector_keyword["angle"]

    return row


class Row:
    def __init__(self, result):
        self.legit = True
        self.content = result.get("value", {}).get("value", None)
        self.source = result.get("value", {}).get("source", None)
        if self.content is None:
            self.legit = False
            return

        self.entity_id = None
        if self.source in [
            "aivaextracteddatapoint",
            "aivataggedentity",
            "operatoreditdatapoint",
            "operatortaggedentity",
        ]:
            self.entity_id = result.get(self.source, {}).get("entity_id")

        ocr = result.get("selection_input", {}).get("ocr", [])
        if ocr is None:
            ocr = []

        self.ocr_words = set(word.get("id", "") for word in ocr)

        self.entity_type = result.get("CHILD ENTITY TYPE", result.get("ENTITY TYPE"))
        self.datapoint = result.get("NAME")
        self.page_number = result["selection_input"]["page"]["document_index"] + 1
        self.id = "v2"
        coo = result["selection_input"].get(
            "pos_original", {"x0": 0, "x1": 0, "y0": 0, "y1": 0}
        )
        self.coordinates = f"{coo['x0']} {coo['y0']} {coo['x1']} {coo['y1']}"


class PseudoDf:
    def __init__(self, results):
        self.rows = [Row(result) for result in results]
        self.rows = [row for row in self.rows if row.legit]

    def iterrows(self):
        return enumerate(self.rows)
