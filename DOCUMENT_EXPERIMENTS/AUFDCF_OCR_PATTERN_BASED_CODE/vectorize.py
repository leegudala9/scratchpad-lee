import sys
import json
import pandas as pd
import boto3
import os
import re
from utilities import first
from get_resources import get_athena_bucket
from keywords import get_all_pages_keywords
from vectorization_functions import (
    get_ec_found_item,
    produce_found_vector_row,
    produce_tagged_vector_row,
    get_entity_matches,
    get_ocr_matches,
    finalize_vectors,
)

# AWS RESOURCES
dynamo = boto3.resource("dynamodb")
table = dynamo.Table("MetadataTable")
athena_client = boto3.client("athena")
s3resource = boto3.resource("s3")
bucket_name = os.getenv("BUCKET_NAME")
athena_bucket = get_athena_bucket(s3resource)


doctype = sys.argv[1]
print("Doctype ", doctype)
rids = sys.argv[2:]

found_entities_dir = "found_entities"
last_rows_dir = "last_row_csvs"
ocr_dir = "ocr"
vectors_dir = "vectors"
entity_match_dir = "entity_match_csvs"
remaining_fe_dir = "remaining_found_entities"
high_threshold_dist = 0.5


def try_delete(dct, del_key):
    return {k: v for k, v in dct.items() if k != del_key}


def match_ent_id(thing):
    if type(thing) != str:
        return False
    return re.match("[a-z0-9]{8}-", thing) is not None


def match_word_id(thing):
    if type(thing) != str:
        return False
    return "word_" in thing


def good_coordinates(thing):
    if type(thing) != str:
        return False
    return len(thing.split(" ")) == 4


for rid in rids:
    print(rid)
    fe_filename = f"{found_entities_dir}/{rid}.json"
    lrd_filename = f"{last_rows_dir}/{rid}.csv"
    vec_filename = f"{vectors_dir}/{rid}.csv"
    ocr_filename = f"{ocr_dir}/{rid}.json"
    remaining_fe_filename = f"{remaining_fe_dir}/{rid}.json"

    # Get the last rows df, ec_data & found_entities, if no ec_data, then {}
    last_rows_df = pd.read_csv(f"{last_rows_dir}/{rid}.csv")

    if not os.path.isfile(lrd_filename) or not os.path.isfile(ocr_filename):
        print("we do not have necessary files")
        continue

    # OCR
    with open(ocr_filename) as f:
        ocr = json.load(f)

    # FOUND ENTITIES
    if os.path.isfile(fe_filename):
        with open(fe_filename) as f:
            found_entities = json.load(f)
    else:
        found_entites = []

    # EC DATA
    try:
        ec_data = json.loads(first(last_rows_df.ec_data.unique()))
    except:
        ec_data = {}

    # GET KEYWORD COORDINATES
    keyword_coordinates = get_all_pages_keywords(ocr, doctype)

    # Start a vectors array
    # (csv, named by remember_id, and put the remember_id in the line)
    vectors = []

    """
    Handle Entity Classification Data
    """

    # Remove tagged Entities from Entity Classifications
    ec_data = {
        k: v
        for k, v in ec_data.items()
        if k not in last_rows_df.datapoint.values.tolist()
    }

    # Remove all remaining entity classification from found_entities
    # (by label and entity_type)
    for label, ec_item in ec_data.items():
        ec_found_entity = first(
            [fe for fe in found_entities if fe["ENTITY_ID"] == ec_item["entity_id"]]
        )
        if ec_found_entity is None:
            ec_found_entity = get_ec_found_item(ec_item)
        found_entities = [
            fe for fe in found_entities if fe["ENTITY_ID"] != ec_item["entity_id"]
        ]
        vectors.append(
            produce_found_vector_row(
                rid, ec_found_entity, doctype, keyword_coordinates, label
            )
        )

    """
    Handle Operator Tagged Entities with an entity_id
    """

    # For each last_row with an entity_id in id
    entity_id_rows = last_rows_df[last_rows_df.id.apply(match_ent_id)]
    for _, row in entity_id_rows.iterrows():
        # Remove from found_entities
        found_entities = [fe for fe in found_entities if fe["ENTITY_ID"] != row.id]

        # Vectorize that row
        vectors.append(
            produce_tagged_vector_row(rid, row, doctype, keyword_coordinates)
        )

    # For all the remaining last_rows (anything with coordinates)
    remaining_rows = last_rows_df[
        last_rows_df.id.apply(lambda iden: not match_ent_id(iden))
    ]

    coordinate_rows = remaining_rows[remaining_rows.coordinates.apply(good_coordinates)]

    # Produce the entity_match df, entity_types match, coordinates overlap
    entity_match_df = get_entity_matches(rid, coordinate_rows, found_entities)
    ocr_match_df = get_ocr_matches(rid, coordinate_rows, ocr)
    full_match_df = pd.concat([entity_match_df, ocr_match_df])

    if entity_match_df is None:
        continue

    # Save this as entity_match csv
    full_match_df.to_csv(f"{entity_match_dir}/{rid}.csv", index=False)

    # For each line of entity_match
    added_content = []
    for _, ent_match_row in full_match_df.iterrows():
        # If the distance is greater than, or equal to the threshold.
        # Or if this row has a word id
        # Vectorize the row
        if (
            match_word_id(ent_match_row.row_id)
            or ent_match_row.distance >= high_threshold_dist
        ) and ent_match_row.content not in added_content:
            new_row = coordinate_rows[
                coordinate_rows.content == ent_match_row.content
            ].iloc[0, :]
            vectors.append(
                produce_tagged_vector_row(rid, new_row, doctype, keyword_coordinates)
            )
            added_content.append(ent_match_row.content)

        # Regardless, remove from found entities
        found_entities = [
            fe for fe in found_entities if fe["ENTITY_ID"] != ent_match_row.entity_id
        ]

    # Save the remaining_found_entities
    with open(remaining_fe_filename, "w") as f:
        json.dump(found_entities, f)

    # Do all the remaining vector stuff
    try:
        fsdf = pd.DataFrame.from_dict(vectors, orient="columns")
    except:
        print("error finalizing")
        continue

    fsdf = finalize_vectors(fsdf)

    # Save the vectors file
    fsdf.to_csv(f"{vectors_dir}/{rid}.csv", index=False)
