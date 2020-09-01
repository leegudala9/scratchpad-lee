import sys
import json
import pandas as pd
import boto3
import os
from utilities import reduce
from keywords import get_all_pages_keywords
from vectorization_functions import (
    produce_v2_vector_row,
    finalize_vectors,
    get_entity_matches,
    get_ocr_matches,
    PseudoDf,
)

# AWS RESOURCES
dynamo = boto3.resource("dynamodb")
table = dynamo.Table("MetadataTable")
athena_client = boto3.client("athena")
s3resource = boto3.resource("s3")
bucket_name = os.getenv("BUCKET_NAME")

doctype = sys.argv[1]
rids = sys.argv[2:]

json_dir = "json_out"
ocr_dir = "ocr"
vectors_dir = "vectors"
fe_dir = "found_entities"
em_dir = "entity_match_csvs"
remaining_dir = "remaining_found_entities"

high_threshold_dist = 0.5

for rid in rids:
    print(rid)

    # Get ocr, found_entities, and json_result
    try:
        json_file = f"{json_dir}/{rid}.json"
        with open(json_file) as f:
            json_obj = json.load(f)
            results = json_obj.get("Results", [])

        ocr_name = f"{ocr_dir}/{rid}.json"
        with open(ocr_name) as f:
            ocr = json.load(f)

        fe_name = f"{fe_dir}/{rid}.json"
        with open(fe_name) as f:
            found_entities = json.load(f)
    except:
        print("some json file is missing")
        continue

    # GET KEYWORD COORDINATES
    kw_coordinates = get_all_pages_keywords(ocr, doctype)
    result_rows = PseudoDf(results)

    entity_match_df = get_entity_matches(rid, result_rows, found_entities)
    ocr_match_df = get_ocr_matches(rid, result_rows, ocr)

    if entity_match_df is None:
        entity_match_df = pd.DataFrame([])

    if ocr_match_df is None:
        entity_match_df = pd.DataFrame([])

    full_match_df = pd.concat([entity_match_df, ocr_match_df])

    if len(full_match_df) > 0:
        # Save this as entity_match csv
        full_match_df.to_csv(f"{em_dir}/{rid}.csv", index=False)

    # Get Vectors
    vectors = reduce(
        lambda acc, new: acc
        + [produce_v2_vector_row(
            rid, new, doctype, kw_coordinates, found_entities)],
        results,
        [],
    )

    vectors = [row for row in vectors if row is not None]

    # Do all the remaining vector stuff
    try:
        fsdf = pd.DataFrame.from_dict(vectors, orient="columns")
    except:
        print("error finalizing")
        continue

    fsdf = finalize_vectors(fsdf)

    # Save the vectors file
    fsdf.to_csv(f"{vectors_dir}/{rid}.csv", index=False)

    # Before we go, remove matched entities and save remaining
    added_content = []
    for _, ent_match_row in full_match_df.iterrows():
        # If the distance is greater than, or equal to the threshold.
        # Or if this row has a word id
        # Vectorize the row
        if ent_match_row.distance >= high_threshold_dist:

            # Regardless, remove from found entities
            found_entities = [
                fe
                for fe in found_entities
                if fe["ENTITY_ID"] != ent_match_row.entity_id
            ]
    with open(f"{remaining_dir}/{rid}.json", "w") as f:
        json.dump(found_entities, f)
