import sys
import json
import pandas as pd
import boto3
import os
from keywords import get_all_pages_keywords
from vectorization_functions import (
    produce_found_vector_row,
    finalize_vectors,
    remove_found_entity_matches
)

# AWS RESOURCES
dynamo = boto3.resource("dynamodb")
table = dynamo.Table("MetadataTable")
s3resource = boto3.resource("s3")
bucket_name = os.getenv("BUCKET_NAME")

doctype = sys.argv[1]
rids = sys.argv[2:]

found_entities_dir = "found_entities"
last_rows_dir = "last_row_csvs"
ocr_dir = "ocr"
vectors_dir = "vectors"
entity_match_dir = "entity_match_csvs"
remaining_fe_dir = "remaining_found_entities"
low_threshold_dist = 0.2


for rid in rids:
    print(rid)
    lrd_filename = f"{last_rows_dir}/{rid}.csv"
    vec_filename = f"{vectors_dir}/{rid}.csv"
    ocr_filename = f"{ocr_dir}/{rid}.json"
    em_filename = f"{entity_match_dir}/{rid}.csv"
    json_filename = f"json_out/{rid}.json"
    remaining_fe_filename = f"{remaining_fe_dir}/{rid}.json"

    # FOUND ENTITIES
    if os.path.isfile(remaining_fe_filename):
        with open(remaining_fe_filename) as f:
            found_entities = json.load(f)
    else:
        found_entites = []

    last_row_content = []
    v2_content = []
    if os.path.isfile(lrd_filename):
        last_rows_df = pd.read_csv(lrd_filename)

        last_row_content = last_rows_df.content.values.tolist()
    elif os.path.isfile(json_filename):
        with open(json_filename) as f:
            v2_data = json.load(f)
        v2_content = [res["value"]["value"] for res in v2_data["Results"]]

    # Remove any low_threshold matching content
    found_entities = remove_found_entity_matches(
        found_entities, last_row_content + v2_content, low_threshold_dist
    )

    if not os.path.isfile(ocr_filename):
        print(lrd_filename)
        print("we do not have necessary files")
        continue

    # OCR
    with open(ocr_filename) as f:
        ocr = json.load(f)

    # GET KEYWORD COORDINATES
    keyword_coordinates = get_all_pages_keywords(ocr, doctype)

    # Start a NEW vectors array
    # (csv, named by remember_id, and put the remember_id in the line)
    vectors = []

    ## Vectorize the rest of the found entities as zeros
    for fe in found_entities:
        vectors.append(produce_found_vector_row(rid, fe, doctype, keyword_coordinates))

    # Do all the remaining vector stuff
    try:
        fsdf = pd.DataFrame.from_dict(vectors, orient="columns")
    except:
        print("error finalizing")
        continue

    fsdf = finalize_vectors(fsdf)

    # Save the vectors file
    fsdf.to_csv(f"{vectors_dir}/{rid}_zero.csv", index=False)
