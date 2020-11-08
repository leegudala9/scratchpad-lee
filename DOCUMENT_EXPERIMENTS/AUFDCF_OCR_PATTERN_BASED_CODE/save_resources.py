import json
import sys
import pandas as pd
import boto3
from get_resources import (
    get_athena_bucket,
    get_rid_df,
    get_found_results,
    get_last_rows,
    get_ocr,
    get_table_result
)

import os
import traceback


# AWS RESOURCES
dynamo = boto3.resource("dynamodb")
table = dynamo.Table("MetadataTable")
athena_client = boto3.client("athena")
s3resource = boto3.resource("s3")
bucket_name = os.getenv("BUCKET_NAME")
athena_bucket = get_athena_bucket(s3resource)

version = sys.argv[1]
rids = sys.argv[2:]

for rid in rids:
    results = []
    print(rid)
    json_name = f"found_entities/{rid}.json"
    csv_name = f"last_row_csvs/{rid}.csv"
    ocr_name = f"ocr/{rid}.json"

    if version == "1":
        print("Version 1, grabbing last_rows of ote")
        try:
            rid_df = get_rid_df(rid, athena_client, athena_bucket)
            last_rows_df = get_last_rows(rid_df)
            last_rows_df.to_csv(csv_name, index=False)

            with open("lexicon_mapping.txt", "a") as f:
                for _, row in last_rows_df.iterrows():
                    f.write(f"{row.entity_type}#{row.datapoint}\n")
        except:
            pd.DataFrame([]).to_csv(csv_name)
            print("bad rid", rid)
            traceback.print_exc()
            continue

    # Regardless of version get these
    try:
        print("Getting Found Entities")
        table_result = get_table_result(rid, table)
        found_entities = get_found_results(table_result, s3resource, bucket_name)
        print("Getting OCR")
        ocr = get_ocr(table_result, s3resource, bucket_name)
        if ocr is None:
            continue

        with open(json_name, "w") as f:
            f.write(json.dumps(found_entities))

        with open(ocr_name, "w") as f:
            f.write(json.dumps(ocr))
    except:
        print("missing resources", rid)
        traceback.print_exc()
        continue
