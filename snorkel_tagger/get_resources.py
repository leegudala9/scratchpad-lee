import json
import sys
import time
import pandas as pd
from boto3.dynamodb.conditions import Key


def get_athena_bucket(s3resource):
    # Get athena bucket name
    all_buckets = list(s3resource.buckets.all())
    athena_buckets = [x.name for x in all_buckets if "athena" in x.name]
    if len(athena_buckets):
        return athena_buckets[0]
    print("No athena buckets available")
    sys.exit()


def fetchall_athena(query_string, client, athena_bucket, get_first_col=False):
    query_id = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": "reports_neo"},
        ResultConfiguration={"OutputLocation": f"s3://{athena_bucket}"},
    )["QueryExecutionId"]
    query_status = None
    while query_status == "QUEUED" or query_status == "RUNNING" or query_status is None:
        query_status = client.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]["State"]
        if query_status == "FAILED" or query_status == "CANCELLED":
            raise Exception(
                'Athena query with the string "{}" failed or was cancelled'.format(
                    query_string
                )
            )
        time.sleep(10)
    results_paginator = client.get_paginator("get_query_results")
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_id, PaginationConfig={"PageSize": 1000}
    )
    results = []
    data_list = []
    for results_page in results_iter:
        for row in results_page["ResultSet"]["Rows"]:
            data_list.append(row["Data"])
    if get_first_col:
        start_index = 0
    else:
        start_index = 1
    for datum in data_list[start_index:]:
        results.append([x.get("VarCharValue") for x in datum])
    return [tuple(x) for x in results]


def add_key(tup, dct):
    dct[tup[0]] = tup[1]
    return dct


def get_rid_df(rid, athena_client, athena_bucket):
    table_columns = [
        "coordinates",
        "content_type",
        "page_number",
        "content",
        "id",
        "datapoint",
        "entity_type",
    ]
    prefix = '"reports_neo"."operator_tagged_entities".'
    selectors = ", ".join(f"{prefix}{col}" for col in table_columns)
    query = f"""
        SELECT
            "reports_neo"."entity_classification"."_remember_id" as  rid,
            "reports_neo"."entity_classification"."data" as ec_data,
            {selectors}

        FROM "reports_neo"."operator_tagged_entities"
        LEFT JOIN "reports_neo"."entity_classification"
        ON "reports_neo"."entity_classification"."_remember_id" = "reports_neo"."operator_tagged_entities"."_remember_id"

        WHERE "operator_tagged_entities"."_remember_id" = '{rid}'

        ORDER BY
            "reports_neo"."operator_tagged_entities"."_remember_id",
            "reports_neo"."operator_tagged_entities"."datapoint",
            "reports_neo"."operator_tagged_entities"."selecttime"
    """
    ent_class_results = fetchall_athena(query, athena_client, athena_bucket)
    df = pd.DataFrame(ent_class_results)
    df.columns = ["rid", "ec_data"] + table_columns
    return df


def get_item_from_dyn(key, dyn_result):
    if key in dyn_result:
        return json.loads(dyn_result[key]).get("data")
    return None


def get_s3_object(s3resource, bucket_name, s3url):
    key_name = s3url.replace("S3://", "").replace(bucket_name, "")[1:]
    return json.loads(s3resource.Object(bucket_name, key_name).get()["Body"].read())


def get_table_result(rid, table):
    dyn_result = table.query(KeyConditionExpression=Key("_remember_id").eq(rid))
    table_result = dyn_result["Items"][0]
    return table_result


def get_found_results(table_result, s3resource, bucket_name):
    # Get all the stuff form dynamo & s3
    agg_form_ent_url = get_item_from_dyn(
        "_aggregated_formatted_entities_path", table_result
    )
    if agg_form_ent_url is None:
        return []

    form_entities = get_s3_object(s3resource, bucket_name, agg_form_ent_url)

    # Flatten Form Entities
    found_entities = [
        add_key(("ENTITY_TYPE", typ), ent)
        for typ, ent_list in form_entities.items()
        for ent in ent_list
    ]
    return found_entities


def get_ocr(table_result, s3resource, bucket_name):
    agg_ocr_url = get_item_from_dyn("_aggregated_ocr", table_result)
    if agg_ocr_url is None:
        return None
    ocr = get_s3_object(s3resource, bucket_name, agg_ocr_url)
    return ocr


def get_last_rows(df):
    return pd.DataFrame(
        [group_df.iloc[-1, :] for dp, group_df in df.groupby(by="datapoint")]
    )


def get_rids(query, cols, filename, athena_client, athena_bucket):
    results = fetchall_athena(query, athena_client, athena_bucket)
    print(len(results))
    df = pd.DataFrame(results, columns=cols)
    df.to_csv(filename, index=False, header=False)
    return df
