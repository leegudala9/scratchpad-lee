import sys
import json
import boto3
from get_resources import get_athena_bucket, get_rids

s3resource = boto3.resource("s3")
athena_client = boto3.client("athena")
athena_bucket = get_athena_bucket(s3resource)

print(athena_bucket)


doctype_str = ""
if len(sys.argv) > 1:
    print("got a specific doctype")
    doctype_str = f"where data like '%{sys.argv[1]}%'"


query_v2 = f"""
SELECT _remember_id as rid, data FROM "reports_neo"."doc_type" {doctype_str}
"""
print(query_v2)

get_doctype = lambda blob: json.loads(blob).get("CONTENT")


all_rids_df = get_rids(
    query_v2, ["rid", "data"], "all_rids.csv", athena_client, athena_bucket
)
print("len_all_rids_df", len(all_rids_df))

all_doctypes = set([get_doctype(val) for val in all_rids_df.data.tolist()])
all_rids_df.data = [get_doctype(val) for val in all_rids_df.data.tolist()]

# Save All doctypes
with open("all_doctypes.txt", "w") as f:
    f.write("\n".join(all_doctypes))

# Save all rids with their doctypes
all_rids_df.to_csv("all_rids.csv", index=False, header=False)

# Get V1 RIDS

query_v1 = f"""
    SELECT
       DISTINCT "reports_neo"."operator_tagged_entities"."_remember_id" as  rid

    FROM "reports_neo"."operator_tagged_entities"
"""

v1_rids_df = get_rids(query_v1, ["rid"], "v1_rids.csv", athena_client, athena_bucket)

all_rids = set(all_rids_df.rid.tolist())
v1_rids = set(v1_rids_df.rid.tolist())
v2_rids = all_rids - v1_rids

with open("v1_most_recent.txt", "w") as f:
    f.write("\n".join(v1_rids))

v2_rids = set(all_rids) - set(v1_rids)
with open("v2_most_recent.txt", "w") as f:
    f.write("\n".join(v2_rids))
