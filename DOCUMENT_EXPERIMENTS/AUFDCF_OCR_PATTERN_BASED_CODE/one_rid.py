import boto3
import json
import requests
from datetime import datetime
from utilities import get_attr_list
import os
import sys
from get_resources import (
    get_athena_bucket,
    get_rid_df,
    get_found_results,
    get_last_rows,
    get_ocr,
    get_table_result,
    fetchall_athena,
)


url = sys.argv[1]
rids = sys.argv[2:]
api_key = os.getenv("API_KEY")


dynamo = boto3.resource("dynamodb")
table = dynamo.Table("MetadataTable")
print(table)

for rid in rids:
    print(rid)
    table_result = get_table_result(rid, table)

    s3_url = json.loads(table_result["_hocr_paths"])["data"]
    print(s3_url)
    payload = {
        "_remember_id": rid,
        "_paginated_hocr_metadata_path": s3_url,
        "_path": "tagging",
        "_read_wp_task_token": "AAAAKgAAAAIAAAAAAAAAAR6J3LYI50Qzc0ukkCD+d8CKvtNW3hYKR5bNUOk8j5HZOmgrRrjm57HRyb0H8nxhP/71LhZhedu9z4xlq8BRDRbDSZ6H8wOSxMMIBwwFsoLK10lV3uyOOTpZy0n/ur6Nn29R3XaHKN+gWFP+jpmMamyrdKeGd2GHxQI99u2eQ0dSb3cp1M621FnvF6nKYzAbhPHdZRjmkiGP0AdskrCczE+BCwrrXXXsMyE6RVPG2ad5KSIxeoh6pPWewDa1kHGHquii1kyQE5Jx7am3LriamSaPP7tTPs+o8J37QCjEsC1KrRCbK9lKOyPlzG6i++4qDa56RV+d2J8AltYSVbvUGPH/x2wxbKC8N66yzVRnscvFoIbNaF7R6I0LPryAlYGeJPzDAdLVOjmqoqWVweHgq7cNi0Qi3wfhf62MZRQocCwBOlaA22sWz0Csy5UP3uFmvjZG6eUQLecEfAbkpC4ZnojqPahc0thnXffbtTrqh7YKsnXp9n9M7OdKU0JoMAiEEkquTkhzB5PS8f+Za1f90iS9TqXtEFa3Yk/L+CkQUwLM8hYnk4LhOXe1MXdcRlgQYFG9dQypnmYcGlBRSabRc2zvMcPNbvPDTo6gTzh69NaLPpunMn5uT1Ln5WTl18kFn0vLTKqnKtfJwMWVGT1Y3lg=",
        "AccountOwner": "USBANK",
        "taskToken": "AAAAKgAAAAIAAAAAAAAAAUyIYVx/PV5iARF8jsQ8y7zWn7nc8crKpYzOVRUNzdNGJ9+5hF3PNKFhgbCt6Sa+d+8Q321XKjeRThytGozxe/RSjh8m5pgFnZnY6xuuYXAgK96QLK+WPeGnZOq/Ei6urYjJvwriDAkd9GsWLAKsDphqWrcO0n5O6LOGjfim87I3SA/3OSmAB9cMtF3MxNa+NQ9jID8UgRArHgcfKNthdNW1pGMh/FYR7B8LAWWPXYXe6H72H1kyHwXYadEMotP0ZMFx3oRDTYDUvdBUQaGXOVB0NSjMYUcE6zslRAIA6FZf54CgorDuYgUcvzgn2YMlomxymWH4zSwAx5QQbk3EIFHF1RUjSZUhrGzyb4pCnV4+qYKWrOKukNsJ7nN07RBi13aszbLMfqNxdSI5Yj6U1XgTklpPLjSJBWHNj859y7pNOlSB1OZ3v27E1Wtjq9jIvaXB9SdRqAHNK3b4AxUX/x0UDmS2ZKxADIfH3gKSmoX1qzR9ZTz3XU5Ua9ukcheDhtewXWp2HA3EfGjKukugBA2lAsiMuCprgActw9R+9IioiugU1FcNB9wBlY0jk9pWnwqvLz3Jm+w7F4bBIrrVxGMnhI9dzwktQVM00fZrgzapbPU8E94Pn5dwsgg0t8GxXnMfPYX9fV0Ahqhfz8qa0bY=",
    }

    res = requests.post(
        url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json", "x-api-key": api_key},
    )
    print(res)

