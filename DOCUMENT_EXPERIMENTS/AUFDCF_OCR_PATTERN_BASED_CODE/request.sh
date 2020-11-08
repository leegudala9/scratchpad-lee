#!/bin/bash

RID="$1"
echo '> ' $RID


MY_URL="https://rememberv2.$URL/latest/query"
DATA="{\"Index\":{ \"ExtractionUI::DocumentId\": \"$RID\" } }"
echo $URL 'url'
echo $MY_URL
echo $DATA
echo $API_KEY

curl --location --request POST $MY_URL \
--header 'Content-Type: application/json' \
--header "Authorization: $API_KEY" \
--data-raw "$DATA" | jq '' > json_out/$RID.json

echo json_out/$RID.json