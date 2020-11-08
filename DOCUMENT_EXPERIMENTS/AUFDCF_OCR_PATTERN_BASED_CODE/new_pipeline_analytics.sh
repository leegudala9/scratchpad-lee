RIDS="$1"
DOCTYPE="$2"


#DOCTYPE="BANK_STATEMENT"
CSV="pipeline_$RIDS.csv"
OUTPUT="full_report_US6689_$RIDS.csv"
PIPELINE_RESULT_DIR="pipeline_results_$DOCTYPE"

chmod +x request.sh
# BUCKET_NAME="hw-app-caliber-prod"

BUCKET_NAME=$(aws ssm get-parameter --name "/account/app-bucket" | jq '.Parameter.Value' | sed 's/\"//g')
URL=$(aws ssm get-parameter --name "/account/root-url" | jq '.Parameter.Value' | sed 's/\"//g')
API_KEY=$(aws ssm get-parameter --name "/account/internal-api-key" | jq '.Parameter.Value' | sed 's/\"//g')
echo $URL
export URL=$URL
url="https://tagentities.$URL/"
echo $url
export BUCKET_NAME=$BUCKET_NAME
export API_KEY=$API_KEY
export PIPELINE_RESULT_DIR=$PIPELINE_RESULT_DIR
echo $PIPELINE_RESULT_DIR
#mkdir "$PIPELINE_RESULT_DIR"
# # only run if want to trigger Tagged Entities again
# echo "Running one rid"
# cat $RIDS | while read rid; do echo $rid; python3 one_rid.py $url $rid; sleep 1; done
# echo "done running one_rid"

#Get operator results and output to json_out directory
cat $RIDS | while read rid; do ./request.sh $rid; done

# save OCR and Found entities to ocr and found_entities directory respectively
cat $RIDS | xargs -L 10 -P 10 python3 ./save_resources.py "2"


#cat $RIDS | xargs -L20 -P20 python3 ./pipeline_analytics_doctype.py "$DOCTYPE"

#cat $PIPELINE_RESULT_DIR/* | grep expected_entity_type | uniq > $CSV
#cat $PIPELINE_RESULT_DIR/* | grep -v expected_entity_type | uniq >> $CSV

#python3 pipeline_reporter.py $CSV $OUTPUT
