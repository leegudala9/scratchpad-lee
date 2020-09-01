search_dir=$1

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

for entry in $search_dir/*
do
    echo "==========================Downloading for $entry=================================="
    #Get operator results and output to json_out directory
    cat $entry | while read rid; do ./request.sh $rid; done

    # save OCR and Found entities to ocr and found_entities directory respectively
    cat $entry | xargs -L 10 -P 10 python3 ./save_resources.py "2"

  
done

python3 ./process_organizer.py

echo "Required json files are now in place"

