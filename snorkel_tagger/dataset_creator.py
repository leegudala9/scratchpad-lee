import entity_formatter
from entity_tagger import entity_tagger as tagger
import requests
import json
import pandas as pd
import boto3
import traceback
import snorkel
import json

full_list = []

ssm = boto3.client("ssm")
s3 = boto3.client("s3")
root_url = ssm.get_parameter(Name=f"/account/root-url")["Parameter"]["Value"]
apikey = ssm.get_parameter(Name="/account/internal-api-key")["Parameter"]["Value"]
v1_url = f"https://remember.{root_url}"
v2_url = f"https://rememberv2.{root_url}/latest"
acc_owner = ssm.get_parameter(Name="/account/owner")["Parameter"]["Value"].upper()
headers = {"x-api-key": apikey, "Authorization": apikey}

def rememberv2_query(index={}, filters={}):
    url = f"{v2_url}/query"
    results = {}
    try:
        payload = {
            "Index": index,
            "Filter": filters
        }
        results = json.loads(requests.post(url=url, data=json.dumps(payload), headers=headers).text)["Results"]
    except:
        print(traceback.format_exc())    
    return results


def rememberv2_read(objectid):
    url = f"{v2_url}/read"
    results = {}
    try:
        payload = {
            "ObjectId": objectid,
        }
        results = json.loads(requests.post(url=url, data=json.dumps(payload), headers=headers).text)["Results"]
    except:
        print(traceback.format_exc())
    return results



def remember_recall(rid, datapoint):
    url = f"{v1_url}/recall?_remember_id={rid}&_datapoint={datapoint}"
    res = {}
    try:
        res = json.loads(requests.get(url=url).text)["datapoints"][0]["data"]
    except:
        print(traceback.format_exc())
    return res
    
# def make_text_blob(word_ocr):
#     text_list = []
    
#     for i in word_ocr["Words"]:
#         text_list.append(i["text"])
#     #print("\n\n\nBefore Sending it off: " , text_list)
#     return text_list

def remember_write(datapoint):
    resp_dict = {}
    url = f"{v2_url}/write"
    try:
        resp = requests.post(
            url=url, data=json.dumps(datapoint), headers=headers
        )
        resp_dict = resp.json()
    except:
        print(traceback.format_exc())
    return resp_dict


def create_datapoint(Type, Fields, TransactionId, Attributes=None):
    datapoint = {
        "Type": Type,
        "Fields": Fields,
        "TransactionId": TransactionId,
    }
    if Attributes != None:
        datapoint["Attributes"] = Attributes
    return remember_write(datapoint)


def remember_memorize(data, rid, datapoint, metadata={}):
    url = f"{v1_url}/memoorize"
    try:
        metadata.update({
            "_remember_id": rid,
            "_datapoint": datapoint
        })
        payload = {
            "data": data,
            "metadata": metadata 
        }
        resp = requests.post(
                url=url, data=json.dumps(payload), headers=headers)
    except:
        print(traceback.format_exc())
    return resp
def do_sner_tag(text):
    text = text.replace("/","-")
    text = text.replace("[]","")
    tagged_list = sner_tagger.tag(word_tokenize(text))
    return tagged_list
def do_spacy_tag(text):
    text = text.replace("/","-")
    

def aggregate_formatted_entities(docid):
    temp_dict = {}
    full_list =[]
    try:
        recall_txn = rememberv2_read(docid)[0]
        txnid = recall_txn["TransactionId"]
        file_pages = recall_txn["Pages"]
        start = file_pages[0]
        doc_pages = list(range(1, len(file_pages)+1))
        page_ocrs_ids = {x['ParentIndex']:x['ObjectId'] for x in rememberv2_query({'PageOcr::TransactionId': txnid}, {'ParentIndex': file_pages})}
        results = {}
        formatted_doc = {}
        for page in sorted(page_ocrs_ids.keys()):
            try:
                words_ocr = rememberv2_query({'Parent': page_ocrs_ids[page]})
                parsed_words = tagger.parse_words(words_ocr[0]['Words'])
                blob = tagger.make_blob(parsed_words)
                for i in parsed_words:
                    index_word = int(i["string_index"])
                    if len(blob[index_word-40:index_word+40])==0:
                        
                        i["text_blob"] = blob
                    else:
                        i["text_blob"] = blob[index_word-40:index_word+40]
                    i["page_ind"] = f'page_{page+1}'
                    i["rid"] = docid
                    i["page"] = page+1
                    full_list.append(i)

            except:
                
                print("============================OBSERVATIONS OF SOME FALILURES(Page)=======================================")
                print(traceback.format_exc())
                print("RID ====> ",docid)
                print("page ===> ",page)
                
                pass
        
        return full_list
    except:
        print("==========================OBSERVATIONS OF SOME FALILURES(failed for rid)=======================================")
        print(traceback.format_exc())
        print("RID ====> ",docid)
        pass
def process_tagged_with_text(page):
    # extract all named entities
    tagged_entities = []
    index_count = 0
    entity_id = ''
    
    for term, tag in sentence:
        if tag != 'O':
            word = term
            word_tag = tag
            entity_id = uuid.uuid4()
            make_entity = {'entity_id': entity_id.hex, 'text': word, 'entity_score': 0.9902280569076538 , 'entity_type': word_tag,'string_index': index_count }                    
            index_count = len(term)+index_count+1
            tagged_entities.append(make_entity)
        else:
            index_count = len(term)+index_count+1
        
    return tagged_entities

def find_untagged_words(untagged,tagged):
    temp_tagged.append(tagged)
    temp_untagged.append(untagged)

def get_bucket_key(path):
    bucket = path.split('/')[2]
    key = path.replace(f'S3://{bucket}/', '')
    return bucket, key


def get_object(path, s3):
    bucket, key = get_bucket_key(path)
    res = s3.get_object(
        Bucket=bucket,
        Key=key
    )['Body'].read().decode('utf-8')
    return res


def put_object(path, s3, data):
    bucket, key = get_bucket_key(path)
    s3.put_object(
        Bucket=bucket,
        Body=json.dumps(data),
        Key=key
    )
def get_tagged_words(tagged):
    list_of_tagged_word_ids = []
    for page in tagged:
        rip_a_page = json.loads(page["body"])
        for entity in rip_a_page["entities"]:
            list_of_tagged_word_ids.append(entity["word_id"])
    return list_of_tagged_word_ids    
    
def get_untagged_words(untagged,list_of_tagged_word_ids):
    list_of_untagged_word_ids = []
    list_of_untagged_entities = []
    for page in untagged:
        for entity in page:
            list_of_untagged_word_ids.append(entity["word_id"])
    l3 = [x for x in list_of_untagged_word_ids if x not in list_of_tagged_word_ids]
    for word in l3:
        for page in untagged:
            for entity in page:
                if entity["word_id"] == word:
                    list_of_untagged_entities.append(entity)
    return list_of_untagged_entities,l3
    
def memorize_results_update_inplace(docid):
    formatted_doc = aggregate_formatted_entities(docid)
    current_path = remember_recall(docid, '_aggregated_formatted_entities_path')
    new_path = current_path.replace("FormattedEntities", f"FormattedEntities{exp_id}")
    put_object(new_path, s3, formatted_doc)
    return new_path
