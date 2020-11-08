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
from feature_generation import generate_feature_set
from collections import defaultdict
from utilities import *
from copy import deepcopy


def find_min(edge, extrema, flat_ocr, word_ids):
    #print(edge, extrema, word_ids)
    #print(flat_ocr.get(word_ids[0], 'NADA'))
    good_words = [
        flat_ocr.get(wid, {}).get(edge)
        for wid in word_ids
        if flat_ocr.get(wid, {}).get(edge) is not None

    ]
    if len(good_words) == 0:
        return 0
    return extrema(good_words)

def get_convex_hull(word_ids, flat_ocr):
    #print('word_ids', word_ids)
    edges = {
        'x0': min,
        'y0': min,
        'x1': max,
        'y1': max
    }
    return {
        edge: find_min(edge, extrema, flat_ocr, word_ids)
        for edge,extrema in edges.items()
    }

def flatten_ocr(ocr):
    return dict(
        (word, ocr[page]['words'].get(word, {}).get('coordinates', {}))
        for page in ocr.keys()
        for word in ocr[page]['words'].keys())

def create_ocr_ent(word_id, ent, flat_ocr):
    new_ent = deepcopy(ent)
    new_ent['CONTENT']['CONTENT']['word_ids'] = [word_id]
    new_ent['CONTENT']['CONTENT']['coordinates'] = flat_ocr.get(word_id, {})
    new_ent['CONTENT']['CONTENT']['value'] = word_id
    new_ent['CONTENT']['CONTENT']['raw_text_unformatted'] = word_id
    return new_ent

def word_id_datapoints(ent, flat_ocr):
    word_ids = ent['CONTENT']['CONTENT']['word_ids']
    if len(word_ids) > 1:
        # multiple
        return [
            create_ocr_ent(word_id, ent, flat_ocr)
            for word_id
            in word_ids
        ]
    ent['CONTENT']['CONTENT']['value'] = '#'.join(
        ent['CONTENT']['CONTENT']['word_ids'])
    ent['CONTENT']['CONTENT']['raw_text_unformatted'] = ent['CONTENT']['CONTENT']['value']

    return [ent]

def single_word_id_datapoints(ent):
    word_ids = ent['CONTENT']['CONTENT']['word_ids']
    ent['CONTENT']['CONTENT']['value'] = '#'.join(word_ids)
    ent['CONTENT']['CONTENT']['raw_text_unformatted'] = ent['CONTENT']['CONTENT']['value']
    return ent


def get_meaningful_results(result):
    source = get_attr_list(['value', 'source'], result)
    if source is None:
        source = ''
    entity_id = get_attr_list([source, 'entity_id'], result)
    entity_type = result.get("CHILD ENTITY TYPE", result.get("ENTITY TYPE"))
    ocr_toks = result.get('selection_input', {}).get('ocr', [])
    if ocr_toks is None:
        ocr_toks = []
    return {
        'label': result['NAME'],
        'word_ids':[tok.get('id') for tok in ocr_toks if type(tok) == dict],
        'source': source,
        'entity_id': entity_id,
        'entity_type': entity_type,
        'document_index': get_attr_list(['selection_input', 'page', 'document_index'], result)
        }
def start_gathering(doct_type,rids_lits,df):
    print("\n")
    print("+++++++++++++++++++START+++++++++++++++++++++++++")

    # AWS RESOURCES
    split=False

    doctype = doct_type
    rids = rids_lits

    with open("./config/config.json", "r") as r:
        config = json.load(r)

    entity_map = config["entity_map"]
    entity_map[None] = 0
    doctype_map = config["doctype_map"]
    keywords = config["keywords_by_doctype"][doctype]

    final_output = {}
    #df = entities_predicted.iloc[:,:]

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

        event = {}
        event['ocr'] = ocr
        entities = defaultdict(list)
        event['doctype'] = doctype

        flat_ocr = flatten_ocr(ocr)
        all_entity_types = [x.upper() for x in config['entity_map'].keys() if x is not None]

        for entity_type in all_entity_types:
            if split:
                type_found_entities = [
                    fe
                    for fe in found_entities
                    if fe['ENTITY_TYPE'] == entity_type
                ]
                entities[entity_type] = [
                    ent
                    for fe in type_found_entities
                    for ent in word_id_datapoints(fe, flat_ocr)
                    if ent['CONTENT']['CONTENT']['coordinates'].get('x0') is not None
                ]
            else:
                entities[entity_type] = [
                    single_word_id_datapoints(fe)
                    for fe in found_entities
                    if fe['ENTITY_TYPE'] == entity_type
                ]
        event['entities'] = entities
        try:
            

            feature_set = generate_feature_set(event, keywords, entity_map, doctype_map)

            rid_df = df[df.rid == rid]

            entities = defaultdict(list)

            for entity_type in all_entity_types:
                et_df = rid_df[rid_df.pred_names == entity_type]
                for ind, row in et_df.iterrows():
                    print('ocr', row.ocr)
                    print('pred_names', row.pred_names)
                    print('entity_type')
                    word_ids = row.word_tokens
                    print("=========================")
                    print(type(row.word_tokens))
                    print(row.word_tokens)
                    print("=========================")
                    print(word_ids)
                    npi = word_ids
                    print('page', row.page, row.page_ind)
                    print(ocr.keys())
                    page = row.page_ind.split('_')[1]
                    ent = {
                        "ENTITY_ID": "--",
                        "PAGE": int(page),
                        "CONTENT": {
                            "CONTENT": {
                                "word_ids": [word_ids],
                                "coordinates": get_convex_hull([word_ids], flat_ocr),
                                #"coordinates": flat_ocr.get(npi),
                                "raw_text_unformatted": npi,
                                "value": npi
                            }
                        },
                        "CONFIDENCE": 0.5237530469894409,
                        "ENTITY_TYPE": entity_type
                    }

                    print(ent)
                    entities[entity_type].append(ent)

            event['entities'] = entities
            try:

                feature_set_snorkel = generate_feature_set(event, keywords, entity_map, doctype_map)


                operator_results = {res['NAME']:get_meaningful_results(res) for res in results}
                print(operator_results.values())
                final_output[rid] = {
                    'vectors': {'features': feature_set['features'] + feature_set_snorkel['features']},
                    'operator_results': list(operator_results.values())
                }
                print("\n\n\n ADDED TO ENTITIES")
            except:
                pass
                print(f"*********lost this rid: {rid}*****************")
           
        except:
            pass
            print(f"**************lost this rid because of feature set genration: {rid} ******************")
    doctype = doctype.replace('/', '')
    doctype = doctype.replace(' ', '').lower()
    with open(f"snorkel_pipeline_sets/{doctype}_pipeline_set.json", "w") as f:
        json.dump(final_output, f)
    print(f'Successfully dumped the file here: snorkel_pipeline_sets/{doctype}_pipeline_set.json')


dynamo = boto3.resource("dynamodb")
table = dynamo.Table("MetadataTable")
athena_client = boto3.client("athena")
s3resource = boto3.resource("s3")
bucket_name = os.getenv("BUCKET_NAME")

high_threshold_dist = 0.5

json_dir = "json_out"
ocr_dir = "ocr"
vectors_dir = "vectors"
fe_dir = "found_entities"
em_dir = "entity_match_csvs"
remaining_dir = "remaining_found_entities"
