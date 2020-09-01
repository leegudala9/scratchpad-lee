import entity_formatter
from entity_tagger import entity_tagger as tagger
import requests
import json
import pandas as pd
import subprocess
import boto3
import traceback
import json
import dataset_creator
import os
from os import listdir
from os.path import isfile, join
from labeling_functions import *
import snorkel
from snorkel.labeling.model import LabelModel
from snorkel.labeling import PandasLFApplier
from dateutil.parser import parse
import re
from subprocess import Popen
import datetime
from snorkel.utils import probs_to_preds
import snorkel_pipeline

inv_et_dct = {
 0: 'NULL',
 1: 'COMPANY NAME',
 2: 'DATE',
 3: 'PERSON NAME',
 4: 'CURRENCY',
 5: 'NUMBER',
 6: 'PERCENT',
 7: 'ROUTE',
 8: 'STATE',
 9: 'CITY',
 10: 'ZIPCODE',
 11: 'YEAR',
 12: 'OTHER',
 13: 'TIME',
 14: 'SPECIAL_NUMBER',
 15: 'LIST',
 16: 'TEXT'

}

lfs = [lf_contains_date_parser,lf_contains_currency,lf_contains_zipcode,\
       lf_contains_state,lf_contains_quntity,lf_contains_phonenumber,lf_contains_SSN,\
       lf_contains_first_name,lf_contains_last_name,lf_contains_last_percent]

def get_rid_list():
    onlyfiles = [f for f in listdir("rids_to_process/") if isfile(join("rids_to_process/", f))]
    onlyfiles = ["rids_to_process/" + s for s in onlyfiles]
    return onlyfiles

def rid_iterator(df):
    final_return = pd.DataFrame()
    for i,j in df.iterrows():
        just_holder = dataset_creator.aggregate_formatted_entities(j.rid)
        final_return = final_return.append(pd.DataFrame(just_holder))
    return final_return

def get_snorkel_labels(frame_to_train):
    print("==============================Labeling is now started=======================================")
    applier = PandasLFApplier(lfs=lfs)
    L_train = applier.apply(df=frame_to_train)
    date_parser_coverage, currency_coverage,\
    zipcode_coverage,state_coverage,\
    quntity_coverage,phonenumber_coverage,SSN_coverage,\
    first_name_coverage,last_name_coverage,percent_coverge= (L_train != ABSTAIN).mean(axis=0)
    frame_to_train.rename(columns={"word_id":"word_tokens","text":"ocr","label_number":"preds"},inplace=True)
    print("==============================Labeling is now complete=======================================")
    print("==============================Summary Stats==================================================")
    print(f"date_parser_coverage: {date_parser_coverage * 100:.1f}%")
    print(f"currency_coverage: {currency_coverage * 100:.1f}%")
    print(f"zipcode_coverage: {zipcode_coverage * 100:.1f}%")
    print(f"state_coverage: {state_coverage * 100:.1f}%")
    print(f"quntity_coverage: {quntity_coverage * 100:.1f}%")
    print(f"phonenumber_coverage: {phonenumber_coverage * 100:.1f}%")
    print(f"SSN_coverage: {SSN_coverage * 100:.1f}%")
    print(f"first_name_coverage: {first_name_coverage * 100:.1f}%")
    print(f"last_name_coverage: {last_name_coverage * 100:.1f}%")
    #print(f"alpha_number_coverage: {alpha_number_coverage * 100:.1f}%")
    print(f"percent_coverage: {percent_coverge * 100:.1f}%")
    label_model = LabelModel(cardinality=15, verbose=True)
    label_model.fit(L_train=L_train, n_epochs=500, log_freq=100, seed=123)
    frame_to_train["label_number"] = label_model.predict(L=L_train, tie_break_policy="abstain")
    frame_to_train.label_number.fillna(0,inplace=True)
    frame_to_train['pred_names'] = frame_to_train.label_number.map(inv_et_dct)
    return frame_to_train

#dataset_df = pd.DataFrame()
paths = get_rid_list()
for i in paths:
    holder = pd.read_csv(i,names=["rid"])
    holder.drop_duplicates(inplace=True)
    holder.fillna("aa",axis=0,inplace=True)
    holder = holder.head(100)
    print(f"===============Dataset generation is in progress for {i}===============")
    iterator = rid_iterator(holder)
    #dataset_df = dataset_df.append(iterator, ignore_index=True)
    print(f"==============================Dataset generation is now complete for {i} ======================================")
    print(f"==============================Predicting Labels is now complete for {i} ======================================")
    trained_labels = get_snorkel_labels(iterator)
#    print("==============================Getting OCR files that are needed======================================")
#     proc = subprocess.Popen('./get_required_jsons.sh %s' %(i) , stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
    temp = i.split("/")[1]
    print(temp)
    snorkel_pipeline.start_gathering(str(temp.replace("_"," ")),trained_labels.rid.unique(),trained_labels)
    #dataset_df = pd.DataFrame()
    
    
    
   