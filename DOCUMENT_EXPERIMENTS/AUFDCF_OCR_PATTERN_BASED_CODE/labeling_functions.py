from snorkel.labeling import labeling_function
import pandas as pd
import snorkel
import numpy as np
import zipcodes
import usaddress
import us
import datefinder
import dateutil
import phonenumbers
from dateutil.parser import parse
import re
import datetime
from names_dataset import NameDataset
m = m = NameDataset()





#inv_et_dct = {v:k for k,v in et_dct.items()}
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

ABSTAIN = -1
NULL = 0
COMPANY_NAME = 1
DATE = 2
PERSON_NAME = 3
CURRENCY = 4
NUMBER = 5
PERCENT = 6
ROUTE = 7
STATE = 8
CITY = 9
ZIPCODE = 10
YEAR = 11
OTHER = 12
TIME = 13
SPECIAL_NUMBER = 14
LIST = 15
TEXT = 16


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

def num_there(s):
    return any(i.isdigit() for i in s)


@labeling_function()
def lf_contains_date_parser(x):
    temp = str.strip(str(x.text))
    try:
        datetime.datetime.strptime(temp, '%m/%d/%Y')
        dateutil.parser.parse(temp).strftime("%m/%d/%Y")
        return DATE
    except:
        return ABSTAIN
    

@labeling_function()
def lf_contains_currency(x):
    try:
        if x.text == "$":
            return ABSTAIN
        if bool(re.search(r'\d', x.text)):
            match = re.findall(r'(?:[\£\$\€]{1}[,\d]+.?\d*)',str(x.text))
            context = x.text_blob[x.string_index-5:x.string_index]
            if len(match)>0:
                return CURRENCY
            elif context.find("$")>0:
                return CURRENCY
            else:
                return ABSTAIN
        else:
            return ABSTAIN
        
    except:
        return ABSTAIN
        
   
    
@labeling_function()    
def lf_contains_zipcode(x):
    try:
        match = zipcodes.matching(str(x.text))
        if len(match)>0:
            return ZIPCODE
        else:
            return ABSTAIN
    except:
        return ABSTAIN

@labeling_function()
def lf_contains_state(x):
    temp = str(x.text)
    if (len(temp) == 2) & (temp.isupper()):
        state = us.states.lookup(temp)
        if state!=None:
            return STATE
        else:
            return ABSTAIN
    else:
        return ABSTAIN

# @labeling_function()
# def lf_contains_middle_name(x):
#     temp = str(x.text)
#     pre_context = str(x.pre_context)
#     post_context = str(x.post_context)
#     try:
        
#         if len(temp) ==1 & temp.isupper():
#             if m.search_last_name(pre_context[-1]) | (m.search_last_name(post_context[0])):
#                 return MIDDLE_NAME
#             elif m.search_first_name(pre_context[-1]) | (m.search_first_name(post_context[0])):
#                 return MIDDLE_NAME
#             else:
#                 return ABSTAIN
#         else:
#             return ABSTAIN
#     except:
#         ABSTAIN
        
    
# @labeling_function()
# def lf_contains_state(x):
#     temp = str(x.ocr)
#     list_ = ["Rd","Dr","state","STATE","RD","ZIP","zip","address","ADRESS","street","STREET"]
#     if any(word in temp.split(" ") for word in list_):
#         state = us.states.lookup(str(x.ocr))
#         if state!=None:
#             return STATE
#         else:
#             return ABSTAIN
#     else:
#         return ABSTAIN

@labeling_function()
def lf_contains_quntity(x):
    try:
        x = str(x.text)
        x = str.strip(x)
        float(x.replace(",",""))
        return NUMBER
    except:
        return ABSTAIN

@labeling_function()
def lf_contains_phonenumber(x):
    res = ''.join(filter(lambda i: i.isdigit(), str(x.text)))
    if len(res)!=0:
        temp=[]
        lets = str(x.text)
        count_of_dash = str.count("-",lets)
        pre_context = x.text_blob[x.string_index-15:x.string_index]
        post_context = x.text_blob[x.string_index:x.string_index+15]
        for match in phonenumbers.PhoneNumberMatcher(lets, "US"):
            temp.append(phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164))
        if len(temp)>0:
            return SPECIAL_NUMBER
        elif re.search(r"\w{3}-\w{3}-\w{4}",lets):
            return SPECIAL_NUMBER
        elif re.search(r"(\w{3})\w{3}-\w{4}",lets):
            return SPECIAL_NUMBER
        elif re.search("\(\w{3}\)\w{3}-\w{4}",lets):
            return SPECIAL_NUMBER
        elif count_of_dash!=0:
            just_test = str(pre_context+lets+post_context)
            for later_search in phonenumbers.PhoneNumberMatcher(just_test, "US"):
                temp.append(later_search)
            if len(temp)>0:
                return SPECIAL_NUMBER
            else:
                return ABSTAIN
        else:
            return ABSTAIN
    else:
        return ABSTAIN

@labeling_function()
def lf_contains_SSN(x):
    res = ''.join(filter(lambda i: i.isdigit(), str(x.text)))
    if len(res)!=0:
        temp = str(x.text)
        temp = temp.replace("|","")
        temp = temp.replace("[","")
        temp = temp.replace("]","")
        if (temp.find("***-**-") !=-1):
            return SPECIAL_NUMBER
        elif (temp.find("**-**-") !=-1):
            return SPECIAL_NUMBER
        elif (temp.find("-**-") !=-1):
            return SPECIAL_NUMBER
        else:
            match = bool(re.match(r'(\d{3}-\d{2}|\*{3}-\*{2}|\#{3}-\#{2}|x{3}-x{2}|X{3}-X{2}|Xx{2}-x{2})-\d{4}',str(x.text)))
            if match:
                return SPECIAL_NUMBER
            else:
                return ABSTAIN
    else:
        return ABSTAIN


@labeling_function()
def lf_contains_first_name(x):
    temp = str(x.text)
    temp = temp.replace(",","")
    if len(temp)>0:
        if temp[0].isupper() & m.search_first_name(temp):
            return PERSON_NAME
        else:
            return ABSTAIN
    else:
        return ABSTAIN

# @labeling_function()
# def lf_contains_alpha_numberic(x):
#     temp = str(x.text)
#     if (temp.isalnum()) & (lf_contains_quntity(x) < 0) :
#         return ALPHA_NUM
#     else:
#         print("")
#         return ABSTAIN

@labeling_function()
def lf_contains_last_name(x):
    temp = str(x.text)
    temp = temp.replace(",","")
    if len(temp)>0:
        if temp[0].isupper() & m.search_last_name(temp):
            return PERSON_NAME
        else:
            return ABSTAIN
    else:
        return ABSTAIN
@labeling_function()
def lf_contains_last_percent(x):
    try:
        pre_context = str(x.text_blob[int(x.string_index)-3])
        if ('%' in str(x.text)) | ("%" in pre_context):
                return PERCENT
        return ABSTAIN
    except:
        if ('%' in str(x.text)):
            return PERCENT
        return ABSTAIN