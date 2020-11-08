import pandas as pd
import numpy as np
import re
import json
from gensim.parsing.preprocessing import remove_stopwords
from collections import Counter
import zipcodes
import datefinder
import datetime

text_file = open("frequent_keywords.txt", "r")
key_words = text_file.read().split('\n')

def get_DU_VERSION(text):
    try:
        val = text.find("version")
        if val != -1:
            result = re.findall("\d+\.\d+", text[val:val+20])[0]
            return ["DU VERSION",result]
        else:
            return ["DU VERSION","NA"]
    except:
        return ["DU VERSION","NA"]

def get_borrower_name(text):
    try:
        
        name = ""
        val = text.find("primary borrower")
        if val != -1:
            string = text[val:val+50]
            print(string)
            for i in string.split(" "):
                if i not in key_words:
                    name +=i+" "
            return ["BORROWER FULL NAME",name]
        else:
            return ["BORROWER FULL NAME","NA"]
    except:
        return ["BORROWER FULL NAME","NA"]
        

def get_REFINANCE_CASH_OUT_DETERMINATION_TYPE(text):
    try:
        purpose = ""
        val = text.find("refi purpose")
        if val != -1:
            string = text[val:val+80]
            limit = string.find("property")
            string = string[:limit]
            #print(string.split(" "))
            for i in string.split(" "):
                if i not in key_words:
                    purpose +=i+" "
                    #print(purpose)
            return ["REFINANCE CASH OUT DETERMINATION TYPE",purpose]
        else:
            return ["REFINANCE CASH OUT DETERMINATION TYPE","NA"]
    except:
        return ["REFINANCE CASH OUT DETERMINATION TYPE","NA"]

def get_PROPERTY_ADDRESS_LINE_1(text):
    try:
        val = text.find("property address")
        if val != -1:
            string = text[val:val+80]
            limit = string.find("number of")
            string = string[:limit]
            string = string.replace("property address","")
            print(string)
            return ["PROPERTY ADDRESS LINE",string.strip()]
        else:
            return ["PROPERTY ADDRESS LINE","NA"]
    except:
        return ["PROPERTY ADDRESS LINE","NA"]

def get_PROPERTY_ADDRESS_ZIPCODE(text):
    try:
        
        val = text.find("property address")
        if val != -1:
            string = text[val:val+100]
            print(string)
            regex= re.compile(r"(\b\d{5}-\d{4}\b|\b\d{5}\b\s)")
            matches= re.findall(regex, string)
            if len(matches)!=0:
                return ["PROPERTY ADDRESS ZIPCODE",matches[0].replace(" ","")]
            else:
                return ["PROPERTY ADDRESS ZIPCODE","NA"]
        else:

            return ["PROPERTY ADDRESS ZIPCODE","NA"]
    except:
        return ["PROPERTY ADDRESS ZIPCODE","NA"]
    
def get_DESKTOP_UNDERWRITER_RECOMMENDATION_TYPE(text):
    try:
        
        val = text.find("recommendation")
        if val != -1:
            string = text[val+len('recommendation'):val+len('recommendation')+18]
            print(string)
            return ["DESKTOP UNDERWRITER RECOMMENDATION TYPE",string.strip()]
        else:
            return ["DESKTOP UNDERWRITER RECOMMENDATION TYPE","NA"]
    except:
        return ["DESKTOP UNDERWRITER RECOMMENDATION TYPE","NA"]
    
def get_CASE_FILE_ID(text):
    try:
        
        val = text.find("casefile id")
        if val != -1:
            string = text[val+len("casefile id"):val+len("casefile id")+30]
            limit = string.find("submission") 
            print(string)
            string = string[:limit]
            
            return ["CASE FILE ID",string.strip()]
        else:
            return ["CASE FILE ID","NA"]
    except:
        return ["CASE FILE ID","NA"]

def get_SUBMISSION_NUMBER(text):
    try:
        
        val = text.find("submission number")
        if val != -1:
            string = text[val+len("submission number"):val+len("submission number")+10]
            print(string)
            if float(string.split(" ")[1]):
                return ["SUBMISSION NUMBER",string.split(" ")[1]]
            else:
                return ["SUBMISSION NUMBER","NA"]
        else:
            return ["SUBMISSION NUMBER","NA"]
    except:
        return ["SUBMISSION NUMBER","NA"]
    
def get_SUBMISSION_DATETIME(text):
    try:
        
        ret_val = []
        val = text.find("submission date")
        if val !=-1:
            string = text[val+len("submission date"):val+len("submission date")+20]
            print(string)
            match = datefinder.find_dates(string)
            if match != None:
                for i in match:
                    ret_val.append(i)   
                if len(ret_val) > 0:
                    return ["SUBMISSION DATETIME",ret_val[0].strftime('%m/%d/%Y %I:%M %p')]

                else:
                    return ["SUBMISSION DATETIME","NA"]
            else:
                return ["SUBMISSION DATETIME","NA"]
        else:
            return ["SUBMISSION DATETIME","NA"]
    except:
        return ["SUBMISSION DATETIME","NA"]

def get_COBORROWER(text):
    try:
        val = text.find("co-borrower")
        if val != -1:
            string = text[val+len("co-borrower"):val+len("co-borrower")+80]
            print(string)
            limit = string.find("lender")
            string = string[:limit]
            return ["CO-BORROWER NAME",string.strip()]
        else:
            return ["CO-BORROWER NAME","NA"]
    except:
        return ["CO-BORROWER NAME","NA"]

def get_LTV_CLTV_HCLTV(text):
    try:
        val = text.find("ltv/")
        if val != -1:
            limit = text.find("note rate")
            if limit != -1:
                string = text[val:limit]
                print(string)
                groups = re.findall(r"(\d*.\d{2}%)/(\d*.\d{2}%)/(\d*.\d{2}%)",string)
            else:
                string = text[val:val+50]
                print(string)
                groups = re.findall(r"(\d*.\d{2}%)/(\d*.\d{2}%)/(\d*.\d{2}%)",string)

        else:
            return ["LTV CLTV HCLTV","NA"]
    
        LTV,CLTV,HCLTV = groups[0][0],groups[0][1],groups[0][1]
        return ["LTV CLTV HCLTV",LTV+CLTV+HCLTV]
    except:
        return ["LTV CLTV HCLTV","NA"]

def get_INTEREST_RATE(text):
    try:
        val = text.find("note rate")
        if val != -1:
            string = text[val:val+40]
            print(string)
            groups = re.findall(r"(\d*.\d*%)",string)
            return ["INTEREST RATE",groups[0]]
        else:
            return ["INTEREST RATE","NA"]
    except:
        return ["INTEREST RATE","NA"]

def get_HOUSING_EXPENSE_RATIO_PERCENT(text):
    try:
        
        val = text.find("housing expense ratio")
        if val != -1:
            string = text[val:val+60]
            print(string)
            groups = re.findall(r"(\d*.\d*%)",string)
            return ["HOUSING EXPENSE RATIO PERCENT",groups[0]]
        else:
            return ["HOUSING EXPENSE RATIO PERCENT","NA"]
    except:
        return ["HOUSING EXPENSE RATIO PERCENT","NA"]

def get_LOAN_PURPOSE_TYPE(text):
    try:
        val = text.find("loan type")
        if val != -1:
            string = text[val+len("loan type"):val+len("loan type")+40]
            print(string)
            limit = string.find("debt-")
            string = string[:limit]
            return ["LOAN PUROPSE TYPE",string.strip()]
        else:
            return ["LOAN PUROPSE TYPE","NA"]
    except:
        return ["LOAN PUROPSE TYPE","NA"]

def get_TOTAL_DEBT_EXPENSE_RATIO_PERCENT(text):
    try:
        
        val = text.find("debt-to-income ratio")
        if val != -1:
            string = text[val:val+50]
            print(string)
            groups = re.findall(r"(\d*.\d*%)",string)
            return ["TOTAL DEBT EXPENSE RATIO PERCENT",groups[0]]
        else:
            val = text.find("total expense ratio")
            if val != -1:
                string = text[val:val+80]
                print(string)
                groups = re.findall(r"(\d*.\d*%)",string)
                return ["TOTAL DEBT EXPENSE RATIO PERCENT",groups[0]]
            else:
                return ["TOTAL DEBT EXPENSE RATIO PERCENT","NA"]
        return ["TOTAL DEBT EXPENSE RATIO PERCENT","NA"]
    except:
        return ["TOTAL DEBT EXPENSE RATIO PERCENT","NA"]
            
def get_LOAN_AMORTIZATION_PERIOD_COUNT(text):
    try:
        
        val = text.find("loan term")
        if val != -1:
            string = text[val:val+50]
            limit = string.find("loan amount")
            string = string[:limit]
            print(string)
            groups = re.findall(r'(\d+)',string)
            return ["LOAN AMORTIZATION PERIOD COUNT",groups[0]]
        else:
            return ["LOAN AMORTIZATION PERIOD COUNT","NA"]
    except:
        return ["LOAN AMORTIZATION PERIOD COUNT","NA"]

def get_TOTAL_LOAN_AMOUNT(text):
    try:
        
        val = text.find("total loan amount")
        if val != -1:
            string = text[val+len("total loan amount"):val+len("total loan amount")+20]
            print(string)
            groups = re.findall(r'(\$\d+)',string)
            return ["TOTAL LOAN AMOUNT",groups[0]]
        else:
            return ["TOTAL LOAN AMOUNT","NA"]
    except:
        return ["TOTAL LOAN AMOUNT","NA"]
        
def get_SALES_PRICE(text):
    try:
        
        val = text.find("sales price")
        if val != -1:
            string = text[val+len("sales price"):val+len("sales price")+20]
            print(string)
            groups = re.findall(r'(\$\d+)',string)
            return ["SALER PRICE",groups[0]]
        else:
            return ["SALER PRICE","NA"]
    except:
        return ["SALER PRICE","NA"]

def get_LOAN_PURPOSE_TYPE(text):
    try:
        
        val = text.find("loan purpose")
        if val != -1:
            string = text[val+len("loan purpose"):val+len("loan purpose")+20]
            print(string)
            string = string.strip()
            return ["LOAN PURPOSE TYPE",string.split(" ")[0]]
        else:
            return ["LOAN PURPOSE TYPE","NA"]
    except:
        return ["LOAN PURPOSE TYPE","NA"]
    
def get_APPRAISAL_AMOUNT(text):
    try:
        val = text.find("actual/estimated appraised value")
        if val !=-1:
            string = text[val+len("actual/estimated appraised value"):val+len("actual/estimated appraised value")+20]
            print(string)
            groups = re.findall(r'(\$\d+)',string)
            return ["APPRISAL AMOUNT",groups[0]]
        else:
            val = text.find("appraised value")
            if val !=-1:
                string = text[val+len("actual/estimated appraised value"):val+len("actual/estimated appraised value")+20]
                print(string)
                groups = re.findall(r'(\$\d+)',string)
                return ["APPRISAL AMOUNT",groups[0]]
            else:
                return ["APPRISAL AMOUNT","NA"]
        return ["APPRISAL AMOUNT","NA"]
    except:
        return ["APPRISAL AMOUNT","NA"]
        
def get_PROPERTY_ADDRESS_STATE(text):
    try:
        just = get_PROPERTY_ADDRESS_ZIPCODE(text)
        if len(just)>0:

            city,state = process_zipcode(just[1])
            return ["PROPERTY ADDRESS STATE",state]
        else:
            return ["PROPERTY ADDRESS STATE","NA"]
        return ["PROPERTY ADDRESS STATE",state]
    except:
        return ["PROPERTY ADDRESS STATE","NA"]

def get_PROPERTY_ADDRESS_CITY(text):
    try:
        just = get_PROPERTY_ADDRESS_ZIPCODE(text)
        if len(just)>0:

            city,state = process_zipcode(just[0])
            return ["PROPERTY ADDRESS CITY",city]
        else:
            return ["PROPERTY ADDRESS CITY","NA"]
        return ["PROPERTY ADDRESS CITY","NA"]
    except:
        return ["PROPERTY ADDRESS CITY","NA"]

def get_frequent_word(text,how_many):
    split_it = text.split() 
    c = Counter(split_it)
    most_occur = c.most_common(how_many)
    return most_occur

def remove_scrap_char(text):
    characters_to_remove = "~!@#$%^&*()_+:'<>/|\;"
    for i in characters_to_remove:
        text.replace(i,"")
    return text

def process_zipcode(zipcode):
    info = zipcodes.matching(zipcode)
    city = info[0]["city"]
    state = info[0]["state"]
    return city,state

def get_expected(just_test,page,docid):
    just_test = just_test.lower()
    list_of_results = []
    functions = [get_DU_VERSION,
    get_REFINANCE_CASH_OUT_DETERMINATION_TYPE,
    get_PROPERTY_ADDRESS_ZIPCODE,
    get_PROPERTY_ADDRESS_LINE_1,
    get_DESKTOP_UNDERWRITER_RECOMMENDATION_TYPE,
    get_CASE_FILE_ID,
    get_SUBMISSION_NUMBER,
    get_SUBMISSION_DATETIME,
    get_COBORROWER,
    get_LTV_CLTV_HCLTV,
    get_INTEREST_RATE,
    get_HOUSING_EXPENSE_RATIO_PERCENT,
    get_LOAN_PURPOSE_TYPE,
    get_TOTAL_DEBT_EXPENSE_RATIO_PERCENT,
    get_LOAN_AMORTIZATION_PERIOD_COUNT,
    get_TOTAL_LOAN_AMOUNT,
    get_SALES_PRICE,
    get_LOAN_PURPOSE_TYPE,
    get_APPRAISAL_AMOUNT,
    get_PROPERTY_ADDRESS_STATE,
    get_PROPERTY_ADDRESS_CITY,
    get_BORROWER_LAST_NAME,
    get_BORROWER_FIRST_NAME,
    get_BORROWER_MIDDLE_NAME]
    
    for fun in functions:
        list_of_results.append(fun(just_test))
        
    final = pd.DataFrame(list_of_results,columns=["LABEL","VALUE"])
    final["RID"] = docid
    final["PAGE"] = page
    
    return final
