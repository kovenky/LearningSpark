"""
this is to access Salesforce Bulk API operations from Python
"""
import pandas as pd
import numpy as np
import datetime
import time
import os
import json
import csv
from pandas import DataFrame, Series
from simple_salesforce import Salesforce  # imported salesforce
import sys
from config import *

# total arguments
n = len(sys.argv)
print("Total arguments passed:", n)
print("All arguments ====== ", sys.argv)

for idx, arg in enumerate(sys.argv):
    print("arg[{0}] is {1}".format(idx, arg))

# Login to Salesforce
print("---- logging into Salesforce ----")
sf = Salesforce(username=username, password=password, security_token=token, domain='test')
print("--- login success! ---")

soqlData = json.load(open("./chunkDetails.json"))
print("--- first 2-series --- ")
print(soqlData[0:2], end="\n")
print("----- length of soqlData Series form Sheet: ----- ", len(soqlData))


def set_record_type(row):
    if row['RecordType']:
        if row['RecordType'].get('Name') == 'Purchase':
            return '0127A000000CwaZQAS'  #Id for Purchase RecordType
        else:
            return '0127A000000CwaaQAC'  #Id for Refinance RecordType


def fireJob(srno):
    finalSOQL = soqlData[int(srno)]['SOQL'] + ' Order By AccountId NULLS LAST '
    print("finalSOQL ----- ", finalSOQL)
    data = sf.bulk.Opportunity.query(finalSOQL)
    df = pd.DataFrame(data)
    del df['attributes']
    fileName = "./exported_files/Opportunities_for_srno_0{}.csv".format(soqlData[int(srno)]['NewSRNO'])
    print("filename ----- ", fileName, " TIME Now:---- ", datetime.datetime.now())
    # print(df.head(5))

    # Transform row data
    changed_rows = df.apply(lambda row: set_record_type(row), axis=1)
    df = df.assign(RecordTypeId=changed_rows.values)
    del df['RecordType']
    del df['LoanPurpose__c']
    df.columns = ['OpportunityId','Name','AccountId','AnticipatedClosingDate__c','OwnerId','AppraisalTurnTime__c','LoanNumber__c','LoanStatusDescription__c','DisbursementDate__c','SigningDate__c','RecordTypeId']
    df.to_csv(fileName, index=False)


def submitBulkQueryJob(beginSRNO, endSRNO):
    for srno in range(int(beginSRNO), int(endSRNO)):
        print(" ---- firing job for series: -----> ", srno)
        fireJob(srno)


# *******************
# submitBulkQueryJob(0, 2)
submitBulkQueryJob(sys.argv[1], sys.argv[2])
# ==========================================

