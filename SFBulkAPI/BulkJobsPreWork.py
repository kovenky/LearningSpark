
import pandas as pd
import os
import json
import csv
from simple_salesforce import Salesforce  # imported salesforce
from config import *

# Login to Salesforce
print("---- logging into Salesforce ----")
sf = Salesforce(username=username, password=password, security_token=token, domain='test')
print("--- login success! ---")

def set_record_type(row):
    if row['SerialNo']:
        return int(row['SerialNo'])


# TODO : move this to a function later with a SOQL string as parameter
print("--- fetching chunk series details from SF ---")
chunkINFO = sf.bulk.HyperBatchOutput__c.query("SELECT Serial_Number__c,BatchState1__c FROM HyperBatchOutput__c WHERE Serial_Number__c !=null AND ObjectName__c ='Opportunity' Order By Serial_Number__c ASC")
chunkDF = pd.DataFrame(chunkINFO)
del chunkDF['attributes']  # to delete 'attributes' column which is not required
chunkDF.columns = ['SerialNo', 'SOQL']
# chunkDF['SRNO'] = chunkDF['SerialNo']

print("------ chunkDF length ===== ", len(chunkDF))
# Transform row data
changed_rows = chunkDF.apply(lambda row: set_record_type(row), axis=1)
chunkDF = chunkDF.assign(NewSRNO=changed_rows.values)

# write output into json file
chunkDF.to_json('./chunkDetails.json', orient='records')

print("--- chunk details saved to chunkDetails.json ---")

print("--- opening details file in VS-Code ---")
os.system("code ./chunkDetails.json")
print("*** All done!!! ***")