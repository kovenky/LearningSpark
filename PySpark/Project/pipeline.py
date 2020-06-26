"""
This is to create sample ETL pipeline process using local CSV files as input and
target output will be in AWS S3, SQS or MySQL DB Table, SQLite
"""

from PySpark.Project.s3helper import S3Helper
from PySpark.Project.sqshelper import SQSHelper
from datetime import date


# 1. Create Queue, and Send messages to queue
# 2. Upload File to S3
# 3. Insert records into MySQL/SQLite : /resources/mysql-connector-java-8.0.20/mysql-connector-java-8.0.20.jar
# 4. Load file into HDFS Location (for now, local HDFS Directory)


s3h = S3Helper()
sqs = SQSHelper()

# generate queue-name using current date
q_name = str(date.today())+'-Queue'

#Create an SQS Queue
q_resp = sqs.create_queue(q_name)

#TODO: Find a way to collect up to 10 msgs and send it to Queue (Max we can put only 10 msgs at a time)
#TODO: Add code here to process input data and send messages to SQS Queue in chunks with each chunk of 10 msgs


# Upload file to S3
upload_resp = s3h.upload_file('file-path-or-file',testbkt,"file-name")


#TODO: Add code here to insert records into SQLite or MySQL DB Table


#TODO: Add code here to add file into local HDFS directory

