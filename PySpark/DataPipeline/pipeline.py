"""
This is to create sample ETL pipeline process using local CSV files as input and
target output will be in AWS S3, SQS or MySQL DB Table, SQLite
"""

from PySpark.DataPipeline.s3helper import S3Helper
from PySpark.DataPipeline.sqshelper import SQSHelper
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import ( StructField,
                                StringType, IntegerType, StructType
                                )
import os


file = open("./pipeline_status.log", "w")

# 1. Create Queue, and Send messages to queue
# 2. Upload File to S3
# 3. Insert records into MySQL/SQLite : /resources/mysql-connector-java-8.0.20/mysql-connector-java-8.0.20.jar
# 4. Load file into HDFS Location (for now, local HDFS Directory)

def push_to_sqs(jsonRows):
    """send data to sqs queue"""
    sqs = SQSHelper()
    # generate queue-name using current date
    q_name = str(date.today()) + '-Queue'
    file.write("[INFO] Creating Queue with name "+ q_name +' '+ str(datetime.now()))

    # Create an SQS Queue
    q_resp = sqs.create_queue(q_name)

    file.write("[INFO] SQS Queue Created! "+str(datetime.now()))
    resp = sqs.send_messages(q_name,jsonRows)
    print("*** sqs resp: ",resp)
    file.write("[INFO] Messages sent to Queue " + str(datetime.now()))


def push_to_s3(fileToLoad,bucketName,objName):
    """upload file to s3 bucket"""
    s3h = S3Helper()
    file.write("[INFO] Uploading file to S3 Bucket - " + str(datetime.now()))
    # Upload file to S3
    upload_resp = s3h.upload_file(fileToLoad, bucketName,objName)
    print("*** S3 upload_resp : ", upload_resp)
    file.write("[INFO] File upload completed to S3 - " + str(datetime.now()))


def push_to_mysql():
    """load data into mysql DB Table"""


def push_to_hdfs():
    """move file to HDFS directory"""


def main():
    print("*** from main() ***")
    file.write("*** BEGINNING PIPELINE ***")

    spark = SparkSession.builder.appName("PipelineVK").getOrCreate()

    # read json file now into a Data Frame
    data_file = '../resources/supermarket_sales.csv'
    df = spark.read.csv(data_file, header=True, sep=",").cache()

    print("People Count: ", df.count())

    gender = df.groupBy('Gender').count()
    print(gender.show())

    df.registerTempTable("sales")
    output = spark.sql('SELECT * from sales')
    output.show()

    output = spark.sql('SELECT COUNT(*) as total, City from sales GROUP BY City')
    output.show()

    # output.write.format('json').save('./filtered.json')
    output.coalesce(1).write.format('json').save(str(date.today())+'/json')

    # jsonFileData = spark.read.json(str(date.today())+"/json/*")

    push_to_sqs(str(date.today())+"/json/*.json")
    push_to_s3(str(date.today())+"/json/*.json","bubba-bkt-001","sales-data")

    file.close()


if __name__ == '__main__':
    main()
