
import logging
import boto3
from botocore.exceptions import ClientError

# =================================
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        print("*** upload_file response : ", response)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# =================================


# Let's use Amazon S3
s3 = boto3.resource('s3')

test_bucket = ''

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)
    if('test' in bucket.name):
        test_bucket = bucket.name
        break

print("*** test_bucket after S3 list fetched: ", test_bucket)

# Upload a new image file =========================
data = open('../test.png', 'rb')
s3.Bucket(test_bucket).put_object(Key='test.jpg', Body=data)

# Upload JSON File to S3 =============
json_file = "/Users/vkollimarla/personal/LearningSpark/PySpark/DataFrames/people.json"

resp = upload_file(json_file,test_bucket,'people.json')
if resp:
    print("File upload success! ", json_file)
else:
    print("Error Occurred!")

# ============================

