"""
this program will create an SQS Queue with current-date as name if it doesn't
exists. and send a sample message to the queue.
"""
import boto3
from datetime import date

# Create SQS client
sqs = boto3.client('sqs')

qname = str(date.today())+'-queue'
print(qname)

# Create a SQS queue
response = sqs.create_queue(
    QueueName=qname,
    Attributes={
        'DelaySeconds': '60',
        'MessageRetentionPeriod': '86400'
    }
)

print(response['QueueUrl'])


queue_url = response['QueueUrl']

# TODO : Need to move this into a separate function
# Send message to SQS queue
response = sqs.send_message(
    QueueUrl=queue_url,
    DelaySeconds=10,
    MessageAttributes={
        'Title': {
            'DataType': 'String',
            'StringValue': 'from bubba'
        },
        'Author': {
            'DataType': 'String',
            'StringValue': 'bubba koven'
        },
        'test': {
            'DataType': 'Number',
            'StringValue': '59'
        }
    },
    MessageBody=(
        'sample msg from bubba koven : ' + qname
    )
)

print(response['MessageId'])

