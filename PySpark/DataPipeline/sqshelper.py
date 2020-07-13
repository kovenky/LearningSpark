"""
This is to help with create Queues,
sending messages to queues,
read messages,
delete queues,
list queues
"""
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
sqs = boto3.resource('sqs')


class SQSHelper:
    # sqs = None

    def __init__(self):
        """ Constructor for SQSHelper Class """
        print("object instantiated")
        # sqs = boto3.client('sqs')


    def create_queue(self, q_name):
        """Creates SQS Queue with a name passed to it and returns the queue URL,
            if same queue exists, it would return the queue URL"""

        print("*** from create_queue() ***", q_name)
        sqs = boto3.client('sqs')
        response = sqs.create_queue(
            QueueName=q_name,
            Attributes={
                'DelaySeconds': '60',
                'MessageRetentionPeriod': '86400'
            }
        )

        if response is not None:
            return response['QueueUrl']

        return response

    def send_message(self, queue_url, message_str):
        """takes the queue URL and message to be sent, and send message to SQS Queue"""
        print("*** from send_message: ", message_str)

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
            MessageBody=message_str
        )

        return response



    def send_messages(self,q_name,messageList):
        """send multiple messages as batches of each 10 messages"""
        queue = sqs.get_queue_by_name(QueueName=q_name)
        maxBatchSize = 10  # current maximum allowed

        # let's chunks input list each chunk of 10 messages
        chunks = [messageList[x:x + maxBatchSize] for x in range(0, len(messageList), maxBatchSize)]

        for chunk in chunks:
            entries = []
            for x in chunk:
                entry = {'Id': str(len(entries)),
                         'MessageBody': str(x),
                         'MessageGroupId': q_name+'-GRP'
                         }
                entries.append(entry)
            response = queue.send_messages(Entries=entries)
            print("*** response *** ",response)

        return response

