"""
This is to help with create Queues,
sending messages to queues,
read messages,
delete queues,
list queues
"""
import boto3


class SQSHelper:
    sqs = None

    def __init__(self):
        """
            Constructor for SQSHelper Class
        """
        print("object instantiated")
        self.sqs = boto3.client('sqs')


    def create_queue(self, q_name):
        """Creates SQS Queue with a name passed to it and returns the queue URL,
            if same queue exists, it would return the queue URL"""

        print("*** from create_queue() ***", q_name)
        # sqs = boto3.client('sqs')
        response = self.sqs.create_queue(
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

        response = self.sqs.send_message(
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
