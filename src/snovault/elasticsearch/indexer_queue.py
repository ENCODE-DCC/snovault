### Class to manage the items for indexing
# First round will use a FIFO SQS queue from AWS

import boto3
import json
import math

log = logging.getLogger(__name__)

def includeme(config):
    config.registry[QUEUE_MANAGER] = QueueManager(config.registry)


class QueueManager(object):
    def __init__(self, registry):
        self.env_name = registry.settings.get('env.name', 'fourfront-backup')
        self.queue_name = self.env_name + '-indexer-queue'
        self.queue_url = self.init_queue()
        self.queue_attrs = {
            'VisibilityTimeout': '3600',  # 1 hour, in seconds
            'MessageRetentionPeriod': '1,209,600',  # 14 days, in seconds
            'ReceiveMessageWaitTimeSeconds': '2'  # 2 seconds of long polling
        }


    def get_queue_url(self):
        """
        Simple function that returns url of associated queue name
        """
        client = boto3.client('sqs')
        try:
            response = client.get_queue_url(
                QueueName=self.queue_name
            )
        except:
            response = {}
        return response.get('QueueUrl')


    def init_queue(self):
        """
        Initialize the queue that is used by this manager.
        For now, this is an AWS SQS queue, with a DLQ associated.
        Define relevant settings in this function.

        Returns a queue url that is guaranteed to link to the right queue.
        """

        client = boto3.client('sqs')
        queue_url = self.get_queue_url()
        should_init = False
        if not queue_url:
            should_init = True
        else:  # see if current settings are up to date
            try:
                curr_attrs = client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=list(self.queue_attrs.keys())
                )
            except:
                should_init = True
            else:
                should_init = self.queue_attrs != curr_attrs  # init if attrs off
        if should_init:
            response = client.create_queue(
                QueueName=self.queue_name,
                Attributes=self.queue_attrs
            )
            queue_url = response['QueueUrl']
        return queue_url


    def clear_queue(self):
        """
        Clear out the queue completely. You can no longer retrieve these
        messages. Takes up to 60 seconds.
        """
        client = boto3.client('sqs')
        response = client.purge_queue(
            QueueUrl=self.queue_url
        )
        return response


    def send_messages(self, messages):
        """
        Send any number of 'messages' in a list.
        Can batch up to 10 messages. For now, one item per message.
        messages argument should be a list of Python messages.
        For now, messages contain uuid and timestamp only.
        MessageId is just uuid+timestamp.
        Returns Ids of failed messages, in form uuid-timestamp.
        """
        failed = []
        client = boto3.client('sqs')
        for n in int(math.ceil(len(messages) / 10)):  # 10 messages per batch
            batch = messages[n:n+10]
            entries = []
            for batch_msg in batch:
                entry = {
                    'Id': '-'.join([batch['uuid'], batch['timestamp']]),
                    'MessageBody': json.dumps(batch)
                }
                entries.append(entry)
            response = client.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries
            )
            failed.extend(response.get('Failed', []))
        return failed


    def recieve_messages(self):
        """
        Recieves up to 10 messages from the queue. Fewer (even 0) messages
        may be returned on any given run. Returns a list of messages with info
        """
        client = boto3.client('sqs')
        response = client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=int(self.queue_attrs['VisibilityTimeout'])
        )
        # messages in response include ReceiptHandle and Body, most importantly
        return response.get('Messages', [])


    def delete_messages(self, messages):
        """
        Called after a message has been successfully received and processed.
        Removes message from the queue.
        Input should be the messages directly from recieve messages. At the
        very least, needs a list of messages with 'Id' and 'ReceiptHandle'.
        Returns a list with any failed attempts.
        """
        failed = []
        client = boto3.client('sqs')
        for n in int(math.ceil(len(messages) / 10)):  # 10 messages per batch
            batch = messages[n:n+10]
            response = client.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=batch
            )
            failed.extend(response.get('Failed', []))
        return failed


    def replace_message(self, messages):
        """
        Called using received messages to place them back on the queue.
        Using a VisibilityTimeout of 0 means these messages are instantly
        available to consumers.
        Input should be the messages directly from recieve messages. At the
        very least, needs a list of messages with 'Id' and 'ReceiptHandle'.
        Returns a list with any failed attempts.
        """
        failed = []
        client = boto3.client('sqs')
        for n in int(math.ceil(len(messages) / 10)):  # 10 messages per batch
            batch = messages[n:n+10]
            for msg in batch:
                msg['VisibilityTimeout'] = 0
            response = client.change_message_visibility_batch(
                QueueUrl=self.queue_url,
                Entries=batch
            )
            failed.extend(response.get('Failed', []))
        return failed
