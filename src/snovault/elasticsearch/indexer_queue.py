### Class to manage the items for indexing
# First round will use a FIFO SQS queue from AWS

import boto3
import json
import math
import logging
import datetime
from pyramid.view import view_config
from .interfaces import INDEXER_QUEUE
from .indexer_utils import find_uuids_for_indexing, get_uuids_for_types

log = logging.getLogger(__name__)

def includeme(config):
    config.add_route('queue_indexing', '/queue_indexing')
    config.registry[INDEXER_QUEUE] = QueueManager(config.registry)
    config.scan(__name__)


@view_config(route_name='queue_indexing', request_method='POST', permission="index")
def queue_indexing(request):
    """
    Endpoint to queue items for indexing. Takes a POST request with index
    priviliges which should contain either a list of uuids under "uuids" key
    or a list of collections under "collections" key of its body.
    """
    req_uuids = request.json.get('uuids', None)
    req_collections = request.json.get('collections', None)
    queue_mode = None  # either queueing 'uuids' or 'collection'
    response = {
        'notification': 'Failure',
        'requested_uuids': [],
        'number_queued': 0,
        'detail': 'Nothing was queued. Make sure to past in a list of uuids in in "uuids" key OR list of collections in the "collections" key of request the POST request.'
    }
    if not req_uuids and not req_collections:
        return response
    if req_uuids and req_collections:
        response['detail'] = 'Nothing was queued. You cannot provide both uuids and a collection for queueing at once.'
    if req_uuids and not isinstance(req_uuids, list):
        response['detail'] = 'Nothing was queued. When queueing uuids, make to sure to put a list of string uuids in the POST request.'
        return response
    if req_collections and not ininstance(req_collections, list):
        response['detail'] = 'Nothing was queued. When queueing a collection, make sure to provide a list of string collection names in the POST request.'
        return response
    queue_indexer = request.registry[INDEXER_QUEUE]
    if req_uuids:
        # strict mode means uuids should be indexed without finding associates
        if request.json.get('strict') == True:
            queued, failed = queue_indexer.add_uuids(request.registry, req_uuids, strict=True)
        else:
            queued, failed = queue_indexer.add_uuids(request.registry, req_uuids)
    else:
        queued, failed = queue_indexer.add_collections(request.registry, req_collections)
    response['notification'] = 'Success'
    response['number_queued'] = len(queued)
    response['requested_uuids'] = req_uuids
    response['detail'] = 'Successfuly queued items!'
    response['errors'] = failed
    return response


class QueueManager(object):
    def __init__(self, registry):
        self.env_name = registry.settings.get('env.name', 'fourfront-backup')
        self.queue_attrs = {
            'VisibilityTimeout': '3600',  # 1 hour, in seconds
            'MessageRetentionPeriod': '1209600',  # 14 days, in seconds
            'ReceiveMessageWaitTimeSeconds': '2'  # 2 seconds of long polling
        }
        self.queue_name = self.env_name + '-indexer-queue'
        self.queue_url = self.init_queue()


    def add_uuids(self, registry, uuids, strict=False):
        """
        Takes a list of string uuids, finds all associated uuids using the
        indexer_utils, and then queues them all up. Also requires a registry,
        which is passed in automatically when using the /queue_indexing route.

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.
        """
        if not strict:
            uuids = set(uuids)
            uuids_to_index, _, _ = find_uuids_for_indexing(registry, uuids, uuids, log)
            uuids_to_index = list(uuids_to_index)
        else:
            uuids_to_index = uuids
        timestamp = datetime.datetime.now().isoformat()
        failed = self.send_messages(uuids_to_index, timestamp)
        return uuids_to_index, failed


    def add_collections(self, registry, collections):
        """
        Takes a list of collection name, finds all associated uuids using the
        indexer_utils, and then queues them all up. Also requires a registry,
        which is passed in automatically when using the /queue_indexing route.

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.
        """
        uuids_to_index = list(get_uuids_for_types(registry, collections))
        timestamp = datetime.datetime.now().isoformat()
        failed = self.send_messages(uuids_to_index, timestamp)
        return uuids_to_index, failed


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
                should_init = self.queue_attrs != curr_attrs['Attributes']  # init if attrs off
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


    def send_messages(self, messages, timestamp):
        """
        Send any number of 'messages' in a list.
        Can batch up to 10 messages. For now, one item per message.
        messages argument should be a list of Python messages.
        For now, messages are a list of uuids.
        MessageId is just uuid+timestamp.
        Returns Ids of failed messages, in form uuid-timestamp.
        """
        failed = []
        client = boto3.client('sqs')
        for n in range(int(math.ceil(len(messages) / 10))):  # 10 messages per batch
            batch = messages[n:n+10]
            entries = []
            for batch_msg in batch:
                entry = {
                    'Id': '_'.join([batch_msg, timestamp.replace(':', '-').replace('.', '-')]),
                    'MessageBody': json.dumps({'uuid':batch_msg, 'timestamp':timestamp})
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
        for n in range(int(math.ceil(len(messages) / 10))):  # 10 messages per batch
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
        for n in range(int(math.ceil(len(messages) / 10))):  # 10 messages per batch
            batch = messages[n:n+10]
            for msg in batch:
                msg['VisibilityTimeout'] = 0
            response = client.change_message_visibility_batch(
                QueueUrl=self.queue_url,
                Entries=batch
            )
            failed.extend(response.get('Failed', []))
        return failed


    def number_of_messages(self):
        """
        Returns a dict with number of waiting messages in the queue and
        number of inflight (i.e. not currently visible) messages.
        """
        client = boto3.client('sqs')
        response = client.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible'
            ]
        )
        formatted = {
            'waiting': response.get('Attributes', {}).get('ApproximateNumberOfMessages'),
            'inflight': response.get('Attributes', {}).get('ApproximateNumberOfMessagesNotVisible')
        }
        return formatted
