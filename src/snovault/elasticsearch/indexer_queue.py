### Class to manage the items for indexing
# First round will use a standard SQS queue from AWS without Elasticache.

import boto3
import json
import math
import logging
import socket
import time
from pyramid.view import view_config
from pyramid.decorator import reify
from .interfaces import INDEXER_QUEUE, INDEXER_QUEUE_MIRROR
from .indexer_utils import (
    find_uuids_for_indexing,
    get_uuids_for_types
)

log = logging.getLogger(__name__)

def includeme(config):
    config.add_route('queue_indexing', '/queue_indexing')
    config.add_route('indexing_status', '/indexing_status')
    env_name = config.registry.settings.get('env.name')
    config.registry[INDEXER_QUEUE] = QueueManager(config.registry)
    # initialize the queue and dlq here
    confif.registry[INDEXER_QUEUE].initialize(dlq=True)
    # INDEXER_QUEUE_MIRROR is used because webprod and webprod2 share a DB
    if env_name and 'fourfront-webprod' in env_name:
        mirror_env = 'fourfront-webprod2' if env_name == 'fourfront-webprod' else 'fourfront-webprod'
        config.registry[INDEXER_QUEUE_MIRROR] = QueueManager(config.registry, mirror_env=mirror_env)
    else:
        config.registry[INDEXER_QUEUE_MIRROR] = None
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
        'number_queued': 0,
        'detail': 'Nothing was queued. Make sure to past in a list of uuids in in "uuids" key OR list of collections in the "collections" key of request the POST request.'
    }
    if not req_uuids and not req_collections:
        return response
    if req_uuids and req_collections:
        response['detail'] = 'Nothing was queued. You cannot provide both uuids and a collection for queueing at once.'
        return response
    if req_uuids and not isinstance(req_uuids, list):
        response['detail'] = 'Nothing was queued. When queueing uuids, make to sure to put a list of string uuids in the POST request.'
        return response
    if req_collections and not isinstance(req_collections, list):
        response['detail'] = 'Nothing was queued. When queueing a collection, make sure to provide a list of string collection names in the POST request.'
        return response
    queue_indexer = request.registry[INDEXER_QUEUE]
    # strict mode means uuids should be indexed without finding associates
    strict = request.json.get('strict', False)
    if req_uuids:
        queued, failed = queue_indexer.add_uuids(request.registry, req_uuids, strict=strict)
        response['requested_uuids'] = req_uuids
    else:
        queued, failed = queue_indexer.add_collections(request.registry, req_collections, strict=strict)
        response['requested_collections'] = req_collections
    response['notification'] = 'Success'
    response['number_queued'] = len(queued)
    response['detail'] = 'Successfuly queued items!'
    response['errors'] = failed
    response['strict'] = strict
    return response


@view_config(route_name='indexing_status', request_method='GET', permission="index")
def indexing_status(request):
    """
    Endpoint to check what is currently on the queue. Uses GET requests
    """
    queue_indexer = request.registry[INDEXER_QUEUE]
    response = {}
    try:
        numbers = queue_indexer.number_of_messages()
    except Exception as e:
        response['detail'] = str(e)
        response['status'] = 'Failure'
    else:
        response['detail'] = numbers
        response['status'] = 'Success'
    return response


class QueueManager(object):
    def __init__(self, registry, mirror_env=None):
        # batch sizes of messages. __all of these should be 10 at maximum__
        self.send_batch_size = 10
        self.receive_batch_size = 10
        self.delete_batch_size = 10
        self.replace_batch_size = 10
        # maximum number of uuids in a message
        self.send_uuid_threshold = 1
        self.env_name = mirror_env if mirror_env else registry.settings.get('env.name')
        # local development
        if not self.env_name:
            # make sure it's something aws likes
            backup = socket.gethostname()[:80].replace('.','-')
            # last case scenario
            self.env_name = backup if backup else 'fourfront-backup'
        self.queue_attrs = {
            'VisibilityTimeout': '120',  # increase if messages going to dlq
            'MessageRetentionPeriod': '1209600',  # 14 days, in seconds
            'ReceiveMessageWaitTimeSeconds': '2',  # 2 seconds of long polling
        }
        self.client = boto3.client('sqs', region_name='us-east-1')
        self.queue_name = self.env_name + '-indexer-queue'
        self.dlq_name = self.queue_name + '-dlq'
        # code below can cause SQS deletion/creation
        # if run from MPIndexer, will work, but not gracefully.
        # changes to queue config should be followed by running create_mapping
        self.dlq_url = self.get_queue_url(self.dlq_name)
        if self.dlq_url:
            # update queue_attrs with dlq info
            dlq_arn = self.get_queue_arn(self.dlq_url)
            redrive_policy = {  # maintain this order of settings
                'deadLetterTargetArn': dlq_arn,
                'maxReceiveCount': 4  # num of fails before sending to dlq
            }
            self.queue_attrs['RedrivePolicy'] = json.dumps(redrive_policy)
        self.queue_url = self.get_queue_url(self.queue_name)

    def add_uuids(self, registry, uuids, strict=False):
        """
        Takes a list of string uuids, finds all associated uuids using the
        indexer_utils, and then queues them all up. Also requires a registry,
        which is passed in automatically when using the /queue_indexing route.

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.

        NOTE: using find_uuids_for_indexing is smart, since it uses ES to find
        all items that either embed or link this uuid. If items are not yet
        indexed, they won't be queued for possibly redudanct re-indexing
        (those items should be independently queued)
        """
        if not strict:
            associated_uuids = find_uuids_for_indexing(registry, set(uuids), log)
            uuids_to_index = self.order_uuids_to_queue(uuids, list(associated_uuids))
        else:
            uuids_to_index = uuids
        failed = self.send_messages(uuids_to_index)
        return uuids_to_index, failed

    def add_collections(self, registry, collections, strict=False):
        """
        Takes a list of collection name, finds all associated uuids using the
        indexer_utils, and then queues them all up. Also requires a registry,
        which is passed in automatically when using the /queue_indexing route.

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.
        """
        ### IS THIS USING DATASTORE HERE?
        uuids = list(get_uuids_for_types(registry, collections))
        if not strict:
            associated_uuids = find_uuids_for_indexing(registry, set(uuids), log)
            uuids_to_index = self.order_uuids_to_queue(uuids, list(associated_uuids))
        else:
            uuids_to_index = uuids
        failed = self.send_messages(uuids_to_index)
        return uuids_to_index, failed

    def get_queue_url(self, queue_name):
        """
        Simple function that returns url of associated queue name
        """
        try:
            response = self.client.get_queue_url(
                QueueName=queue_name
            )
        except:
            response = {}
        return response.get('QueueUrl')

    def get_queue_arn(self, queue_url):
        """
        Get the ARN of the specified queue
        """
        response = self.client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        return response['Attributes']['QueueArn']

    def initialize(self, dlq=False):
        """
        Initialize the queue that is used by this manager.
        For now, this is an AWS SQS standard queue.
        Will use whatever attributes are defined within self.queue_attrs.
        If dlq arg is True, then the dead letter queue will be initialized
        as well.

        Returns a queue url that is guaranteed to link to the right queue.
        """
        queue_names = [self.queue_name, self.dlq_name] if dlq else [self.queue_name]
        for queue_name in queue_names:
            queue_url = self.get_queue_url(queue_name)
            should_set_attrs = False
            if queue_url:  # see if current settings are up to date
                curr_attrs = self.client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=list(self.queue_attrs.keys())
                ).get('Attributes', {})
                # must remove JSON formatting from redrivePolicy to compare
                compare_attrs = self.queue_attrs.copy()
                if 'RedrivePolicy' in compare_attrs:
                    compare_attrs['RedrivePolicy'] = json.loads(compare_attrs['RedrivePolicy'])
                if 'RedrivePolicy' in curr_attrs:
                    curr_attrs['RedrivePolicy'] = json.loads(curr_attrs['RedrivePolicy'])
                if compare_attrs == curr_attrs:
                    should_set_attrs = False
            else:  # queue needs to be created
                for backoff in [30, 30, 10, 20, 30, 60, 90]:  # totally arbitrary
                    try:
                        response = self.client.create_queue(
                            QueueName=queue_name,
                            Attributes=self.queue_attrs
                        )
                    except self.client.exceptions.QueueDeletedRecently:
                        log.warning('\n___MUST WAIT TO CREATE QUEUE FOR %ss___\n' % str(backoff))
                        time.sleep(backoff)
                    else:
                        log.warning('\n___CREATED QUEUE WITH NAME %s___\n' % queue_name)
                        break
            if queue_url and should_set_attrs:  # set attributes on an existing queue
                self.client.set_queue_attributes(
                    QueueUrl=queue_url,
                    Attributes=self.queue_attrs
                )


    def clear_queue(self):
        """
        Clear out the queue and dlq completely. You can no longer retrieve
        these messages. Takes up to 60 seconds.
        """
        for queue_url in [self.queue_url, self.dlq_url]:
            try:
                self.client.purge_queue(
                    QueueUrl=queue_url
                )
            except self.client.exceptions.PurgeQueueInProgress:
                log.warning('\n___QUEUE IS ALREADY BEING PURGED: %s___\n' % queue_url)

    def delete_queue(self, queue_url):
        """
        Remove the SQS queue with given queue_url from AWS
        Should really only be needed for local development.
        """
        response = self.client.delete_queue(
            QueueUrl=queue_url
        )
        self.queue_url = None
        return response

    def chunk_messages(self, messages, chunksize):
        """
        Chunk a given number of messages into chunks of given chunksize
        """
        for i in range(0, len(messages), chunksize):
            yield messages[i:i + chunksize]

    def send_messages(self, messages):
        """
        Send any number of 'messages' in a list.
        Can batch up to 10 messages, controlled by self.send_batch_size.
        This is easily controlled by self.send_uuid_threshold.

        messages argument should be a list of uuid strings.
        Returns information on messages that failed to queue
        """
        failed = []
        # we can handle 10 * self.send_uuid_threshold number of messages per go
        for total_batch in self.chunk_messages(messages, self.send_uuid_threshold * self.send_batch_size):
            entries = []
            for msg_batch in self.chunk_messages(total_batch, self.send_uuid_threshold):
                entries.append({
                    'Id': str(int(time.time() * 1000000)),
                    'MessageBody': ','.join(msg_batch)
                })
                time.sleep(0.001)  # in edge cases, Ids were repeated?
            response = self.client.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries
            )
            failed.extend(response.get('Failed', []))
        return failed

    def receive_messages(self):
        """
        Recieves up to self.receive_batch_size number of messages from the queue.
        Fewer (even 0) messages may be returned on any given run.
        Returns a list of messages with message metdata
        """
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.receive_batch_size,
            VisibilityTimeout=int(self.queue_attrs['VisibilityTimeout'])
        )
        # messages in response include ReceiptHandle and Body, most importantly
        return response.get('Messages', [])

    def delete_messages(self, messages):
        """
        Called after a message has been successfully received and processed.
        Removes message from the queue.
        Splits messages into a batch size given by self.delete_batch_size.
        Input should be the messages directly from receive messages. At the
        very least, needs a list of messages with 'Id' and 'ReceiptHandle'.
        Returns a list with any failed attempts.
        """
        failed = []
        for batch in self.chunk_messages(messages, self.delete_batch_size):
            # need to change message format, since deleting takes slightly
            # different fields what's return from receiving
            for i in range(len(batch)):
                to_delete = {
                    'Id': batch[i]['MessageId'],
                    'ReceiptHandle': batch[i]['ReceiptHandle']
                }
                batch[i] = to_delete
            response = self.client.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=batch
            )
            failed.extend(response.get('Failed', []))
        return failed

    def replace_messages(self, messages):
        """
        Called using received messages to place them back on the queue.
        Using a VisibilityTimeout of 0 means these messages are instantly
        available to consumers.
        Number of messages in a batch is controlled by self.replace_batch_size
        Input should be the messages directly from receive messages. At the
        very least, needs a list of messages with 'Id' and 'ReceiptHandle'.
        Returns a list with any failed attempts.
        """
        failed = []
        for batch in self.chunk_messages(messages, self.replace_batch_size):
            for i in range(len(batch)):
                to_replace = {
                    'Id': batch[i]['MessageId'],
                    'ReceiptHandle': batch[i]['ReceiptHandle'],
                    'VisibilityTimeout': 0
                }
                batch[i] = to_replace
            response = self.client.change_message_visibility_batch(
                QueueUrl=self.queue_url,
                Entries=batch
            )
            failed.extend(response.get('Failed', []))
        return failed

    def number_of_messages(self):
        """
        Returns a dict with number of waiting messages in the queue and
        number of inflight (i.e. not currently visible) messages.
        Also returns info on items in the dlq.
        """
        response = self.client.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible'
            ]
        )
        dlq_response = self.client.get_queue_attributes(
            QueueUrl=self.dlq_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible'
            ]
        )
        formatted = {
            'waiting': response.get('Attributes', {}).get('ApproximateNumberOfMessages'),
            'inflight': response.get('Attributes', {}).get('ApproximateNumberOfMessagesNotVisible'),
            'dlq_waiting': dlq_response.get('Attributes', {}).get('ApproximateNumberOfMessages'),
            'dlq_inflight': dlq_response.get('Attributes', {}).get('ApproximateNumberOfMessagesNotVisible')
        }
        # transform in integers
        for entry in formatted:
            try:
                formatted[entry] = int(formatted[entry])
            except ValueError:
                formatted[entry] = None
        return formatted

    def order_uuids_to_queue(self, original, to_add):
        """
        Given a list of original uuids and list of associated uuids that need
        to be indexed, extends the first list with the second without
        introducting duplicates. Returns extended list
        """

        unique_to_add = [uuid for uuid in to_add if uuid not in original]
        return original + unique_to_add

    def shutdown(self):
        pass
