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
from .indexer_utils import get_uuids_for_types

log = logging.getLogger(__name__)

def includeme(config):
    config.add_route('queue_indexing', '/queue_indexing')
    config.add_route('indexing_status', '/indexing_status')
    env_name = config.registry.settings.get('env.name')
    config.registry[INDEXER_QUEUE] = QueueManager(config.registry)
    # INDEXER_QUEUE_MIRROR is used because webprod and webprod2 share a DB
    if env_name and 'fourfront-webprod' in env_name:
        mirror_env = 'fourfront-webprod2' if env_name == 'fourfront-webprod' else 'fourfront-webprod'
        mirror_queue = QueueManager(config.registry, mirror_env=mirror_env)
        if not mirror_queue.queue_url:
            log.error('INDEXING: Mirror queue %s is not available!' % mirror_queue.queue_name)
            raise Exception('INDEXING: Mirror queue %s is not available!' % mirror_queue.queue_name)
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
    """
    Class for handling the queues responsible for coordinating indexing.
    Contains methods to inititalize queues, add both uuids and collections of
    uuids to the queue, and also various helper methods to receive/delete/replace
    messages on the queue.
    Currently the set up uses 3 queues:
    1. Primary queue for items that are directly posted, patched, or added.
    2. Secondary queue for associated items of those in the primary queue.
    3. Dead letter queue (dlq) for handling items that have issues processing
       from either the primary or secondary queues.
    """
    def __init__(self, registry, mirror_env=None):
        """
        __init__ will build all three queues needed with the desired settings.
        send_uuid_threshold and batch_size parameters are used to how many
        uuids are in a message and how many messages are batched together,
        respectively.
        """
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
        self.dlq_attrs = {
            'VisibilityTimeout': '120',  # increase if messages going to dlq
            'MessageRetentionPeriod': '1209600',  # 14 days, in seconds
            'ReceiveMessageWaitTimeSeconds': '2',  # 2 seconds of long polling
        }
        self.client = boto3.client('sqs', region_name='us-east-1')
        # primary queue name
        self.queue_name = self.env_name + '-indexer-queue'
        # secondary queue name
        self.second_queue_name = self.env_name + '-secondary-indexer-queue'
        self.dlq_name = self.queue_name + '-dlq'
        # initialize the queue and dlq here, but not on mirror queue
        if not mirror_env:
            response_urls = self.initialize(dlq=True)
            self.queue_url = response_urls.get(self.queue_name)
            self.second_queue_url = response_urls.get(self.second_queue_name)
            self.dlq_url = response_urls.get(self.dlq_name)
        else:  # assume the urls exist
            self.queue_url = self.get_queue_url(self.queue_name)
            self.second_queue_url = self.get_queue_url(self.second_queue_name)
            self.dlq_url = self.get_queue_url(self.dlq_name)

    def add_uuids(self, registry, uuids, strict=False, secondary=False):
        """
        Takes a list of string uuids queues them up. Also requires a registry,
        which is passed in automatically when using the /queue_indexing route.

        If strict, the uuids will be queued with info instructing associated
        uuids NOT to be queued.

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.
        """
        # secondary queue is always strict
        if secondary: strict = True
        failed = self.send_messages(uuids, strict=strict, secondary=secondary)
        return uuids, failed

    def add_collections(self, registry, collections, strict=False):
        """
        Takes a list of collection name and queues all uuids for them.
        Also requires a registry, which is passed in automatically when using
        the /queue_indexing route.

        If strict, the uuids will be queued with info instructing associated
        uuids NOT to be queued. Never uses secondary.

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.
        """
        ### IS THIS USING DATASTORE HERE?
        uuids = list(get_uuids_for_types(registry, collections))
        failed = self.send_messages(uuids_to_index, strict=strict)
        return uuids, failed

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
        # dlq MUST be initialized first if used
        if dlq:
            queue_names = [self.dlq_name, self.queue_name, self.second_queue_name]
        else:
            queue_names = [self.queue_name, self.second_queue_name]
        queue_urls = {}
        for queue_name in queue_names:
            queue_attrs = self.dlq_attrs if queue_name == self.dlq_name else self.queue_attrs
            queue_url = self.get_queue_url(queue_name)
            should_set_attrs = False
            if queue_url:  # see if current settings are up to date
                curr_attrs = self.client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=list(queue_attrs.keys())
                ).get('Attributes', {})
                # must remove JSON formatting from redrivePolicy to compare
                compare_attrs = queue_attrs.copy()
                if 'RedrivePolicy' in compare_attrs:
                    compare_attrs['RedrivePolicy'] = json.loads(compare_attrs['RedrivePolicy'])
                if 'RedrivePolicy' in curr_attrs:
                    curr_attrs['RedrivePolicy'] = json.loads(curr_attrs['RedrivePolicy'])
                should_set_attrs = compare_attrs != curr_attrs
            else:  # queue needs to be created
                for backoff in [30, 30, 10, 20, 30, 60, 90, 120]:  # totally arbitrary
                    try:
                        response = self.client.create_queue(
                            QueueName=queue_name,
                            Attributes=queue_attrs
                        )
                    except self.client.exceptions.QueueDeletedRecently:
                        log.warning('\n___MUST WAIT TO CREATE QUEUE FOR %ss___\n' % str(backoff))
                        time.sleep(backoff)
                    else:
                        log.warning('\n___CREATED QUEUE WITH NAME %s___\n' % queue_name)
                        queue_url = response['QueueUrl']
                        break
            # update the queue attributes with dlq information, which can only
            # be obtained after the dlq is created
            if queue_name == self.dlq_name:
                dlq_arn = self.get_queue_arn(queue_url)
                redrive_policy = {  # maintain this order of settings
                    'deadLetterTargetArn': dlq_arn,
                    'maxReceiveCount': 4  # num of fails before sending to dlq
                }
                # set redrive policy for main queue
                self.queue_attrs['RedrivePolicy'] = json.dumps(redrive_policy)
            # set attributes on an existing queue. not hit if queue was just created
            if should_set_attrs:
                self.client.set_queue_attributes(
                    QueueUrl=queue_url,
                    Attributes=queue_attrs
                )
            queue_urls[queue_name] = queue_url
        return queue_urls


    def clear_queue(self):
        """
        Clear out the queue and dlq completely. You can no longer retrieve
        these messages. Takes up to 60 seconds.
        """
        for queue_url in [self.queue_url, self.second_queue_url, self.dlq_url]:
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
        setattr(self, queue_url, None)
        return response

    def chunk_messages(self, messages, chunksize):
        """
        Chunk a given number of messages into chunks of given chunksize
        """
        for i in range(0, len(messages), chunksize):
            yield messages[i:i + chunksize]

    def send_messages(self, messages, strict=False, secondary=False):
        """
        Send any number of 'messages' in a list.
        Can batch up to 10 messages, controlled by self.send_batch_size.
        This is easily controlled by self.send_uuid_threshold.
        If secondary is True, then the secondary queue will be used. Otherwise,
        the primary queue is used by default.

        messages argument should be a list of uuid strings.
        strict is a boolean that determines whether or not associated uuids
        will be found for these uuids.
        Returns information on messages that failed to queue
        """
        failed = []
        # we can handle 10 * self.send_uuid_threshold number of messages per go
        for total_batch in self.chunk_messages(messages, self.send_uuid_threshold * self.send_batch_size):
            entries = []
            for msg_batch in self.chunk_messages(total_batch, self.send_uuid_threshold):
                proc_batch = ['/'.join([msg, str(strict)]) for msg in msg_batch]
                entries.append({
                    'Id': str(int(time.time() * 1000000)),
                    'MessageBody': ','.join(proc_batch)
                })
                time.sleep(0.001)  # in edge cases, Ids were repeated?
            response = self.client.send_message_batch(
                QueueUrl=self.second_queue_url if secondary else self.queue_url,
                Entries=entries
            )
            failed.extend(response.get('Failed', []))
        return failed

    def receive_messages(self, secondary=False):
        """
        Recieves up to self.receive_batch_size number of messages from the queue.
        Fewer (even 0) messages may be returned on any given run.
        If secondary is True, then the secondary queue will be used. Otherwise,
        the primary queue is used by default.

        Returns a list of messages with message metdata
        """
        response = self.client.receive_message(
            QueueUrl=self.second_queue_url if secondary else self.queue_url,
            MaxNumberOfMessages=self.receive_batch_size,
            VisibilityTimeout=int(self.queue_attrs['VisibilityTimeout'])
        )
        # messages in response include ReceiptHandle and Body, most importantly
        return response.get('Messages', [])

    def delete_messages(self, messages, secondary=False):
        """
        Called after a message has been successfully received and processed.
        Removes message from the queue.
        Splits messages into a batch size given by self.delete_batch_size.
        Input should be the messages directly from receive messages. At the
        very least, needs a list of messages with 'Id' and 'ReceiptHandle'.
        If secondary is True, then the secondary queue will be used. Otherwise,
        the primary queue is used by default.

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
                QueueUrl=self.second_queue_url if secondary else self.queue_url,
                Entries=batch
            )
            failed.extend(response.get('Failed', []))
        return failed

    def replace_messages(self, messages, secondary=False):
        """
        Called using received messages to place them back on the queue.
        Using a VisibilityTimeout of 0 means these messages are instantly
        available to consumers.
        Number of messages in a batch is controlled by self.replace_batch_size
        Input should be the messages directly from receive messages. At the
        very least, needs a list of messages with 'Id' and 'ReceiptHandle'.
        If secondary is True, then the secondary queue will be used. Otherwise,
        the primary queue is used by default.

        Returns a list with any failed attempts.
        """
        failed = []
        for batch in self.chunk_messages(messages, self.replace_batch_size):
            for i in range(len(batch)):
                to_replace = {
                    'Id': batch[i]['MessageId'],
                    'ReceiptHandle': batch[i]['ReceiptHandle'],
                    'VisibilityTimeout': 5
                }
                batch[i] = to_replace
            response = self.client.change_message_visibility_batch(
                QueueUrl=self.second_queue_url if secondary else self.queue_url,
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
        responses = []
        for queue_url in [self.queue_url, self.second_queue_url, self.dlq_url]:
            response = self.client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesNotVisible'
                ]
            )
            responses.append(response)
        formatted = {
            'primary_waiting': responses[0].get('Attributes', {}).get('ApproximateNumberOfMessages'),
            'primary_inflight': responses[0].get('Attributes', {}).get('ApproximateNumberOfMessagesNotVisible'),
            'secondary_waiting': responses[1].get('Attributes', {}).get('ApproximateNumberOfMessages'),
            'secondary_inflight': responses[1].get('Attributes', {}).get('ApproximateNumberOfMessagesNotVisible'),
            'dlq_waiting': responses[2].get('Attributes', {}).get('ApproximateNumberOfMessages'),
            'dlq_inflight': responses[2].get('Attributes', {}).get('ApproximateNumberOfMessagesNotVisible')
        }
        # transform in integers
        for entry in formatted:
            try:
                formatted[entry] = int(formatted[entry])
            except ValueError:
                formatted[entry] = None
        return formatted

    def shutdown(self):
        pass
