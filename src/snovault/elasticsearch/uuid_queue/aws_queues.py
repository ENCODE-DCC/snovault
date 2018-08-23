'''Aws uuid queue'''
import time
import boto3

from .base_queue import UuidBaseQueue
from .base_queue import UuidBaseQueueMeta


AWS_SQS = 'AWS_SQS'


def _check_status_code(res):
    if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
        return True
    return False


class AwsClient(object):
    '''
    AWS Queue Client

    Creates or finds a preexisting AWS SQS message queue by name.  The
    queue url can be found by name too.  The queue url is used to find
    the actual queue.

    * purge is implement in the client because the client class seems to be
    the way to access exceptions.
    * Some AWS SQS functionality, like purge and create, must have wait times
    between requests.

    '''
    allowed_create_waits = 1

    def __init__(self, args):
        session = boto3.session.Session(profile_name=args['profile_name'])
        self._client = session.client('sqs')

    @classmethod
    def purge(cls, queue, retry=True):
        '''Remove aws queue'''
        try:
            res = queue.purge()
            if not _check_status_code(res):
                raise ValueError('AwsSqsQueue.purge() Failed:' + str(res))
        except cls.exceptions.PurgeQueueInProgress as ecp:  # pylint: disable=no-member
            if retry:
                print('Waiting 65 seconds due to PurgeQueueInProgress')
                time.sleep(65)
                cls.purge(queue, retry=False)
            raise ecp

    def _create_queue(self, queue_name, args):
        try:
            res = self._client.create_queue(
                QueueName=queue_name,
                Attributes=args
            )
        except self._client.exceptions.QueueDeletedRecently:
            if self.allowed_create_waits:
                self.allowed_create_waits -= 1
                print('Waiting 65 seconds due to QueueDeletedRecently')
                time.sleep(65)
                return self._create_queue(queue_name, args)
        if _check_status_code(res):
            return True
        return False

    def _get_queue_url(self, queue_name):
        try:
            res = self._client.get_queue_url(QueueName=queue_name)
        except self._client.exceptions.QueueDoesNotExist:
            res = {}
        if res.get('QueueUrl'):
            return res['QueueUrl']
        return None

    def get_queue(self, queue_name, queue_type, queue_options):
        '''Get queue from AWS, creates if does not exist'''
        if queue_type == AWS_SQS:
            queue_url = self._get_queue_url(queue_name)
            if not queue_url:
                if self._create_queue(queue_name, queue_options['attributes']):
                    queue_url = self._get_queue_url(queue_name)
            if not queue_url:
                raise ValueError(
                    'Queue %s is not found nor could be created.' %
                    queue_name
                )
            return AwsSqsQueue(queue_name, queue_url)
        else:
            raise ValueError('Queue %s is not available' % queue_type)


class AwsSqsQueueMeta(UuidBaseQueueMeta):

    def purge_meta(self):
        print('AwsSqsQueueMeta.purge')

class AwsSqsQueue(UuidBaseQueue):
    _max_entry_values = 10  # AWS limitation on message per entry.  May 2018.
    max_value_size = 262144  # AWS SQS Limit for single and batches. May 2018
    queue_type = AWS_SQS

    def __init__(self, queue_name, queue_url):
        resource = boto3.resource('sqs')
        self.queue = resource.Queue(queue_url)
        self.queue_name = queue_name
        self.qmeta = AwsSqsQueueMeta()

    # Add Values
    def _add_value(self, entries):
        res = self.queue.send_messages(Entries=entries)
        if _check_status_code(res):
            return True
        return False

    def _build_entires(self, values):
        '''
        Build entries list given a list of values
        -Entries are used to maximize the messages sent per AWS SQS
        send messages calls.
        -Each value needs a unique id within each request
        -Total entries size is limited by max_value_size too
        '''
        entries = []
        entries_msg_size = 0
        for id_int, value in enumerate(values, 1):
            new_size = len(value)
            if new_size > self.max_value_size:
                raise ValueError('AWS SQS message is too long.')
            to_be_size = entries_msg_size + new_size
            if to_be_size >= self.max_value_size:
                # Total entries messages size limit reached
                yield entries, entries_msg_size
                entries = []
                entries_msg_size = 0
            entries_msg_size += new_size
            entries.append({'Id': str(id_int), 'MessageBody': value})
            if len(entries) == self._max_entry_values:
                # Total entries list limit reached
                yield entries, entries_msg_size
                entries = []
                entries_msg_size = 0
        yield entries, entries_msg_size

    def add_values(self, values):
        '''
        Wrapper to add many values to AWS SQS queue at once

        What is an entry? AWS SQS allows us to send 10 values at once.
        The 'Entries' concept exists in the AWS SQS queue to send many
        values at once.  Max of 10.  Size is limited to 262144 bytes for
        the whole list.  This is in addition to uuid_queue_adapter
        functionality to combine values into groups.  Oddly, the size limit
        on an individual message is the same as the size limit on a group
        of 10 messages.
        '''
        failed = []
        bytes_added = 0
        call_cnt = 0
        for entries, entries_bytes in self._build_entires(values):
            if entries:
                ret_value = self._add_value(entries)
                if ret_value is False:
                    failed.append(entries)
                else:
                    call_cnt += 1
                    bytes_added += entries_bytes
        return failed, bytes_added, call_cnt

    # Get Values
    @staticmethod
    def _get_msg_values(got_msgs):
        values = []
        msg_size = 0
        for sqs_msg in got_msgs:
            value = sqs_msg.body
            res_del = sqs_msg.delete()
            if _check_status_code(res_del):
                msg_size += len(value)
                values.append(value)
        return values, msg_size

    def get_values(self, get_count):
        '''
        Aws has a max number for receive messages and does not guarantee we
        receive the max.  We always ask for the max, so the number of return
        values could be greater than asked for.
        '''
        values = []
        call_cnt = 0
        got_msg = self.queue.receive_messages(
            MaxNumberOfMessages=self._max_entry_values
        )
        while got_msg:
            msg_values, msg_bytes = self._get_msg_values(got_msg)
            values.extend(msg_values)
            call_cnt += 1
            if len(values) >= get_count:
                break
            got_msg = self.queue.receive_messages(
                MaxNumberOfMessages=self._max_entry_values
            )
        return values, call_cnt

    # Other
    def purge(self):
        '''
        Clear all values from AWS SQS queue
        - Pass to the AwsClient because client has access to exceptions
        '''
        AwsClient.purge(self.queue)
