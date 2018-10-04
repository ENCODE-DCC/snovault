"""
Adapater to connect public uuid queue functionality to a specific queue type
"""
import copy

from .aws_queues import AwsClient
from .aws_queues import AWS_SQS

from .base_queue import BaseClient
from .base_queue import BASE_IN_MEMORY

from .redis_queues import RedisClient
from .redis_queues import REDIS_LIST
from .redis_queues import REDIS_LIST_PIPE
from .redis_queues import REDIS_SET
from .redis_queues import REDIS_SET_PIPE
from .redis_queues import REDIS_SET_PIPE_EXEC


def _get_combined_uuids_gen(batch_by, uuid_len, max_value_size, uuids):
    combo_uuid = ''
    combine_count = 0
    combo_uuid_size = 0
    for uuid in uuids:
        new_size = len(uuid)
        if not new_size == uuid_len:
            raise ValueError(
                '_get_combined_uuids_gen: uuids are not correct length.'
                ' %d != %d' % (new_size, uuid_len)
            )
        to_be_size = combo_uuid_size + new_size
        if combine_count < batch_by and to_be_size < max_value_size:
            combine_count += 1
            combo_uuid_size = to_be_size
            combo_uuid += uuid
        else:
            yield combo_uuid
            combine_count = 1
            combo_uuid_size = new_size
            combo_uuid = uuid
    if combo_uuid:
        yield combo_uuid


def _get_uncombined_uuids(uuid_len, combo_uuid):
    uuids = []
    uuid_count = len(combo_uuid) // uuid_len
    for ind in range(0, uuid_count):
        start = uuid_len * ind
        uuid = combo_uuid[start:start  + uuid_len]
        uuids.append(uuid)
    return uuids


class UuidQueueTypes(object):
    '''
    Queue Type Manager

    * python enum package was not used due to versioning.  I assume it
    would be a better choice.
    '''
    AWS_SQS = AWS_SQS
    BASE_IN_MEMORY = BASE_IN_MEMORY
    REDIS_LIST = REDIS_LIST
    REDIS_LIST_PIPE = REDIS_LIST_PIPE
    REDIS_SET = REDIS_SET
    REDIS_SET_PIPE = REDIS_SET_PIPE
    REDIS_SET_PIPE_EXEC = REDIS_SET_PIPE_EXEC

    @classmethod
    def check_queue_type(cls, queue_type):
        '''Verify Queue Type'''
        for key in dir(cls):
            if not key[0] == '_' and key.isupper():
                if getattr(cls, key) == queue_type:
                    return True
        return False

    @classmethod
    def get_all(cls):
        '''Return all available queue types'''
        values = []
        for key in dir(cls):
            if not key[0] == '_' and key.isupper():
                value = getattr(cls, key)
                if cls.check_queue_type(value):
                    values.append(value)
        return values


class UuidQueueWorker(object):
    '''
    UuidQueueWorker is the base class for the UuidQueue

    * Worker needs a server ready to start.  After init loop
    server_ready to check for server queue availability
    '''
    def __init__(self, queue_name, queue_type, client_options):
        # arg must be set by calling server_ready
        self._batch_by = None
        self.restart = None
        self.snapshot_id = None
        self.uuid_len = None
        self.xmin = None
        if UuidQueueTypes.BASE_IN_MEMORY == queue_type:
            raise TypeError(
                'Not allowed to create a UuidQueueWorker of type %s.'
                '  Server and worker must be the same for this type.' % (
                    queue_type,
                )
            )
        client = self.get_client(queue_type, client_options)
        self._queue = client.get_queue(queue_name, queue_type)

    def _init_run_args(self, run_args):
        '''Called from server ready to set additional init args'''
        self._batch_by = run_args['batch_by']
        self.restart = run_args['restart']
        self.snapshot_id = run_args['snapshot_id']
        self.uuid_len = run_args['uuid_len']
        self.xmin = run_args['xmin']

    @staticmethod
    def get_client(queue_type, client_options, is_server=False):
        '''Get a queue client, used in worker and server'''
        if not UuidQueueTypes.check_queue_type(queue_type):
            raise ValueError('UuidQueueTypes type %s not handled' % queue_type)
        if UuidQueueTypes.BASE_IN_MEMORY == queue_type:
            if not is_server:
                raise TypeError(
                    'Not allowed to create a UuidQueueWorker of type %s.'
                    '  Server and worker must be the same for this type.' % (
                        queue_type,
                    )
                )
            client_cls = BaseClient
        elif UuidQueueTypes.AWS_SQS == queue_type:
            client_options['profile_name'] = client_options.get('profile_name', 'default')
            client_cls = AwsClient
        else:
            client_cls = RedisClient
            client_options['host'] = client_options.get('host', 'localhost')
            client_options['port'] = client_options.get('port', '6379')
        return client_cls(client_options)

    @property
    def queue_name(self):
        '''Getter for queue name'''
        return self._queue.queue_name

    @property
    def queue_type(self):
        '''Getter for queue type'''
        return self._queue.queue_type

    def _add_batch_to_qmeta(self, uuids):
        '''Save batch data to queue meta as they are removed'''
        batch_id = self._queue.qmeta.add_batch(uuids)
        return batch_id

    def add_finished(self, batch_id, successes, errors):
        '''Update queue with consumed uuids'''
        self._queue.qmeta.remove_batch(batch_id, successes, errors)

    def get_errors(self):
        '''Get all errors from queue that were sent in add_finished'''
        errors = self._queue.qmeta.get_errors()
        return errors

    def get_uuids(self, get_count=1):
        '''Get uuids as specified by get_count'''
        uuids = []
        call_cnt = 0
        if self._batch_by == 1:
            uuids, call_cnt = self._queue.get_values(get_count)
        else:
            if get_count < self._batch_by:
                batch_get_count = 1
            else:
                batch_get_count = ((get_count + 1)//self._batch_by) + 1
            combined_uuids_list, call_cnt = self._queue.get_values(
                batch_get_count
            )
            for combined_uuids in combined_uuids_list:
                uncombined_uuids = _get_uncombined_uuids(
                    self.uuid_len,
                    combined_uuids,
                )
                uuids.extend(uncombined_uuids)
        batch_id = None
        if uuids:
            batch_id = self._add_batch_to_qmeta(uuids)
        return batch_id, uuids, call_cnt

    def is_finished(self, max_age_secs=4999, listener_restarted=False):
        '''Check if queue has been consumed'''
        readd_values, is_done = self._queue.qmeta.is_finished(
            max_age_secs=max_age_secs, listener_restarted=listener_restarted,
        )
        return readd_values, is_done

    def queue_running(self):
        '''Get stop flag for queue'''
        return self._queue.qmeta.is_server_running()

    def server_ready(self):
        '''
        Called after creating a queue worker to check for
        queue server and related information

        * There are required args need to start a worker set
        by the server when it is created.
        '''
        required = (
            ('batch_by', int),
            ('uuid_len', int),
            ('snapshot_id', str),
            ('xmin', str),
            ('restart', bool),
        )
        args = self._queue.qmeta.get_run_args()
        if not args:
            return False
        for key, val_type in required:
            if key not in args:
                return False
            try:
                args[key] = val_type(args[key])
            except (TypeError, ValueError):
                return False
        self._init_run_args(args)
        return True


class UuidQueue(UuidQueueWorker):
    '''
    Indexer to Queue Adapter / Manager

    * Child class of UuidQueueWorker
    * Expands functionality so allow
        - Loading of uuids
        - Purging the queue
        - Updating the meta data added count
        - Setting the meta data stop flag to tell workers to stop
    '''
    def __init__(self, queue_name, queue_type, client_options):
        client = self.get_client(queue_type, client_options, is_server=True)
        self._queue = client.get_queue(queue_name, queue_type)

    def initialize(self, run_args):
        '''
        Initialize queue.

        * If queue crashes we do not want to reset in __init__.  Do this
        after we determine the last run finished successfully
        '''
        self._queue.qmeta.set_args()
        self._queue.qmeta.set_run_args(run_args)
        if self.server_ready():
            return True
        return False

    def _update_qmeta_count(self, count):
        '''
        Update the queue meta data uuids count when loaded

        - Could be negative when updating for readded uuids
        '''
        self._queue.qmeta.values_added(count)

    def _set_qmeta_stop_flag(self):
        '''Set queue meta data stop flag'''
        self._queue.qmeta.set_to_not_running()

    def load_uuids(self, uuids, readded=False):
        '''
        Load uuids into queue.

        * Will be batched by specified batch_by
        * readded are uuids that will be put back into the queue
            - queue meta add count needs to be fixed
        '''
        failed = []
        bytes_added = 0
        success_cnt = 0
        call_cnt = 0
        if self._batch_by == 1:
            failed, bytes_added, call_cnt = self._queue.add_values(uuids)
        else:
            combined_uuids_gen = _get_combined_uuids_gen(
                self._batch_by,
                self.uuid_len,
                self._queue.max_value_size,
                uuids,
            )
            failed, bytes_added, call_cnt = self._queue.add_values(
                combined_uuids_gen
            )
        success_cnt = bytes_added//self.uuid_len
        if readded:
            self._update_qmeta_count(-1 * len(uuids))
        self._update_qmeta_count(success_cnt)
        return failed, success_cnt, call_cnt


    def purge(self):
        '''Clear Queue'''
        self._queue.purge()
        self._queue.qmeta.purge_meta()

    def stop(self):
        '''Parent processes has determined the queue has been consume'''
        self._set_qmeta_stop_flag()
