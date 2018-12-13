"""
Adapater to connect public uuid queue functionality to a specific queue type
"""
import time

from .queues.base_queue import (
    BaseQueueClient,
    BASE_QUEUE_TYPE,
)
from .queues.redis_queues import (
    REDIS_LIST,
    REDIS_LIST_PIPE,
    REDIS_SET,
    REDIS_SET_PIPE,
    REDIS_SET_PIPE_EXEC,
    RedisClient,
)


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
        if combine_count < batch_by and to_be_size <= max_value_size:
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


class QueueTypes(object):
    '''
    Queue Type Manager
    '''
    QUEUES = [
        (BASE_QUEUE_TYPE, BaseQueueClient),
        (REDIS_LIST, RedisClient),
        (REDIS_LIST_PIPE, RedisClient),
        (REDIS_SET, RedisClient),
        (REDIS_SET_PIPE, RedisClient),
        (REDIS_SET_PIPE_EXEC, RedisClient),
    ]

    @classmethod
    def check_queue_type(cls, queue_type):
        '''Verify Queue Type'''
        queues = cls.get_all()
        if queue_type in queues:
            return True
        return False

    @classmethod
    def get_all(cls):
        '''Return all available queue types'''
        values = []
        for queue_type, _ in cls.QUEUES:
            values.append(queue_type)
        return values

    @classmethod
    def get_queue_client_class(cls, given_queue_type):
        '''Get the client for given queue type'''
        for queue_type, queue_client_class in cls.QUEUES:
            if queue_type == given_queue_type:
                return queue_client_class
        return None


class QueueAdapter(object):
    """
    Indexer to Queue Adapter / Manager
    """
    def __init__(self, queue_name, queue_type, queue_options):
        self._queue_type = queue_type
        self._queue_options = queue_options
        self._start_us = int(time.time() * 1000000)
        self.queue_id = str(self._start_us)
        self._queue = self._get_queue(queue_name)
        if self._queue:
            self._queue_name = self._queue.queue_name


    # Errors
    def _has_errors(self):
        """Check if index session errors in queue meta through queue"""
        return self._queue.has_errors()

    def pop_errors(self):
        """Pop errors from queue meta through queue"""
        return self._queue.pop_errors()

    # Worker
    def _get_worker_id(self):
        worker_conn_cnt = self._queue.get_worker_conn_count()
        time_id = self._start_us + worker_conn_cnt + 1
        return str(time_id)

    def _get_worker_queue(self):
        """Queue to create workers with"""
        if self._queue.queue_type == BASE_QUEUE_TYPE:
            # For base queue types,
            # server and worker queue point to same obj
            worker_queue = self._queue
        else:
            worker_queue = self._get_queue(self._queue_name, is_worker=True)
        return worker_queue

    def get_worker(self):
        """Return the associated worker"""
        worker_id = self._get_worker_id()
        # Assume that work conn id does not already exist
        self._queue.add_worker_conn(worker_id)
        worker_queue = self._get_worker_queue()
        return WorkerAdapter(
            self._queue_name,
            self._queue_options,
            worker_id,
            worker_queue,
        )

    def update_worker_conn(self, worker_id, uuid_cnt, get_cnt):
        """Update worker connection in queue meta through queue"""
        self._queue.update_worker_conn(worker_id, uuid_cnt, get_cnt)

    # Uuids
    def has_uuids(self, errs_cnt=0):  # pylint: disable=unused-argument
        '''Are there uuids in the queue'''
        return self._queue.has_uuids(errs_cnt=errs_cnt)

    def _load_uuids(self, uuids): #
        """
        Call load uuids on specific queue.

        * Handles combining uuids if configured
        """
        failed_batches = []
        bytes_added = 0
        success_cnt = 0
        batch_size = self._queue_options['batch_size']
        uuid_len = self._queue_options['uuid_len']
        if batch_size == 1:
            bytes_added, failed_batches = self._queue.load_uuids(uuids)
        else:
            combined_uuids_gen = _get_combined_uuids_gen(
                batch_size,
                uuid_len,
                self._queue.max_value_size,
                uuids,
            )
            bytes_added, failed_batches = self._queue.load_uuids(
                combined_uuids_gen
            )
        success_cnt = bytes_added//uuid_len
        if failed_batches:
            # ToDo: Handle failed uuids
            print(
                'UuidQueue adapater, handle failed uuids on load: %d batches'
                ' failed' % len(failed_batches)
            )
        return success_cnt

    def load_uuids(self, uuids):
        '''
        Wrapper for loading uuids

        * Checks if indexing already running, one load per indexing session
        * Handles meta data
        '''
        if self.is_indexing():
            return None
        success_cnt = self._load_uuids(uuids)
        self._queue.update_uuid_count(success_cnt)
        return success_cnt

    # Run
    def is_indexing(self, errs_cnt=0):
        '''Is an indexing process currently running'''
        if self.has_uuids(errs_cnt=errs_cnt) or self._has_errors():
            return True
        worker_conns = self._queue.get_worker_conns()
        for worker_conn in worker_conns.values():
            if int(worker_conn['uuid_cnt']):
                return True
        return False

    # Queue Client
    def _get_queue(self, queue_name, is_worker=False):
        client_class = QueueTypes.get_queue_client_class(self._queue_type)
        if client_class:
            client = client_class(self._queue_options)
            return client.get_queue(
                queue_name,
                self._queue_type,
                is_worker=is_worker,
            )
        return None

    def close_indexing(self):
        '''Close indexing sessions'''
        self._queue.close_indexing()


class WorkerAdapter(object):
    '''
    UuidQueueWorker to adapt to all uuid queue types

    * For in memory base queue,
    the queue arg in init should be the same object as the server
    '''
    def __init__(self, queue_name, queue_options, worker_id, queue):
        self._queue_name = queue_name
        self._queue_options = queue_options
        self.worker_id = worker_id
        self._queue = queue
        self.is_running = False
        # Worker conn info
        self.get_cnt = 0
        self.uuid_cnt = 0
        self.processes = self._queue_options['processes']
        self.chunk_size = self._queue_options['chunk_size']

    # Uuids
    @staticmethod
    def _get_uncombined_uuids(uuid_len, combo_uuid):
        """Wrapper to call top level function"""
        return _get_uncombined_uuids(uuid_len, combo_uuid)

    def _get_uuids(self):
        '''The only way to get uuids from queue'''
        uuids = []
        batch_size = self._queue_options['batch_size']
        get_size = self._queue_options['get_size']
        get_count = get_size // batch_size + 1
        if batch_size == 1:
            uuids = self._queue.get_uuids(1)
        else:
            combined_uuids_list = self._queue.get_uuids(get_count)
            for combined_uuids in combined_uuids_list:
                uncombined_uuids = self._get_uncombined_uuids(
                    self._queue_options['uuid_len'],
                    combined_uuids,
                )
                uuids.extend(uncombined_uuids)
        return uuids

    def get_uuids(self, get_all=False):
        '''Get uuids and update queue meta data'''
        self.get_cnt += 1
        uuids = []
        if self.uuid_cnt == 0:
            if get_all:
                tmp_uuids = self._get_uuids()
                while tmp_uuids:
                    uuids.extend(tmp_uuids)
                    tmp_uuids = self._get_uuids()
            else:
                uuids = self._get_uuids()
            self.uuid_cnt = len(uuids)
            self._queue.update_uuid_count(-1 * self.uuid_cnt)
        self._queue.update_worker_conn(
            self.worker_id,
            self.uuid_cnt,
            self.get_cnt,
        )
        return uuids

    # Run
    def update_finished(self, batch_results):
        '''Update server with batch results'''
        self._queue.update_success_count(batch_results['successes'])
        self._queue.update_errors_count(len(batch_results['errors']))
        msg = self._queue.update_finished(self.worker_id, batch_results)
        if msg == 'Okay':
            self.uuid_cnt = 0
            msg = None
        else:
            msg = 'Update finished could not reset worker: %s' % msg
        return msg
