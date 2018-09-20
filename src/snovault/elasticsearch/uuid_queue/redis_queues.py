'''Redis Queues'''
import time

from redis import StrictRedis # pylint: disable=import-error

from .base_queue import UuidBaseQueue
from .base_queue import UuidBaseQueueMeta


REDIS_LIST = 'REDIS_LIST'
REDIS_LIST_PIPE = 'REDIS_LIST_PIPE'
REDIS_SET = 'REDIS_SET'
REDIS_SET_PIPE = 'REDIS_SET_PIPE'
REDIS_SET_PIPE_EXEC = 'REDIS_SET_PIPE_EXEC'


# pylint: disable=too-few-public-methods
class RedisClient(StrictRedis):
    '''One and only Redis Client class'''

    def __init__(self, args):
        super().__init__(
            charset="utf-8",
            decode_responses=True,
            db=args.get('db', 0),
            host=args['host'],
            port=args['port'],
            socket_timeout=5,
        )

    def get_queue(self, queue_name, queue_type):
        '''Return the queue by queue_type'''
        if queue_type == REDIS_LIST:
            queue_class = RedisListQueue
        elif queue_type == REDIS_LIST_PIPE:
            queue_class = RedisListPipeQueue
        elif queue_type == REDIS_SET:
            queue_class = RedisSetQueue
        elif queue_type == REDIS_SET_PIPE:
            queue_class = RedisSetPipeQueue
        elif queue_type == REDIS_SET_PIPE_EXEC:
            queue_class = RedisSetPipeExecQueue
        else:
            raise ValueError('Queue %s is not available' % queue_type)
        return queue_class(queue_name, self)


class RedisQueueMeta(UuidBaseQueueMeta):
    '''
    Redis meta wrapper

    - Server and client(workers) use meta store in redis
    '''
    def __init__(self, queue_name, client):
        super().__init__()
        self._queue_name = queue_name
        self._client = client
        self._base_id = int(time.time() * 1000000)
        self._exceptions = []
        # mb for base meta, ra for run args, etc...
        self._key_metabase = queue_name + ':mb'
        self._key_runargs = self._key_metabase + ':ra'
        self._key_addedcount = self._key_metabase + ':ca'
        self._key_successescount = self._key_metabase + ':cs'
        self._key_errors = self._key_metabase + ':be'
        self._key_errorscount = self._key_metabase + ':ce'
        self._key_isrunning = self._key_metabase + ':running'

    def set_args(self):
        '''Initial args'''
        self._client.set(self._key_addedcount, 0)
        self._client.set(self._key_isrunning, 'true')
        self._client.set(self._key_errorscount, 0)
        self._client.set(self._key_successescount, 0)

    def _add_errors(self, errors):
        '''Add errors as batch after consumed'''
        uuid_errors = []
        for error in errors:
            error_key = self._key_errors + ':' + error['uuid']
            uuid_errors.append(error['uuid'])
            self._client.hmset(error_key, error)
        self._client.incrby(self._key_errorscount, len(errors))

    def _get_batch_keys_for_id(self, batch_id):
        '''Return batch keys for uuids'''
        # ex for expired, ts for timestamp, vs for values
        expired = self._key_metabase + ':' + batch_id + ':ex'
        timestamp = self._key_metabase + ':' + batch_id + ':ts'
        values = self._key_metabase + ':' + batch_id + ':vs'
        return expired, timestamp, values

    def _get_batch_id_from_key(self, key_str):
        '''Extracts the batch_id from any batch_keys'''
        return key_str[len(self._key_metabase) + 1:-3]

    def _expire_batch(self, bk_expired):
        '''This will expire a batch'''
        self._client.set(bk_expired, 1)

    def _check_expired(self, max_age_secs, listener_restarted=False):
        '''
        Checks for expired batches, deletes, and returns values
        * if listener_restarted then all batches will be considered expired
        '''
        expired_values = []
        # bk_timestamp all key
        _, bk_timestamp_all, _ = self._get_batch_keys_for_id('*')
        for bk_timestamp in self._client.keys(bk_timestamp_all):
            batch_id = self._get_batch_id_from_key(bk_timestamp)
            bk_expired, bk_timestamp, bk_values = self._get_batch_keys_for_id(str(batch_id))
            timestamp = int(self._client.get(bk_timestamp)) // 1000000
            age = time.time() - timestamp
            int_expired = int(self._client.get(bk_expired))
            if age >= max_age_secs or listener_restarted or int_expired == 1:
                batch_uuids = self._client.lrange(bk_values, 0, -1)
                expired_values.extend(batch_uuids)
                self._expire_batch(bk_expired)
        return expired_values

    def is_server_running(self):
        '''Return boolean for server running flag'''
        if self._client.exists(self._key_isrunning):
            str_val = self._client.get(self._key_isrunning)
            if str_val == 'true':
                return True
        return False

    def set_to_not_running(self):
        '''
        Set server running flag off

        - Tells the workers to stop
        '''
        self._client.set(self._key_isrunning, 'false')

    def purge_meta(self):
        '''Remove all keys with queue_name:meta'''
        for key in self._client.keys(self._key_metabase + '*'):
            self._client.delete(key)

    def add_batch(self, values):
        '''
        Values removed from queue are stored in batch as a list for values
        until consumed and remove_batch is called with the batch_id.

        -A timestamp is added for expiration.
        -Expiration handled in is_finished.
        '''
        if values:
            batch_id = str(self._base_id)
            self._base_id += 1
            bk_expired, bk_timestamp, bk_values = self._get_batch_keys_for_id(str(batch_id))
            self._client.set(bk_timestamp, int(time.time() * 1000000))
            self._client.set(bk_expired, 0)
            self._client.lpush(bk_values, *values)
            return batch_id
        return None

    def _remove_batch(self, batch_id, successes, errors, bk_expired, bk_timestamp, bk_values):
        '''Method for removing a batch'''
        self._client.incrby(self._key_successescount, successes)
        if errors:
            self._add_errors(errors)
        self._client.delete(bk_expired)
        self._client.delete(bk_timestamp)
        self._client.delete(bk_values)

    def remove_batch(self, batch_id, successes, errors):
        '''
        Updates queue server after a batch is consumed by a queue worker.
        - If any checks fail the batch is set as expired. Expired batches are
          handled by the queue server as it loops through is_finished.

        Results:
            batch passes and batch_keys for batch_id are removed OR batch is
            expired. Expired batches are handled by the queue server as it
            loops through is_finished.
        '''
        # TODO: Maybe try to hide this method from the server queue.
        bk_expired, bk_timestamp, bk_values = self._get_batch_keys_for_id(str(batch_id))
        len_batch_values = self._client.llen(bk_values)
        did_check_out = (len(errors) + successes) == len_batch_values
        if int(self._client.get(bk_expired)) != 0 or not did_check_out:
            # TODO: Warn about expiring batches here, or have a message
            # somewhere.
            self._expire_batch(bk_expired)
        else:
            self._remove_batch(
                batch_id, successes, errors, bk_expired, bk_timestamp, bk_values
            )

    def get_run_args(self):
        '''Return run args needed for workers'''
        run_args = self._client.hgetall(self._key_runargs)
        return run_args

    def set_run_args(self, run_args):
        '''Add run args needed for workers'''
        set_args = {
            'batch_by': run_args['batch_by'],
            'restart': run_args['restart'],
            'snapshot_id': run_args['snapshot_id'],
            'uuid_len': run_args['uuid_len'],
            'xmin': run_args['xmin'],
        }
        self._client.hmset(self._key_runargs, set_args)

    def get_errors(self):
        '''Get all errors from queue that were sent in remove_batch'''
        errors = []
        errors_cnt = int(self._client.get(self._key_errorscount))
        for error_key in self._client.keys(self._key_errors + ':*'):
            err_dict = self._client.hgetall(error_key)
            errors.append(err_dict)
        if errors_cnt != len(errors):
            print(
                'MAKE A WARNING: error count is off: %d != %d' % (
                    errors_cnt,
                    len(errors),
                )
            )
        return errors

    def is_finished(self, max_age_secs=5002, listener_restarted=False):
        '''
        Looped in server queue to check if queue has been consumed.

        args:
            - max_age_secs: Used by _check_expired to expire batches
            older than value.
            - listener_restarted: Used by _check_expired to
            expire all batches now.  For Example, due to a server restart during an
            indexing process.

        returns:
            - expired_values: values from expired batches.
            - did_finish: True if no expred_values and consumed values is equal
              the values added.
        '''
        # TODO: Maybe try to hide this method from the worker queue.
        expired_values = []
        did_finish = False
        if max_age_secs:
            expired_values = self._check_expired(
                max_age_secs,
                listener_restarted=listener_restarted,
            )
        if not expired_values:
            errors_cnt = int(self._client.get(self._key_errorscount))
            successes_cnt = int(self._client.get(self._key_successescount))
            uuids_added = int(self._client.get(self._key_addedcount))
            uuids_handled = successes_cnt + errors_cnt
            did_finish = uuids_handled == uuids_added
        return expired_values, did_finish

    def values_added(self, len_values):
        '''
        Update successfully added values

        - Also used to remove readded values so could be negative
        '''
        self._client.incrby(self._key_addedcount, len_values)

    def is_useable(self):
        '''
        Check the consistency of the meta data values

        * Useful when queue crashes and needs a restart
        '''
        # self._key_runargs = self._key_metabase + ':ra'  # ra for run args
        added_cnt = 0
        errors_cnt = 0
        errors_len = 0
        successes_cnt = 0
        queue_len = 0
        if self._client.exists(self._key_addedcount):
            added_cnt = int(self._client.get(self._key_addedcount))
        else:
            self._client.set(self._key_addedcount, added_cnt)
        if self._client.exists(self._key_errorscount):
            errors_cnt = int(self._client.get(self._key_errorscount))
        else:
            self._client.set(self._key_errorscount, errors_cnt)
        if self._client.exists(self._key_errors):
            errors_len = self._client.llen(self._key_errors)
        if self._client.exists(self._key_runargs):
            try:
                self.get_run_args()
            except KeyError:
                return False
        if self._client.exists(self._key_successescount):
            successes_cnt = int(self._client.get(self._key_successescount))
        else:
            self._client.set(self._key_successescount, successes_cnt)
        if self._client.exists(self._queue_name):
            queue_len = self._client.llen(self._queue_name)
        if added_cnt != (errors_cnt + successes_cnt + queue_len):
            return False
        if errors_cnt != errors_len:
            return False
        return True


class RedisQueue(UuidBaseQueue):
    '''
    Base non-pipe redis queue for all redis queue types, includeing pipes

    - Cannot be used directly
    - Should override all methods in UuidBaseQueue
    '''
    add_str = None
    get_str = None
    len_str = None
    max_value_size = 262144  # Arbitraitly set to AWS SQS Limit
    queue_type = None

    def __init__(self, queue_name, client):
        super().__init__(queue_name)
        self._values = None  # Not used in redis queue
        self._client = client
        self.qmeta = RedisQueueMeta(self.queue_name, self._client)

    def _call_func(self, func_str, value=None):
        """
        Connection Error wrapper for all redis client calls
        """
        if not hasattr(self._client, func_str):
            raise ValueError(
                'Queue %s does not have %s' % (self.queue_name, func_str)
            )
        func = getattr(self._client, func_str)
        try:
            if value:
                return func(self.queue_name, value)
            return func(self.queue_name)
        except ConnectionError:  # pylint: disable=undefined-variable
            return False

    # Add Values
    def _add_value(self, value):
        ret_val = self._call_func(self.add_str, value)
        return ret_val

    # Get Values
    def _get_value(self):
        value = self._call_func(self.get_str)
        return value

    # Other
    def does_exist(self):
        '''
        Checks if the queue keys exists and is correct type

        * Redis exists should return as boolean
        '''
        if int(self._client.exists(self.queue_name)):
            return True
        return False

    def has_values(self):
        '''Check queue for values'''
        if self.queue_length():
            return True
        return False

    def purge(self):
        self._client.delete(self.queue_name)

    def queue_length(self):
        '''Get queue length from redis'''
        if self.len_str:
            return self._call_func(self.len_str)
        return None

    def is_queue_empty(self):
        '''Checks if queue is empty'''
        return self.does_exist()


class RedisPipeQueue(RedisQueue):
    '''
    Base pipe redis queue for all redis queue types

    - Pipe allows many queries to be sent at once
    - Cannot be used directly
    - Should override all methods in UuidBaseQueue
    '''

    @staticmethod
    def _call_pipe(pipe):
        try:
            # At the time of writting pipe return a list of length
            # Each item is the length of the queue when it was added.
            ret_list = pipe.execute()
            return ret_list
        except ConnectionError: # pylint: disable=undefined-variable
            return None

    def _get_pipe(self, func_str):
        pipe = self._client.pipeline()
        pipe_func = getattr(pipe, func_str)
        return pipe, pipe_func

    # Add Values
    def add_values(self, values):
        pipe, pipe_func = self._get_pipe(self.add_str)
        failed = []
        bytes_added = 0
        call_cnt = 0
        for value in values:
            value_len = len(value) # currently all values should be same length
            pipe_func(self.queue_name, value)
            bytes_added += value_len
            call_cnt += 1
        ret_val = self._call_pipe(pipe)
        # ret_val is a list of the redis queue count after insertion of pipe item
        if ret_val is False:
            ret_val = []
            failed = values
        else:
            diff = call_cnt - len(ret_val)
            bytes_added -= value_len * diff
            failed = [str(num) for num in range(diff)]
        return failed, bytes_added, call_cnt

    # Get Values
    def get_values(self, get_count):
        pipe, pipe_func = self._get_pipe(self.get_str)
        values = []
        call_cnt = 0
        while call_cnt < get_count:
            pipe_func(self.queue_name)
            call_cnt += 1
        ret_val = self._call_pipe(pipe)
        if ret_val:
            for val in ret_val:
                if val:
                    values.append(val)
        return values, call_cnt


class RedisListQueue(RedisQueue):
    '''List queue'''
    queue_type = REDIS_LIST
    add_str = 'lpush'
    get_str = 'lpop'
    ret_str = 'rpush'
    len_str = 'llen'


class RedisListPipeQueue(RedisPipeQueue):
    '''Pipe List queue'''
    queue_type = REDIS_LIST_PIPE
    add_str = 'lpush'
    get_str = 'lpop'
    ret_str = 'rpush'
    len_str = 'llen'


class RedisSetQueue(RedisQueue):
    '''Set queue'''
    queue_type = REDIS_SET
    add_str = 'sadd'
    get_str = 'spop'
    ret_str = 'sadd'
    len_str = 'scard'


class RedisSetPipeQueue(RedisPipeQueue):
    '''Pipe queue'''
    queue_type = REDIS_SET_PIPE
    add_str = 'sadd'
    get_str = 'spop'
    ret_str = 'sadd'
    len_str = 'scard'


class RedisSetPipeExecQueue(RedisSetPipeQueue):
    '''
    Set pipe qith exec functionality

    - Probably the fastest
    '''
    queue_type = REDIS_SET_PIPE_EXEC
    get_str = 'SPOP'

    def get_values(self, get_count):
        """
        Remove and return a random member of set ``name``

        Modified from https://github.com/andymccurdy/redis-py/blob/master/redis/client.py
        directly sinces it is not implemented in this version
        """
        values = []
        bytes_got = 0
        call_cnt = 0
        # Forgot how execute_command works.  Fix the pylint error when you remember.
        args = (get_count is not None) and [get_count] or []
        ret_val = self._client.execute_command(self.get_str, self.queue_name, *args)
        call_cnt += 1
        if ret_val:
            for val in ret_val:
                if val:
                    bytes_got += len(val)
                    values.append(val)
        return values, call_cnt
