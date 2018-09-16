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
            db=0,
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
        self._meta_base_key = queue_name + ':bm'  # bm for base meta
        self._run_args_key = self._meta_base_key + ':ra'  # ra for run args
        self._added_cnt_key = self._meta_base_key + ':ca'  # ca for count added
        self._successes_cnt_key = self._meta_base_key + ':cs'  # cs for count successes
        self._errors_key = self._meta_base_key + ':be'  # be for base errors
        self._errors_cnt_key = self._meta_base_key + ':ce' # ce for count errors
        self._is_running_key = self._meta_base_key + ':running'
        self._exceptions = []

    def set_args(self):
        '''Initial args'''
        self._client.set(self._added_cnt_key, 0)
        self._client.set(self._is_running_key, 'true')
        self._client.set(self._errors_cnt_key, 0)
        self._client.set(self._successes_cnt_key, 0)

    def _add_errors(self, batch_id, errors):
        '''Add errors as batch after consumed'''
        uuid_errors = []
        for error in errors:
            error_key = self._errors_key + ':' + error['uuid']
            uuid_errors.append(error['uuid'])
            self._client.hmset(error_key, error)
        self._client.incrby(self._errors_cnt_key, len(errors))

    def _get_batch_keys(self, batch_id):
        '''Return base keys for uuids'''
        bk_expired = self._meta_base_key + ':' + batch_id + ':ex'  # ex for values
        bk_timestamp = self._meta_base_key + ':' + batch_id + ':ts'  # ts for timestamp
        bk_values = self._meta_base_key + ':' + batch_id + ':vs'  # vs for values
        return bk_expired, bk_timestamp, bk_values

    def _get_readds(self, max_age_secs, listener_restarted=False):
        '''Checks for expired batches, deletes, and returns values'''
        readd_values = []
        _, timestamp_match, _ = self._get_batch_keys('*')
        for timestamp_key in self._client.keys(timestamp_match):
            error_readd = False
            expire_readd = False
            batch_id = timestamp_key[len(self._meta_base_key) + 1:-3]
            bk_expired, bk_timestamp, bk_values = self._get_batch_keys(str(batch_id))
            # TODO: Do more than catch
            # TypeError: int() argument must be a string,
            # a bytes-like object or a number, no      t 'NoneType'
            # after es 906f58f3-153c-4430-9bc8-6331c7e6dc67 analysis_step_run 19551104
            # before doc 2e24f230-f301-4527-821c-625221e5c826 20
            try:
                timestamp = int(self._client.get(timestamp_key))
            except TypeError as ecp:
                print(
                    'MAKE A WARNING: '
                    'redis_queues meta _get_readds TypeError READD',
                    timestamp_key,
                )
                self._exceptions = [repr(ecp)]
                error_readd = True
            if not error_readd:
                timestamp = timestamp / 1000000
                age = time.time() - timestamp
                if age >= max_age_secs:
                    expire_readd = True
                    print(
                        'MAKE A WARNING: '
                        'redis_queues meta _get_readds EXPIRE READD',
                        timestamp_key,
                    )
            if not error_readd or not expire_readd or listener_restarted:
                batch_uuids = self._client.lrange(bk_values, 0, -1)
                if not batch_uuids:
                    print(
                        'MAKE A WARNING: '
                        'redis_queues meta _get_readds NO UUIDS CANNOT READD',
                        timestamp_key,
                    )
                else:
                    readd_values.extend(batch_uuids)
                    self._client.delete(bk_expired)
                    self._client.delete(bk_timestamp)
                    self._client.delete(bk_values)
        return readd_values

    def is_server_running(self):
        '''Return boolean for server running flag'''
        if self._client.exists(self._is_running_key):
            str_val = self._client.get(self._is_running_key)
            if str_val == 'true':
                return True
        return False

    def set_to_not_running(self):
        '''
        Set server running flag off

        - Tells the workers to stop
        '''
        self._client.set(self._is_running_key, 'false')

    def purge_meta(self):
        '''Remove all keys with queue_name:meta'''
        for key in self._client.keys(self._meta_base_key + '*'):
            self._client.delete(key)

    def add_batch(self, values):
        '''
        Values removed from queue are stored in batch as a list for values
        until consumed and add_finished is called with the batch_id.

        -A timestamp is added for expiration.
        -Expiration handled in is_finished.
        '''
        batch_id = str(self._base_id)
        self._base_id += 1
        bk_expired, bk_timestamp, bk_values = self._get_batch_keys(str(batch_id))
        self._client.set(bk_timestamp, int(time.time() * 1000000))
        self._client.set(bk_expired, 0)
        self._client.lpush(bk_values, *values)
        return batch_id

    def add_finished(self, batch_id, successes, errors):
        '''
        Update batch after values are consumed

        - If any checks fail the batch is not removed, so it will
        expire. Expired batches will be readded in is_finished and
        eventaully reindexed.
        '''
        bk_expired, bk_timestamp, bk_values = self._get_batch_keys(str(batch_id))
        len_batch_uuids = self._client.llen(bk_values)
        if len_batch_uuids is None:
            print(
                'MAKE A WARNING: add_finished.  '
                'If id was corrupted the batch should expire and rerun.'
            )
        else:
            # TODO: Do more than catch
            # TypeError: int() argument must be a string,
            # a bytes-like object or a number, not 'NoneType'
            # after es a0445f86-ba86-4c44-8637-dd6ab5bd7594 analysis_step_run 19551104
            # before crash
            expired = 0
            try:
                expired = int(self._client.get(bk_expired))
            except TypeError as ecp:
                print('MAKE A WARNING: redis_queues meta add_finished', bk_expired, repr(ecp))
                self._exceptions = [repr(ecp)]
            did_check_out = (len(errors) + successes) == len_batch_uuids
            if expired != 0:
                print(
                    'MAKE A WARNING: add_finished.  '
                    'Batch expired and then finished.'
                )
            elif not did_check_out:
                print(
                    'MAKE A WARNING: add_finished.  '
                    'Batch counts were off.  Let it expire.'
                )
            else:
                # The one location to update success counter
                self._client.incrby(self._successes_cnt_key, successes)
                if errors:
                    self._add_errors(batch_id, errors)
                self._client.delete(bk_expired)
                self._client.delete(bk_timestamp)
                self._client.delete(bk_values)

    def get_run_args(self):
        '''Return run args needed for workers'''
        run_args = self._client.hgetall(self._run_args_key)
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
        self._client.hmset(self._run_args_key, set_args)

    def get_errors(self):
        '''Get all errors from queue that were sent in add_finished'''
        errors = []
        errors_cnt = int(self._client.get(self._errors_cnt_key))
        for error_key in self._client.keys(self._errors_key + ':*'):
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
        '''Check if queue has been consumed'''
        readd_values = []
        did_finish = False
        if max_age_secs:
            readd_values = self._get_readds(
                max_age_secs,
                listener_restarted=listener_restarted,
            )
            for index, item in enumerate(self._exceptions):
                print('MAKE A WARNING: redis_queues meta is_finished _get_readds excptions:', index, item)
        if not readd_values:
            errors_cnt = int(self._client.get(self._errors_cnt_key))
            successes_cnt = int(self._client.get(self._successes_cnt_key))
            uuids_added = int(self._client.get(self._added_cnt_key))
            uuids_handled = successes_cnt + errors_cnt
            did_finish = uuids_handled == uuids_added
        return readd_values, did_finish

    def values_added(self, len_values):
        '''
        Update successfully added values

        - Also used to remove readded values so could be negative
        '''
        self._client.incrby(self._added_cnt_key, len_values)

    def is_useable(self):
        '''
        Check the consistency of the meta data values

        * Useful when queue crashes and needs a restart
        '''
        # self._run_args_key = self._meta_base_key + ':ra'  # ra for run args
        added_cnt = 0
        errors_cnt = 0
        errors_len = 0
        successes_cnt = 0
        queue_len = 0
        if self._client.exists(self._added_cnt_key):
            added_cnt = int(self._client.get(self._added_cnt_key))
        else:
            self._client.set(self._added_cnt_key, added_cnt)
        if self._client.exists(self._errors_cnt_key):
            errors_cnt = int(self._client.get(self._errors_cnt_key))
        else:
            self._client.set(self._errors_cnt_key, errors_cnt)
        if self._client.exists(self._errors_key):
            errors_len = self._client.llen(self._errors_key)
        if self._client.exists(self._run_args_key):
            try:
                self.get_run_args()
            except KeyError:
                return False
        if self._client.exists(self._successes_cnt_key):
            successes_cnt = int(self._client.get(self._successes_cnt_key))
        else:
            self._client.set(self._successes_cnt_key, successes_cnt)
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
        return self._client.exists(self.queue_name)

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
