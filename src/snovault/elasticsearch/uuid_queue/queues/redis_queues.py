'''Redis Queues'''
import time

from redis import StrictRedis  # pylint: disable=import-error

from .base_queue import (  # pylint: disable=relative-beyond-top-level
    BaseQueue,
    BaseQueueMeta,
)


REDIS_LIST = 'REDIS_LIST'
REDIS_LIST_PIPE = 'REDIS_LIST_PIPE'
REDIS_SET = 'REDIS_SET'
REDIS_SET_PIPE = 'REDIS_SET_PIPE'
REDIS_SET_PIPE_EXEC = 'REDIS_SET_PIPE_EXEC'
PD_RESTARTS = 'pd:srvrs'

# TODo: has_uuids failed becuase set_args was never called on init.  Tests did not catch this
# TODo: new/updated so check tests: _get_worker_ids, get_worker_conn_count


# pylint: disable=too-few-public-methods
class RedisClient(StrictRedis):
    '''One and only Redis Client class'''

    def __init__(self, queue_options):
        super().__init__(
            encoding="utf-8",
            decode_responses=True,
            db=queue_options.get('db', 0),
            host=queue_options['host'],
            port=queue_options['port'],
            socket_timeout=5,
        )

    def get_queue(self, queue_name, queue_type, is_worker=False):
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
        return queue_class(queue_name, self, is_worker=is_worker)


class RedisQueueMeta(BaseQueueMeta):
    # pylint: disable=too-many-instance-attributes
    '''
    Redis meta wrapper

    - Server and client(workers) use meta store in redis
    '''
    def __init__(self, queue_name, client, is_worker=False):
        self._base_id = int(time.time() * 1000000)
        self._client = client
        if not is_worker:
            restarts = self.get_server_restarts()
            self.queue_name = queue_name + str(restarts)
            self._setup_redis_keys()
            self.set_args(kill_workers=True)
            self._init_persistant_data()
        else:
            self.queue_name = queue_name
            self._setup_redis_keys()

    # Persistant Server Data
    def _init_persistant_data(self):
        # persistant data : server restarts
        self._client.incrby(PD_RESTARTS, 1)

    def get_server_restarts(self):
        '''
        Number of times server queue meta has been initialized
        - Should be the number of times the server has started
        '''
        cnt = self._client.get(PD_RESTARTS)
        if not cnt:
            self._client.set(PD_RESTARTS, 0)
            cnt = 0
        else:
            cnt = int(self._client.get(PD_RESTARTS))
        return cnt

    # Errors
    def add_errors(self, errors):
        """
        Add errors per batch after consumed
        - error uuids are added to error list
        - error hashs(dicts) are stored as individual items per uuid
        """
        errors_added = 0
        for error in errors:
            err_uuid = error['uuid']
            error_key = self._key_uuid_errors + ':' + str(err_uuid)
            self._client.hmset(error_key, error)
            self._client.lpush(self._key_errors, err_uuid)
            errors_added += 1
        return errors_added

    def has_errors(self):
        """Do errors exist during this indexing session"""
        errors_count = int(self._client.get(self._key_errorscount))
        errors_list_count = self._client.llen(self._key_errors)
        if errors_count or errors_list_count:
            return True
        return False

    def pop_errors(self):
        """
        Get and remove all errors from uuids list including count
        - Error hashes are not removed from db here
        """
        errors = []
        err_uuids = self._client.lrange(self._key_errors, 0, -1)
        self._client.delete(self._key_errors)
        for err_uuid in err_uuids:
            uuid_error_key = self._key_uuid_errors + ':' + err_uuid
            error_hash = self._client.hgetall(uuid_error_key)
            errors.append(error_hash)
            self._client.delete(uuid_error_key)
        self._client.set(self._key_errorscount, 0)
        return errors

    # Workers
    def add_worker_conn(self, worker_id):
        """Add worker connection"""
        self._client.lpush(self._key_workers, worker_id)
        worker_conn_key = self._key_worker_conn + ':' + worker_id
        self._client.hmset(worker_conn_key, BaseQueueMeta._get_blank_worker())

    def _get_worker_conn(self, worker_id):
        worker_conn_key = self._key_worker_conn + ':' + worker_id
        return self._client.hgetall(worker_conn_key)

    def _get_worker_ids(self):
        """Get list of worker ids"""
        return self._client.lrange(self._key_workers, 0, -1)

    def get_worker_conns(self):
        """Return worker connection dicts"""
        worker_conns = {}
        worker_ids = self._get_worker_ids()
        for worker_id in worker_ids:
            worker_conns[worker_id] = self._get_worker_conn(worker_id)
        return worker_conns

    def get_worker_conn_count(self):
        """Return number of worker conns"""
        return self._client.llen(self._key_workers)

    def update_worker_conn(self, worker_id, uuid_cnt, get_cnt):
        """Set worker conn info"""
        worker_conn_key = self._key_worker_conn + ':' + worker_id
        worker_conn = self._get_worker_conn(worker_id)
        if worker_conn:
            worker_conn['uuid_cnt'] = uuid_cnt
            worker_conn['get_cnt'] = get_cnt
            self._client.hmset(worker_conn_key, worker_conn)

    def save_work_results(self, worker_id, results):
        """Save work results"""
        worker_conn = self._get_worker_conn(worker_id)
        results['errors'] = len(results['errors'])
        if worker_conn:
            results_count = int(worker_conn.get('results_count', 0))
            results_count += 1
            worker_res_key = '{}:{}:{}'.format(
                self._key_worker_results,
                worker_id,
                results_count,
            )
            self._client.hmset(worker_res_key, results)
            worker_conn['results_count'] = results_count
            worker_conn_key = self._key_worker_conn + ':' + worker_id
            self._client.hmset(worker_conn_key, worker_conn)

    # Uuids
    def get_uuid_count(self):
        """
        Get remaining count of uuids
        - Does not include working uuids
        """
        try:
            uuid_count = int(self._client.get(self._key_uuidcount))
        except TypeError:
            return 0
        return uuid_count

    def has_uuids(self, errs_cnt=0):
        """Boolean for if uuid has uuids"""
        added_cnt = int(self._client.get(self._key_addedcount))
        success_cnt = int(self._client.get(self._key_successescount))
        errors_cnt = errs_cnt + int(self._client.get(self._key_errorscount))
        cnt = added_cnt - (success_cnt + errors_cnt)
        return cnt > 0

    def update_uuid_count(self, len_values):
        '''
        Update uuid count, total added and current left
        - Added when loading uuids
        - Subtracted when getting uuids
        '''
        if len_values > 0:
            self._client.incrby(self._key_addedcount, len_values)
        self._client.incrby(self._key_uuidcount, len_values)

    def update_success_count(self, len_values):
        '''Update successfully indexed uuids'''
        self._client.incrby(self._key_successescount, len_values)

    def update_errors_count(self, len_values):
        '''Update errors for indexed uuids'''
        self._client.incrby(self._key_errorscount, len_values)

    # Run Args
    def _setup_redis_keys(self):
        """
        Create redis keys and base keys

        mb(metabase)-Base key for storing values
        ra(runargs, hash?)-Queue needs to store run args for remote workers
        cu(uuids count, int)-Current number of uuids to index
        ca(added count, int)-Total Number of uuids in indexing session
        cs(successes count, int)-Number of uuids successfully processed
        ue(errors for uuid, hash)-Errors per uuid as a hash.  Set on indexing batch
        ce(errors count, int)-Number of all errors
        errors-list of uuids that had errors
        wi(worker ids, list)-List of worker connection ids
        wk(worker connection, Base)-Base key for worker connection hashes with worker id
        wr(worker results, base for hash)-Base key for worker results.  Conns can have
        mulitple results
        Ex)
            if self.queue_name = 'testqueuename'
            redis keys will look like
            self._key_addedcount = testqueuename:mb:ca
        """
        # pylint: disable=attribute-defined-outside-init
        self._key_metabase = self.queue_name + ':mb'
        self._key_runargs = self._key_metabase + ':ra'
        # Queue run info
        self._key_uuidcount = self._key_metabase + ':cu'
        self._key_addedcount = self._key_metabase + ':ca'
        self._key_errors = self._key_metabase + ':errors'
        self._key_uuid_errors = self._key_metabase + ':ue'
        self._key_errorscount = self._key_metabase + ':ce'
        self._key_successescount = self._key_metabase + ':cs'
        # Worker Connections
        self._key_workers = self._key_metabase + ':wi'
        self._key_worker_conn = self._key_metabase + ':wk'
        self._key_worker_results = self._key_metabase + ':wr'

    def set_args(self, kill_workers=False):
        """Initialize indexing run args"""
        self._client.set(self._key_addedcount, 0)
        self._client.set(self._key_uuidcount, 0)
        self._client.delete(self._key_errors)
        self._client.set(self._key_errorscount, 0)
        self._client.set(self._key_successescount, 0)
        if kill_workers:
            # Worker Connections
            self._client.delete(self._key_workers)

    def get_run_args(self):
        '''Return run args needed for workers'''
        run_args = self._client.hgetall(self._key_runargs)
        if run_args:
            run_args['batch_by'] = int(run_args['batch_by'])
            run_args['restart'] = False if run_args['restart'] == 'false' else True
            run_args['uuid_len'] = int(run_args['uuid_len'])
            run_args['xmin'] = int(run_args['xmin'])
        return run_args

    def set_run_args(self, run_args):
        '''
        Add run args needed for workers
        * Only defined values are added
        * Type checks when getting run args
        '''
        run_args = run_args.copy()
        if 'restart' in run_args:
            if run_args['restart'] is True:
                run_args['restart'] = 'true'
            else:
                run_args['restart'] = 'false'
        set_run_args = {
            'batch_by': run_args['batch_by'],
            'restart': run_args['restart'],
            'snapshot_id': run_args['snapshot_id'],
            'uuid_len': run_args['uuid_len'],
            'xmin': run_args['xmin'],
        }
        self._client.hmset(self._key_runargs, set_run_args)


class RedisQueue(BaseQueue):
    '''
    Base non-pipe redis queue for all redis queue types, includeing pipes
    - Cannot be used directly
    '''
    add_str = None
    get_str = None
    len_str = None
    max_value_size = 262144  # Arbitraitly set to AWS SQS Limit
    queue_type = None

    def __init__(self, queue_name, client, is_worker=False):
        self._client = client
        self._qmeta = RedisQueueMeta(
            queue_name,
            self._client,
            is_worker=is_worker
        )
        self.queue_name = self._qmeta.queue_name

    def _call_func(self, func_str, value=None):
        """
        Redis Connection Error wrapper for all redis client calls
        for RedisQueue but not Meta Data
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

    # Errors - See base class
    # Worker - See base class
    # Uuids
    def _get_uuid(self):
        ret_val = self._call_func(self.get_str)
        return ret_val

    def _load_uuid(self, uuid):
        if self._call_func(self.add_str, uuid):
            return True
        return False

    # Run
    def close_indexing(self):
        '''Close indexing sessions'''
        self._qmeta.set_args()


class RedisPipeQueue(RedisQueue):
    '''
    Base pipe redis queue for all redis queue types

    - Pipe allows many queries to be sent at once
    - Cannot be used directly
    - Should override all methods in BaseQueue
    '''

    @staticmethod
    def _call_pipe(pipe):
        try:
            # At the time of writting pipe returns a list of length
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
        '''Add values in a group'''
        pipe, pipe_func = self._get_pipe(self.add_str)
        failed = []
        bytes_added = 0
        call_cnt = 0
        for value in values:
            if value is not None:
                value_len = len(value)
            pipe_func(self.queue_name, value)
            bytes_added += value_len
            call_cnt += 1
        ret_val = self._call_pipe(pipe)
        #   ret_val is a list of the redis queue
        # count after insertion of pipe item
        if ret_val is False:
            ret_val = []
            failed = values
            bytes_added = 0
        else:
            diff = call_cnt - len(ret_val)
            bytes_added -= value_len * diff
            failed = [str(num) for num in range(diff)]
        return failed, bytes_added, call_cnt

    # Get Values
    def get_values(self, get_count):
        '''Get values in a group'''
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
    len_str = 'llen'


class RedisListPipeQueue(RedisPipeQueue):
    '''Pipe List queue'''
    queue_type = REDIS_LIST_PIPE
    add_str = 'lpush'
    get_str = 'lpop'
    len_str = 'llen'


class RedisSetQueue(RedisQueue):
    '''Set queue'''
    queue_type = REDIS_SET
    add_str = 'sadd'
    get_str = 'spop'
    len_str = 'scard'


class RedisSetPipeQueue(RedisPipeQueue):
    '''Pipe queue'''
    queue_type = REDIS_SET_PIPE
    add_str = 'sadd'
    get_str = 'spop'
    len_str = 'scard'


class RedisSetPipeExecQueue(RedisSetPipeQueue):
    '''
    SetPipe with exec functionality

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
        # pylint: disable=consider-using-ternary
        args = (get_count is not None) and [get_count] or []
        ret_val = self._client.execute_command(self.get_str, self.queue_name, *args)
        call_cnt += 1
        if ret_val:
            for val in ret_val:
                if val:
                    bytes_got += len(val)
                    values.append(val)
        return values, call_cnt
