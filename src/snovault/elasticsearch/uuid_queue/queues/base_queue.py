"""
Base in memory queue stored as list

- Base class for all other queues
"""
import time


BASE_QUEUE_TYPE = 'BASE_QUEUE_TYPE'


# pylint: disable=too-few-public-methods
class BaseQueueClient(object):
    '''
    Place holder queue client

    Redis and AWS queues have a client.  This exists to keep the queues
    consistent.

    * Queue client must expose the 'get_queue' function to the adapter.
    '''
    def __init__(self, queue_options):
        pass

    # pylint: disable=no-self-use, unused-argument
    def get_queue(self, queue_name, queue_type, is_worker=False):
        '''Create a Queue'''
        if queue_type == BASE_QUEUE_TYPE:
            return BaseQueue(queue_name)
        else:
            raise ValueError('Queue %s is not available' % queue_type)


class BaseQueueMeta(object):
    '''
    Basic meta data storage for queue

    The current pattern is QueueAdapter will initialize with a
    meta object class but not access it directly.  Updating meta data
    is handled through the adapter.
    '''

    def __init__(self, queue_name):
        self.queue_name = queue_name
        self._base_id = int(time.time() * 1000000)
        self._errors = []
        self._errors_count = 0
        self._uuid_count = 0
        self._added_count = 0
        self._success_count = 0
        self._worker_conns = {}
        self._worker_results = {}

    def get_server_restarts(self):  # pylint: disable=no-self-use
        '''
        Number of times server queue meta has been initialized
        - Should return 1 for the base queue since to persistance
        '''
        return 1

    # Errors
    def add_errors(self, errors):
        """Add errors after worker finishes"""
        errors_added = 0
        for error in errors:
            errors_added += 1
            self._errors.append(error)
        return errors_added

    def has_errors(self):
        """Do errors exist during this indexing session"""
        if self._errors:
            return True
        return False

    def pop_errors(self):
        """Get and remove all errors"""
        errors = self._errors
        self._errors = []
        return errors

    # Workers
    @staticmethod
    def _get_blank_worker():
        return {
            'uuid_cnt': 0,
            'get_cnt': 0,
        }

    def add_worker_conn(self, worker_id):
        """Add worker connection"""
        self._worker_results[worker_id] = []
        self._worker_conns[worker_id] = self._get_blank_worker()

    def get_worker_conns(self):
        """Return worker connection dicts"""
        return self._worker_conns

    def get_worker_conn_count(self):
        """Return number of worker conns"""
        return len(self._worker_conns)

    def update_worker_conn(self, worker_id, uuid_cnt, get_cnt):
        """Set worker conn info"""
        worker_conn = self._worker_conns[worker_id]
        worker_conn['uuid_cnt'] = uuid_cnt
        worker_conn['get_cnt'] = get_cnt

    def save_work_results(self, worker_id, results):
        """Save work results"""
        self._worker_results[worker_id].append(results)

    # Uuids
    def has_uuids(self, errs_cnt=0):  # pylint: disable=unused-argument
        """Boolean for if uuid has uuids"""
        return self._uuid_count > 0

    def update_uuid_count(self, len_values):
        '''
        Update successfully loaded and got values
        '''
        if len_values > 0:
            self._added_count += len_values
        self._uuid_count += len_values

    def update_success_count(self, len_values):
        '''Update successfully indexed uuids'''
        self._success_count += len_values

    def update_errors_count(self, len_values):
        '''Update errors for indexed uuids'''
        self._errors_count += len_values


class BaseQueue(object):
    '''
    Basic in memory queue built from Python List

    This Classes defines the publicly allowed methods for any child queue.
    Function implementation is made to look like AWS and Redis queues.
    '''
    max_value_size = 262144  # Arbitraitly set to AWS SQS Limit
    queue_type = BASE_QUEUE_TYPE

    def __init__(self, queue_name):
        self._uuids = []
        self._qmeta = BaseQueueMeta(queue_name)
        self.queue_name = self._qmeta.queue_name

    # Persistant Data
    def get_server_restarts(self):
        '''Pass to queue meta get_server_restarts'''
        return self._qmeta.get_server_restarts()

    # Errors
    def add_errors(self, worker_id, result_errors):
        """Add errors from results to queue meta through queue"""
        batch_errors = []
        for batch_error in result_errors:
            batch_error['worker_id'] = worker_id
            batch_errors.append(batch_error)
        return self._qmeta.add_errors(batch_errors)

    def has_errors(self):
        """Return Queue Meta has errors"""
        return self._qmeta.has_errors()

    def pop_errors(self):
        """Get and remove queue meta"""
        return self._qmeta.pop_errors()

    # Worker
    def add_worker_conn(self, worker_id):
        """Add worker to queue meta"""
        self._qmeta.add_worker_conn(worker_id)

    def get_worker_conns(self):
        """Get queue meta workers"""
        return self._qmeta.get_worker_conns()

    def get_worker_conn_count(self):
        """Get queue meta workers count"""
        return self._qmeta.get_worker_conn_count()

    def update_worker_conn(self, worker_id, uuid_cnt, get_cnt):
        """Update worker connection in queue meta"""
        self._qmeta.update_worker_conn(worker_id, uuid_cnt, get_cnt)

    def save_work_results(self, worker_id, results):
        """Save work results through queue meta"""
        self._qmeta.save_work_results(worker_id, results)

    # Uuids
    def has_uuids(self, errs_cnt=0):
        """Return Queue Meta has uuids"""
        return self._qmeta.has_uuids(errs_cnt=errs_cnt)

    def update_uuid_count(self, len_values):
        """Return Queue Meta update uuid count"""
        return self._qmeta.update_uuid_count(len_values)

    def update_success_count(self, len_values):
        """Return Queue Meta success uuid count"""
        return self._qmeta.update_success_count(len_values)

    def update_errors_count(self, len_values):
        """Return Queue Meta errors uuid count"""
        return self._qmeta.update_errors_count(len_values)

    def _get_uuid(self):
        if self._uuids:
            return self._uuids.pop()
        return None

    def get_uuids(self, cnt):
        '''The only way to get uuids from queue'''
        uuids = []
        if cnt <= -1:
            cnt = len(self._uuids)
        if cnt:
            while cnt > 0:
                uuid = self._get_uuid()
                if not uuid:
                    break
                uuids.append(uuid)
                cnt -= 1
        return uuids

    def _load_uuid(self, uuid):
        '''
        Load uuid into queue
        - Uuid ordering impacts indexing time, list is reversed due to
        pop being used in get uuid
        '''
        try:
            self._uuids.insert(0, uuid)
            return True
        except Exception as ecp:  # pylint: disable=broad-except
            # ToDo: Make log
            print('Base queue failed to load uuids: %s' % repr(ecp))
            return False

    def load_uuids(self, uuids):
        '''
        The only way to load uuids in queue
        '''
        bytes_added = 0
        failed_uuids = []
        for uuid in uuids:
            if self._load_uuid(uuid):
                bytes_added += len(uuid)
            else:
                failed_uuids.append(uuid)
        return bytes_added, failed_uuids

    # Run
    def update_finished(self, given_worker_id, given_results):
        """Update queue and queue meta for finished work"""
        msg = 'Worker id(%s) DNE.' % given_worker_id
        worker_conns = self.get_worker_conns()
        for worker_id, worker_conn in worker_conns.items():
            if worker_id == given_worker_id:
                errors_added = self.add_errors(worker_id, given_results['errors'])
                uuid_cnt = int(worker_conn['uuid_cnt'])
                uuid_cnt -= given_results['successes']
                uuid_cnt -= errors_added
                self.update_worker_conn(
                    worker_id,
                    uuid_cnt,
                    worker_conn['get_cnt'],
                )
                self.save_work_results(worker_id, given_results)
                if uuid_cnt != 0:
                    msg = 'Queue Server cannot close worker (%s)' % worker_id
                else:
                    msg = 'Okay'
                break
        return msg

    # Run
    def close_indexing(self):
        '''Close indexing sessions'''
        pass
