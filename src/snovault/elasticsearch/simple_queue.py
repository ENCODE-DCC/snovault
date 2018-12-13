'''
Simple uuid queue for indexing process
- Can be overridden by another script, class, module as long as it exposes
the proper functions used in the indexer classes.
'''
import time


class SimpleUuidServer(object):    #pylint: disable=too-many-instance-attributes
    '''Simple uuid queue as a list'''
    def __init__(self, queue_options):
        self._queue_options = queue_options
        self._start_us = int(time.time() * 1000000)
        self.queue_id = str(self._start_us)
        self._uuids = []
        self._uuid_count = 0
        self._worker_conns = {}
        self._worker_results = {}
        self._errors = []

    # Errors
    def add_errors(self, worker_id, errors):
        """Add errors after worker finishes"""
        errors_added = 0
        for batch_error in errors:
            batch_error['worker_id'] = worker_id
            errors_added += 1
            self._errors.append(batch_error)
        return errors_added

    def _has_errors(self):
        """Check if queue has errors"""
        if self._errors:
            return True
        return False

    def pop_errors(self):
        """Get and remove errors, they are expected to be handled after this"""
        errors = self._errors.copy()
        self._errors = []
        return errors

    # Worker
    @staticmethod
    def _get_blank_worker():
        return {
            'uuid_cnt': 0,
            'get_cnt': 0,
        }

    def _add_worker_conn(self, worker_id):
        """Add worker connection"""
        self._worker_results[worker_id] = []
        self._worker_conns[worker_id] = self._get_blank_worker()

    def get_worker_conns(self):
        """Return worker connection dicts"""
        return self._worker_conns

    def get_worker_conn_count(self):
        """Return number of worker conns"""
        return len(self._worker_conns)

    def _get_worker_id(self):
        time_id = self._start_us + self.get_worker_conn_count() + 1
        return str(time_id)

    def _get_worker_queue(self):
        """Queue to create workers with"""
        return self

    def get_worker(self):
        """Return the associated worker"""
        worker_id = self._get_worker_id()
        self._add_worker_conn(worker_id)
        worker_queue = self._get_worker_queue()
        return SimpleUuidWorker(
            self._queue_options,
            worker_id,
            worker_queue,
        )

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
        '''Are there uuids in the queue'''
        if self._uuids:
            return True
        return False

    def _get_uuid(self):
        if self._uuids:
            return self._uuids.pop()
        return None

    def get_uuids(self, cnt, get_all=False):
        '''The only way to get uuids from queue'''
        uuids = []
        if cnt == -1 or get_all:
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
        - Uuid ordering impacts indexing time,
        list is reversed due to pop being used in
        get uuid
        '''
        self._uuids.insert(0, uuid)

    def load_uuids(self, uuids):
        '''
        The only way to load uuids in queue
        - Uuids are to be loaded once per indexing session
        '''
        if self.is_indexing():
            return None
        for uuid in uuids:
            self._load_uuid(uuid)
        return len(self._uuids)

    # Run
    def is_indexing(self, errs_cnt=0):  # pylint: disable=unused-argument
        '''Is an indexing process currently running'''
        if self.has_uuids(errs_cnt=errs_cnt) or self._has_errors():
            return True
        for worker_conn in self._worker_conns.values():
            if worker_conn['uuid_cnt']:
                return True
        return False

    def update_finished(self, given_worker_id, given_results):
        '''The way a worker finishes a batch of uuids'''
        msg = 'Worker id(%s) DNE.' % given_worker_id
        worker_conns = self.get_worker_conns()
        for worker_id, worker_conn in worker_conns.items():
            if worker_id == given_worker_id:
                errors_added = self.add_errors(worker_id, given_results['errors'])
                uuid_cnt = worker_conn['uuid_cnt'] - given_results['successes'] - errors_added
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

    def close_indexing(self):
        '''Close indexing sessions'''
        pass


class SimpleUuidWorker(object):  #pylint: disable=too-many-instance-attributes
    '''Basic uuid worker to get uuids for indexing'''
    def __init__(self, queue_options, worker_id, queue):
        self.queue_options = queue_options
        self.worker_id = worker_id
        self._queue = queue
        self.is_running = False
        # Worker conn info
        self.get_cnt = 0
        self.uuid_cnt = 0
        # MPIndexer
        self.processes = self.queue_options['processes']
        self.chunk_size = self.queue_options['chunk_size']

    # Uuids
    def get_uuids(self, get_all=False):
        '''Get all or some of uuids'''
        self.get_cnt += 1
        uuids = []
        if self.uuid_cnt == 0:
            uuids = self._queue.get_uuids(self.queue_options['batch_size'], get_all=get_all)
            self.uuid_cnt = len(uuids)
        self._queue.update_worker_conn(
            self.worker_id,
            self.uuid_cnt,
            self.get_cnt,
        )
        return uuids

    # Run
    def update_finished(self, results):
        '''Update server with batch results'''
        msg = self._queue.update_finished(self.worker_id, results)
        if msg == 'Okay':
            self.uuid_cnt = 0
            return None
        return 'Update finished could not reset worker: {}'.format(msg)
