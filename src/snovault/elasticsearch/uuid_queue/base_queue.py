"""
Base in memory queue stored as list

- Base class for all other queues
"""
import time


BASE_IN_MEMORY = 'BASE_IN_MEMORY'


# pylint: disable=too-few-public-methods
class BaseClient(object):
    '''
    Place holder queue client

    Redis and AWS queues have a client.  This exists to keep the queues
    consistent.

    * Queue client must expose the 'get_queue' function to the adapter.
    '''
    def __init__(self, args):
        pass

    # pylint: disable=no-self-use, unused-argument
    def get_queue(self, queue_name, queue_type, args):
        '''Create a Queue'''
        if queue_type == BASE_IN_MEMORY:
            queue_class = UuidBaseQueue
        else:
            raise ValueError('Queue %s is not available' % queue_type)
        return queue_class(queue_name)


class UuidBaseQueueMeta(object):
    '''
    Basic meta data storage for queue

    The current pattern is UuidQueues will initialize with a
    meta object class but not access it directly.  Updating meta data
    is handled through the adapter.
    '''

    def __init__(self):
        self._base_id = int(time.time() * 1000000)
        self._errors = {'meta': {'total': 0}}
        self._got_batches = {}
        self._uuids_added = 0
        self._successes = 0

    def _add_errors(self, batch_id, errors):
        '''Add errors as batch after consumed'''
        self._errors[batch_id] = errors
        self._errors['meta']['total'] += len(errors)

    def purge_meta(self):
        '''Remove all meta data'''
        self._errors = {'meta': {'total': 0}}
        self._got_batches = {}
        self._uuids_added = 0
        self._successes = 0

    def add_batch(self, values):
        '''Add values as batch after getting from queue'''
        batch_id = str(self._base_id)
        self._base_id += 1
        self._got_batches[batch_id] = {
            'expired': 0,
            'timestamp': int(time.time() * 1000000),
            'uuids': values,
        }
        return batch_id


    def add_finished(self, batch_id, successes, errors):
        '''Update with outcome consumed uuids'''
        batch = self._got_batches.get(batch_id, None)
        if batch is None:
            print(
                'MAKE A WARNING: add_finished.  '
                'If id was corrupted the batch should expire and rerun.'
            )
        else:
            did_check_out = (len(errors) + successes) == len(batch['uuids'])
            if batch['expired']:
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
                self._successes += successes
                if errors:
                    self._add_errors(batch_id, errors)
                del self._got_batches[batch_id]

    def get_errors(self):
        '''Get all errors from queue that were sent in add_finished'''
        errors = []
        for key, batch_errors in self._errors.items():
            if key != 'meta':
                errors.extend(batch_errors)
        return errors

    def is_finished(
            self,
            max_age_secs=5001,
            listener_restarted=False,
        ):
        '''Check if queue has been consumed'''
        readd_values = []
        did_finish = False
        if max_age_secs:
            for batch in self._got_batches.values():
                age = time.time() - batch['timestamp']
                if age >= max_age_secs:
                    readd_values.extend(batch['uuids'])
        if not readd_values:
            uuids_handled = self._successes + self._errors['meta']['total']
            print(uuids_handled, self._uuids_added)
            did_finish = uuids_handled == self._uuids_added
        return readd_values, did_finish

    def values_added(self, len_values):
        '''
        Update successfully added values

        - Also used to remove readded uuids
        '''
        print(len_values)
        self._uuids_added += len_values


class UuidBaseQueue(object):
    '''
    Basic in memory queue built from Python List

    This Classes defines the publicly allowed methods for any child queue.
    Function implementation is made to look like AWS and Redis queues.
    '''
    max_value_size = 262144  # Arbitraitly set to AWS SQS Limit
    queue_type = BASE_IN_MEMORY

    def __init__(self, queue_name):
        self._values = []
        self.queue_name = queue_name
        self.qmeta = UuidBaseQueueMeta()

    def _add_value(self, value):
        if value:
            self._values.append(value)
            return True
        return False

    def _get_value(self):
        value = None
        if self._values:
            value = self._values.pop()
        return value

    def add_values(self, values):
        '''Add values to queue'''
        failed = []
        bytes_added = 0
        call_cnt = 0
        for value in values:
            ret_value = self._add_value(value)
            if ret_value is False:
                failed.append(value)
            else:
                call_cnt += 1
                bytes_added += len(value)
        return failed, bytes_added, call_cnt

    def get_values(self, get_count):
        '''Get values from queue'''
        values = []
        call_cnt = 0
        value = self._get_value()
        while value:
            call_cnt += 1
            values.append(value)
            if len(values) >= get_count:
                break
            value = self._get_value()
        return values, call_cnt

    def purge(self):
        '''Clear Queue'''
        self._values = []
