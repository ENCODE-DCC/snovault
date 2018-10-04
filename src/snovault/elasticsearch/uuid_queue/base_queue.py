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
        self._errors = {}
        self._errors_count = 0
        self._got_batches = {}
        self._uuids_added = 0
        self._successes = 0

    def _add_errors(self, errors):
        '''Add errors as batch after consumed'''
        for error in errors:
            self._errors[error['uuid']] = error
            self._errors_count += 1

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

    def remove_batch(self, batch_id, successes, errors):
        '''Update with outcome consumed uuids'''
        batch = self._got_batches.get(batch_id, None)
        did_finish = False
        err_msg = None
        if batch is None:
            err_msg = 'Batch Id %s does not exist' % batch_id
        else:
            batch_uuids_len = len(batch['uuids'])
            errors_len = len(errors)
            did_check_out = ((errors_len + successes) == batch_uuids_len)
            if batch['expired']:
                err_msg = 'Batch Id %s expired' % batch_id
            elif not did_check_out:
                err_msg = (
                    'Batch Id {} errors {} plus success {} '
                    'does not equal batch uuids {}'.format(
                        batch_id,
                        errors_len,
                        successes,
                        batch_uuids_len,
                    )
                )
            else:
                self._successes += successes
                if errors:
                    self._add_errors(errors)
                del self._got_batches[batch_id]
                did_finish = True
        return did_finish, err_msg

    def get_errors(self):
        '''Get all errors from queue that were sent in remove_batch'''
        return self._errors

    # pylint: disable=unused-argument
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
                age = time.time() - (batch['timestamp'] / 1000000)
                if age >= max_age_secs:
                    batch['expired'] = 1
                    readd_values.extend(batch['uuids'])
        if not readd_values:
            uuids_handled = self._successes + self._errors_count
            did_finish = uuids_handled == self._uuids_added
        return readd_values, did_finish

    def purge_meta(self):
        '''Remove all meta data'''
        self._errors = {}
        self._got_batches = {}
        self._uuids_added = 0
        self._successes = 0

    def values_added(self, len_values):
        '''
        Update successfully added values

        - Also used to remove readded uuids
        '''
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
