"""
Test the uuid_queue module base_queue file
classes
    - BaseClient
    - UuidBaseQueueMeta
    - UuidBaseQueue
"""
import copy
import time
import unittest

from snovault.elasticsearch.uuid_queue.uuid_queue_adapter import UuidQueueTypes
from snovault.elasticsearch.uuid_queue.base_queue import (
    BaseClient,
    UuidBaseQueueMeta,
    UuidBaseQueue,
)

class TestBaseClient(unittest.TestCase):
    '''BaseClient is a placeholder for more complex Clients'''
    def test_get_queue(self):
        '''Test BaseClient get_queue'''
        args = {}
        client = BaseClient(args)
        queue = client.get_queue(
            'testbaseQ',
            UuidQueueTypes.BASE_IN_MEMORY,
            args,
        )
        self.assertIsInstance(queue, UuidBaseQueue)

    def test_get_queue_bad_type(self):
        '''Test BaseClient get_queue with wrong type'''
        args = {}
        client = BaseClient(args)
        try:
            _ = client.get_queue(
                'testbaseQ',
                'FakeType',
                args,
            )
        except ValueError:
            pass
        else:
            self.fail('ValueError expected')

class TestUuidBaseQueueMeta(unittest.TestCase):
    '''UuidBaseQueueMeta is the base class for storing queue meta data'''
    @classmethod
    def setUpClass(cls):
        cls.qmeta = UuidBaseQueueMeta()

    def setUp(self):
        self.qmeta.purge_meta()

    def test_init(self):
        '''Test UuidBaseQueueMeta init'''
        expected_keys = [
            '_add_errors',
            '_base_id',
            '_errors',
            '_errors_count',
            '_got_batches',
            '_successes',
            '_uuids_added',
            'add_batch',
            'add_finished',
            'get_errors',
            'is_finished',
            'purge_meta',
            'values_added'
        ]
        keys = [
            key
            for key in dir(self.qmeta)
            if key[0:2] != '__'
        ]
        keys.sort()
        expected_keys.sort()
        self.assertListEqual(keys, expected_keys)
        self.assertTrue(hasattr(self.qmeta, '_base_id'))
        self.assertIsInstance(getattr(self.qmeta, '_base_id'), int)
        self.assertTrue(hasattr(self.qmeta, '_errors'))
        self.assertIsInstance(getattr(self.qmeta, '_errors'), dict)
        self.assertTrue(hasattr(self.qmeta, '_errors_count'))
        self.assertIsInstance(getattr(self.qmeta, '_errors_count'), int)
        self.assertTrue(hasattr(self.qmeta, '_got_batches'))
        self.assertIsInstance(getattr(self.qmeta, '_got_batches'), dict)
        self.assertTrue(hasattr(self.qmeta, '_uuids_added'))
        self.assertIsInstance(getattr(self.qmeta, '_uuids_added'), int)
        self.assertTrue(hasattr(self.qmeta, '_successes'))
        self.assertIsInstance(getattr(self.qmeta, '_successes'), int)

    def test_add_errors(self):
        '''Test UuidBaseQueueMeta add_errors'''
        errors = [
            {'uuid': str(index), 'msg': 'error' + str(index)}
            for index in range(3)
        ]
        # pylint: disable=protected-access
        self.qmeta._add_errors(errors)
        for error in errors:
            for uuid, batch_error in self.qmeta._errors.items():
                if error['uuid'] == uuid:
                    self.assertDictEqual(error, batch_error)
        self.assertEqual(3, self.qmeta._errors_count)

    def test_add_batch(self):
        '''Test UuidBaseQueueMeta add_batch'''
        # pylint: disable=protected-access
        values = ['uuid1', 'uuid2', 'uuid3']
        expected_batch_keys = ['expired', 'timestamp', 'uuids']
        start_base = self.qmeta._base_id
        expected_batch_id = str(start_base)
        batch_id = self.qmeta.add_batch(values)
        self.assertEqual(expected_batch_id, batch_id)
        self.assertTrue(batch_id in self.qmeta._got_batches)
        batch_keys = list(self.qmeta._got_batches[batch_id])
        batch_keys.sort()
        self.assertListEqual(expected_batch_keys, batch_keys)
        self.assertEqual(self.qmeta._got_batches[batch_id]['expired'], 0)
        self.assertIsInstance(self.qmeta._got_batches[batch_id]['timestamp'], int)
        batch_uuids = self.qmeta._got_batches[batch_id]['uuids']
        batch_uuids.sort()
        self.assertListEqual(batch_uuids, values)
        self.assertEqual(start_base + 1, self.qmeta._base_id)

    def test_add_finished(self):
        '''Test UuidBaseQueueMeta add_finished'''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        errors = [
            {'uuid': uuid, 'msg': 'error' + uuid}
            for uuid in values[0:2]
        ]
        successes = len(values) - len(errors)
        did_finish, err_msg = self.qmeta.add_finished(batch_id, successes, errors)
        self.assertTrue(did_finish)
        self.assertIsNone(err_msg)
        # pylint: disable=protected-access
        self.assertEqual(successes, self.qmeta._successes)
        batch_errors = self.qmeta.get_errors()
        for error in errors:
            for uuid, batch_error in batch_errors.items():
                if error['uuid'] == uuid:
                    self.assertDictEqual(error, batch_error)
        self.assertFalse(batch_id in self.qmeta._got_batches)

    def test_add_finished_expired(self):
        '''
        Test UuidBaseQueueMeta add_finished with expired batch
        - This also tests that expired fails before checkout
        '''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        # pylint: disable=protected-access
        self.qmeta._got_batches[batch_id]['expired'] = 1
        successes = len(values)
        errors = [
            {'uuid': str(index), 'msg': 'error' + str(index)}
            for index in range(1)
        ]
        did_finish, err_msg = self.qmeta.add_finished(batch_id, successes, errors)
        self.assertFalse(did_finish)
        self.assertEqual(
            err_msg,
            'Batch Id %s expired' % batch_id
        )

    def test_add_finished_no_batch(self):
        '''Test UuidBaseQueueMeta add_finished with bad batch_id'''
        batch_id = 'bad-batch-id'
        successes = 10
        errors = [
            {'uuid': str(index), 'msg': 'error' + str(index)}
            for index in range(1)
        ]
        did_finish, err_msg = self.qmeta.add_finished(batch_id, successes, errors)
        self.assertFalse(did_finish)
        self.assertEqual(
            err_msg,
            'Batch Id %s does not exist' % batch_id
        )

    def test_add_finished_no_checkout(self):
        '''Test UuidBaseQueueMeta add_finished with bad checkout'''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        successes = len(values)
        errors = [
            {'uuid': str(index), 'msg': 'error' + str(index)}
            for index in range(1)
        ]
        did_finish, err_msg = self.qmeta.add_finished(batch_id, successes, errors)
        self.assertFalse(did_finish)
        self.assertEqual(
            err_msg,
            'Batch Id {} errors {} plus success {} '
            'does not equal batch uuids {}'.format(
                batch_id,
                len(errors),
                successes,
                len(values),
            )
        )

    def test_get_errors(self):
        '''Test UuidBaseQueueMeta get_errors'''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        errors = [
            {'uuid': str(index), 'msg': 'error' + str(index)}
            for index in range(2)
        ]
        successes = len(values) - len(errors)
        self.qmeta.add_finished(batch_id, successes, errors)
        batch_errors = self.qmeta.get_errors()
        print(batch_errors)
        self.assertEqual(len(batch_errors), len(errors))
        for error in errors:
            for uuid, batch_error in batch_errors.items():
                if error['uuid'] == uuid:
                    self.assertDictEqual(error, batch_error)

    def test_get_errors_none(self):
        '''Test UuidBaseQueueMeta get_errors when no errors'''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        errors = {}
        successes = len(values) - len(errors)
        self.qmeta.add_finished(batch_id, successes, errors)
        batch_errors = self.qmeta.get_errors()
        self.assertEqual(len(batch_errors), len(errors))
        self.assertDictEqual(errors, batch_errors)

    def test_is_finished(self):
        '''Test UuidBaseQueueMeta is_finished'''
        self.qmeta.values_added(3)
        # pylint: disable=protected-access
        self.qmeta._successes = 2
        self.qmeta._errors_count = 1
        readd_values, did_finish = self.qmeta.is_finished()
        self.assertTrue(did_finish)
        self.assertListEqual(readd_values, [])

    def test_is_finished_expired(self):
        '''Test UuidBaseQueueMeta is_finished with expired'''
        max_age_secs = 0.001
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        time.sleep(max_age_secs * 2)
        readd_values, did_finish = self.qmeta.is_finished(max_age_secs=max_age_secs)
        self.assertFalse(did_finish)
        readd_values.sort()
        self.assertListEqual(readd_values, values)
        # pylint: disable=protected-access
        self.assertEqual(self.qmeta._got_batches[batch_id]['expired'], 1)

    def test_is_finished_not_expired(self):
        '''Test UuidBaseQueueMeta is_finished with not expired'''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        self.qmeta.values_added(len(values))
        readd_values, did_finish = self.qmeta.is_finished()
        self.assertFalse(did_finish)
        readd_values.sort()
        self.assertListEqual(readd_values, [])
        # pylint: disable=protected-access
        self.assertEqual(self.qmeta._got_batches[batch_id]['expired'], 0)

    def test_purge_meta(self):
        '''Test UuidBaseQueueMeta purge_meta'''
        # pylint: disable=protected-access
        init_errors = {}
        init_got_batches = {}
        init_uuids_added = 0
        init_successes = 0
        batch_id = 'some-batch-id'
        errors = ['some-error-1', 'some-error-2']
        self.qmeta._errors = copy.deepcopy(init_errors)
        self.qmeta._errors_count = len(errors)
        self.qmeta._errors[batch_id] = errors
        self.qmeta._got_batches = copy.copy(init_got_batches)
        self.qmeta._got_batches[batch_id] = {'test': 'junk'}
        self.qmeta._uuids_added = 10
        self.qmeta._successes = 500
        self.qmeta.purge_meta()
        self.assertDictEqual(self.qmeta._errors, init_errors)
        self.assertDictEqual(self.qmeta._got_batches, init_got_batches)
        self.assertEqual(self.qmeta._uuids_added, init_uuids_added)
        self.assertEqual(self.qmeta._successes, init_successes)

    def test_values_added(self):
        '''Test UuidBaseQueueMeta values_added'''
        values_added = 3
        self.qmeta.values_added(values_added)
        # pylint: disable=protected-access
        self.assertEqual(self.qmeta._uuids_added, values_added)


class TestUuidBaseQueue(unittest.TestCase):
    '''UuidBaseQueue is the base class for queue functionality'''
    @classmethod
    def setUpClass(cls):
        queue_name = 'testbaseQ'
        cls.queue_name = queue_name
        cls.queue = UuidBaseQueue(queue_name)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.queue.purge()

    def test_init(self):
        '''Test TestUuidBaseQueue init'''
        expected_keys = [
            '_add_value',
            '_get_value',
            'add_values',
            'get_values',
            'purge',
            '_values',
            'max_value_size',
            'queue_name',
            'queue_type',
            'qmeta',
        ]
        keys = [
            key
            for key in dir(self.queue)
            if key[0:2] != '__'
        ]
        keys.sort()
        expected_keys.sort()
        self.assertListEqual(keys, expected_keys)
        self.assertTrue(hasattr(self.queue, 'max_value_size'))
        self.assertEqual(getattr(self.queue, 'max_value_size'), 262144)
        self.assertTrue(hasattr(self.queue, 'queue_name'))
        self.assertEqual(getattr(self.queue, 'queue_name'), self.queue_name)
        self.assertTrue(hasattr(self.queue, 'qmeta'))
        self.assertIsInstance(getattr(self.queue, 'qmeta'), UuidBaseQueueMeta)
        self.assertTrue(hasattr(self.queue, '_values'))
        # pylint: disable=protected-access
        self.assertListEqual(getattr(self.queue, '_values'), [])

    def test_add_value(self):
        '''Test TestUuidBaseQueue _add_value'''
        value = 'some-value'
        # pylint: disable=protected-access
        result = self.queue._add_value(value)
        self.assertTrue(result is True)
        self.assertListEqual(self.queue._values, [value])

    def test_add_value_none(self):
        '''Test TestUuidBaseQueue _add_value with None'''
        value = None
        # pylint: disable=protected-access
        result = self.queue._add_value(value)
        self.assertTrue(result is False)
        self.assertListEqual(self.queue._values, [])

    def test_get_value(self):
        '''Test TestUuidBaseQueue _get_value'''
        value = 'some-value'
        # pylint: disable=protected-access
        self.queue._add_value(value)
        result_value = self.queue._get_value()
        self.assertEqual(result_value, value)
        self.assertListEqual(self.queue._values, [])

    def test_get_value_empty(self):
        '''Test TestUuidBaseQueue _get_value when empty'''
        # pylint: disable=protected-access
        result_value = self.queue._get_value()
        self.assertIsNone(result_value)
        self.assertListEqual(self.queue._values, [])

    def test_add_values(self):
        '''Test TestUuidBaseQueue add_values'''
        values = ['value1', 'value2', 'value3']
        failed, bytes_added, call_cnt = self.queue.add_values(values)
        self.assertListEqual(failed, [])
        self.assertEqual(bytes_added, len(''.join(values)))
        self.assertEqual(call_cnt, len(values))
        # pylint: disable=protected-access
        self.queue._values.sort()
        self.assertListEqual(self.queue._values, values)

    def test_add_values_fail(self):
        '''Test TestUuidBaseQueue add_values with bad value'''
        good_values = ['value1', 'value2', 'value3']
        bad_values = [None, 0, '']
        values = good_values + bad_values
        failed, bytes_added, call_cnt = self.queue.add_values(values)
        self.assertEqual(len(bad_values), len(failed))
        for bad_value in bad_values:
            self.assertTrue(bad_value in failed)
        self.assertEqual(bytes_added, len(''.join(good_values)))
        self.assertEqual(call_cnt, len(good_values))
        # pylint: disable=protected-access
        self.queue._values.sort()
        good_values.sort()
        self.assertListEqual(self.queue._values, good_values)

    def test_get_values(self):
        '''Test TestUuidBaseQueue get_values'''
        values = ['value1', 'value2', 'value3']
        get_count = len(values)
        self.queue.add_values(values)
        result_values, call_cnt = self.queue.get_values(get_count)
        result_values.sort()
        self.assertListEqual(values, result_values)
        self.assertEqual(call_cnt, get_count)

    def test_get_values_greater_cnt(self):
        '''Test TestUuidBaseQueue get_values with count greater than values'''
        values = ['value1', 'value2', 'value3']
        get_count = len(values) + 1
        self.queue.add_values(values)
        result_values, call_cnt = self.queue.get_values(get_count)
        result_values.sort()
        self.assertListEqual(values, result_values)
        self.assertEqual(call_cnt, len(values))

    def test_get_values_less_cnt(self):
        '''Test TestUuidBaseQueue get_values with count less than values'''
        values = ['value1', 'value2', 'value3']
        get_count = len(values) - 1
        self.queue.add_values(values)
        result_values, call_cnt = self.queue.get_values(get_count)
        result_values.sort()
        for res_val in result_values:
            self.assertTrue(res_val in values)
        self.assertEqual(call_cnt, get_count)

    def test_get_values_zero(self):
        '''Test TestUuidBaseQueue get_values with no values'''
        get_count = 100
        result_values, call_cnt = self.queue.get_values(get_count)
        self.assertListEqual(result_values, [])
        self.assertEqual(call_cnt, 0)

    def test_purge(self):
        '''Test TestUuidBaseQueue purge'''
        values = ['value1', 'value2', 'value3']
        self.queue.add_values(values)
        self.queue.purge()
        # pylint: disable=protected-access
        self.assertListEqual(self.queue._values, [])
