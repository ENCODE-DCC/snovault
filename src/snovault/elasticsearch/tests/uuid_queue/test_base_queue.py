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

    def test_init(self):
        '''Test UuidBaseQueueMeta init'''
        expected_keys = [
            '_add_errors',
            '_base_id',
            '_errors',
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
        self.assertTrue(hasattr(self.qmeta, '_got_batches'))
        self.assertIsInstance(getattr(self.qmeta, '_got_batches'), dict)
        self.assertTrue(hasattr(self.qmeta, '_uuids_added'))
        self.assertIsInstance(getattr(self.qmeta, '_uuids_added'), int)
        self.assertTrue(hasattr(self.qmeta, '_successes'))
        self.assertIsInstance(getattr(self.qmeta, '_successes'), int)

    def test_add_errors(self):
        '''Test UuidBaseQueueMeta add_errors'''
        batch_id = 'some-batch-id'
        errors = ['some-error-1', 'some-error-2']
        # pylint: disable=protected-access
        self.qmeta._add_errors(batch_id, errors)
        self.assertTrue(batch_id in self.qmeta._errors)
        self.assertEqual(2, len(list(self.qmeta._errors.keys())))
        self.assertEqual(2, self.qmeta._errors['meta']['total'])
        self.qmeta.purge_meta()

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
        self.qmeta.purge_meta()

    def test_add_finished(self):
        '''Test UuidBaseQueueMeta add_finished'''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        errors = ['error1', 'error2']
        successes = len(values) - len(errors)
        did_finish, err_msg = self.qmeta.add_finished(batch_id, successes, errors)
        self.assertTrue(did_finish)
        self.assertIsNone(err_msg)
        # pylint: disable=protected-access
        self.assertEqual(successes, self.qmeta._successes)
        batch_errors = self.qmeta._errors[batch_id]
        batch_errors.sort()
        self.assertListEqual(errors, batch_errors)
        self.assertFalse(batch_id in self.qmeta._got_batches)
        self.qmeta.purge_meta()

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
        errors = ['error1']
        did_finish, err_msg = self.qmeta.add_finished(batch_id, successes, errors)
        self.assertFalse(did_finish)
        self.assertEqual(
            err_msg,
            'Batch Id %s expired' % batch_id
        )
        self.qmeta.purge_meta()

    def test_add_finished_no_batch(self):
        '''Test UuidBaseQueueMeta add_finished with bad batch_id'''
        batch_id = 'bad-batch-id'
        successes = 10
        errors = ['error1']
        did_finish, err_msg = self.qmeta.add_finished(batch_id, successes, errors)
        self.assertFalse(did_finish)
        self.assertEqual(
            err_msg,
            'Batch Id %s does not exist' % batch_id
        )
        self.qmeta.purge_meta()

    def test_add_finished_no_checkout(self):
        '''Test UuidBaseQueueMeta add_finished with bad checkout'''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        successes = len(values)
        errors = ['error1']
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
        self.qmeta.purge_meta()

    def test_get_errors(self):
        '''Test UuidBaseQueueMeta get_errors'''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        errors = ['error1', 'error2']
        successes = len(values) - len(errors)
        self.qmeta.add_finished(batch_id, successes, errors)
        batch_errors = self.qmeta.get_errors()
        batch_errors.sort()
        self.assertListEqual(errors, batch_errors)
        self.qmeta.purge_meta()

    def test_get_errors_none(self):
        '''Test UuidBaseQueueMeta get_errors when no errors'''
        values = ['uuid1', 'uuid2', 'uuid3']
        batch_id = self.qmeta.add_batch(values)
        errors = []
        successes = len(values) - len(errors)
        self.qmeta.add_finished(batch_id, successes, errors)
        batch_errors = self.qmeta.get_errors()
        batch_errors.sort()
        self.assertListEqual(errors, batch_errors)
        self.qmeta.purge_meta()

    def test_is_finished(self):
        '''Test UuidBaseQueueMeta is_finished'''
        self.qmeta.values_added(3)
        # pylint: disable=protected-access
        self.qmeta._successes = 2
        self.qmeta._errors['meta']['total'] = 1
        readd_values, did_finish = self.qmeta.is_finished()
        self.assertTrue(did_finish)
        self.assertListEqual(readd_values, [])
        self.qmeta.purge_meta()

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
        self.qmeta.purge_meta()

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
        self.qmeta.purge_meta()

    def test_purge_meta(self):
        '''Test UuidBaseQueueMeta purge_meta'''
        # pylint: disable=protected-access
        init_errors = {'meta': {'total': 0}}
        init_got_batches = {}
        init_uuids_added = 0
        init_successes = 0
        batch_id = 'some-batch-id'
        errors = ['some-error-1', 'some-error-2']
        self.qmeta._errors = copy.deepcopy(init_errors)
        self.qmeta._errors['meta']['total'] = len(errors)
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
        self.qmeta.purge_meta()

    def test_values_added(self):
        '''Test UuidBaseQueueMeta values_added'''
        values_added = 3
        self.qmeta.values_added(values_added)
        # pylint: disable=protected-access
        self.assertEqual(self.qmeta._uuids_added, values_added)


# class TestUuidBaseQueue(unittest.TestCase):
#     '''UuidBaseQueue is the base class for queue functionality'''
#     @classmethod
#     def setUpClass(cls):
#         cls._connection = createExpensiveConnectionObject()
#
#     @classmethod
#     def tearDownClass(cls):
#         cls._connection.destroy()
