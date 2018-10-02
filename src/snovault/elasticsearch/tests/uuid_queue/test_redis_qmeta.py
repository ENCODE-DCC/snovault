"""
Test the uuid_queue module redis_queues file
classes
    - RedisClient
    - RedisQueueMeta
"""
import time
import unittest

from ddt import ddt, data, unpack

from snovault.elasticsearch.uuid_queue.uuid_queue_adapter import UuidQueueTypes
from snovault.elasticsearch.uuid_queue.redis_queues import (
    RedisClient,
    RedisQueueMeta,
    RedisQueue,
    RedisPipeQueue,
    RedisListQueue,
    RedisListPipeQueue,
    RedisSetQueue,
    RedisSetPipeQueue,
    RedisSetPipeExecQueue,
)


REDIS_TEST_DB = 3


def _check_redis_conn(client):
    try:
        client.ping()
    except Exception:  # pylint: disable=broad-except
        return False
    return True


class TestRedisClient(unittest.TestCase):
    '''RedisClient is used in all Redis Queues for redis connections'''
    # TODO: Add test to check that RedisQueue cannot be created in RedisClient
    # And gives the correct error
    @classmethod
    def setUpClass(cls):
        cls.queue_name = 'testredisQ'
        args = {
            'host': 'localhost',
            'port': 6379,
            'db': REDIS_TEST_DB,
        }
        cls.args = args
        cls.client = RedisClient(args)

    def setUp(self):
        if not _check_redis_conn(self.client):
            self.fail('Redis Connection Failed')

    def test_init(self):
        '''Test BaseClient init'''
        try:
            RedisClient(self.args)
        except Exception: # pylint: disable=broad-except
            self.fail('Could not create redis client')

    def test_init_missing_args(self):
        '''Test BaseClient missing host'''
        args_list = [
            {'host': 'localhost'},
            {'port': 6379},
        ]
        for args in args_list:
            try:
                RedisClient(args)
            except KeyError:
                pass
            else:
                self.fail('Should fail with ValueError')

    def test_get_queue(self):
        '''Test BaseClient get_queue'''
        for queue_type in UuidQueueTypes.get_all():
            if 'REDIS' in queue_type:
                try:
                    self.client.get_queue(self.queue_name, queue_type)
                except Exception: # pylint: disable=broad-except
                    self.fail(
                        'Could not create redis clinet type %s' % queue_type
                    )

    def test_get_queue_bad_type(self):
        '''Test BaseClient get_queue with bad type'''
        queue_type = 'bad-queue-type'
        try:
            self.client.get_queue(self.queue_name, queue_type)
        except ValueError:
            pass
        else:
            self.fail('Should fail with ValueError')

@ddt
class TestRedisQueueMeta(unittest.TestCase):
    '''RedisQueueMeta is used in all Redis Queue for meta data'''
    # pylint: disable=too-many-public-methods
    @classmethod
    def setUpClass(cls):
        cls.queue_name = 'testredisQ'
        cls.args = {
            'host': 'localhost',
            'port': 6379,
            'db': REDIS_TEST_DB,
        }
        cls.client = RedisClient(cls.args)
        cls.queue_type = UuidQueueTypes.REDIS_LIST
        cls.queue = cls.client.get_queue(cls.queue_name, cls.queue_type)

    def setUp(self):
        if not _check_redis_conn(self.client):
            self.fail('Redis Connection Failed')
        self.queue.qmeta.set_args()

    def tearDown(self):
        self.queue.qmeta.purge_meta()

    def test_init_keys(self):
        '''Test RedisQueueMeta init redis db keys'''
        expected_redis_keys = [
            self.queue_name + ':mb:' + key
            for key in ['ra', 'ca', 'cs', 'be', 'ce', 'running']
        ]
        expected_redis_keys.append(self.queue_name + ':mb')
        found_redis_keys = []
        for item in list(dir(self.queue.qmeta)):
            if item[0:4] == '_key':
                found_redis_keys.append(getattr(self.queue.qmeta, item))
        expected_redis_keys.sort()
        found_redis_keys.sort()
        self.assertListEqual(expected_redis_keys, found_redis_keys)
        set_found_list = list(set(found_redis_keys))
        set_found_list.sort()
        self.assertListEqual(set_found_list, found_redis_keys)

    def test_set_args(self):
        '''Test RedisQueueMeta set_args'''
        self.queue.qmeta.set_args()
        # pylint: disable=protected-access
        checks = [
            (self.queue.qmeta._key_addedcount, '0'),
            (self.queue.qmeta._key_isrunning, 'true'),
            (self.queue.qmeta._key_errorscount, '0'),
            (self.queue.qmeta._key_successescount, '0'),
        ]
        for key, value in checks:
            got_val = self.queue._client.get(key)
            self.assertEqual(got_val, value)

    def test_add_errors(self):
        '''Test RedisQueueMeta _add_errors'''
        errors = [
            {'uuid': str(index), 'msg': 'error' + str(index)}
            for index in range(3)
        ]
        # pylint: disable=protected-access
        self.queue.qmeta._add_errors(errors)
        all_error_keys = self.queue._client.keys(
            self.queue.qmeta._key_errors + ':*'
        )
        self.assertEqual(len(all_error_keys), len(errors))
        errors_count = int(self.queue._client.get(self.queue.qmeta._key_errorscount))
        self.assertEqual(errors_count, len(errors))
        for error in errors:
            key = self.queue.qmeta._key_errors + ':' + error['uuid']
            err_hash = self.queue._client.hgetall(key)
            self.assertEqual(len(err_hash.keys()), len(error.keys()))
            self.assertEqual(err_hash['msg'], error['msg'])
            self.assertEqual(err_hash['uuid'], error['uuid'])

    def test_get_batch_keys_for_id(self):
        '''Test RedisQueueMeta _get_batch_keys_for_id'''
        batch_id = 'some-batch-id'
        # pylint: disable=protected-access
        res_bk_expired = self.queue.qmeta._key_metabase + ':' + batch_id + ':ex'
        res_bk_timestamp = self.queue.qmeta._key_metabase + ':' + batch_id + ':ts'
        res_bk_values = self.queue.qmeta._key_metabase + ':' + batch_id + ':vs'
        (bk_expired, bk_timestamp,
         bk_values) = self.queue.qmeta._get_batch_keys_for_id(batch_id)
        self.assertEqual(bk_expired, res_bk_expired)
        self.assertEqual(bk_timestamp, res_bk_timestamp)
        self.assertEqual(bk_values, res_bk_values)

    def test_get_batch_id_from_key(self):
        '''Test RedisQueueMeta _get_batch_id_from_key'''
        batch_id = 'some-batch-id'
        # pylint: disable=protected-access
        res_bk_expired = self.queue.qmeta._key_metabase + ':' + batch_id + ':ex'
        res_bk_timestamp = self.queue.qmeta._key_metabase + ':' + batch_id + ':ts'
        res_bk_values = self.queue.qmeta._key_metabase + ':' + batch_id + ':vs'
        self.assertEqual(
            self.queue.qmeta._get_batch_id_from_key(res_bk_expired), batch_id
        )
        self.assertEqual(
            self.queue.qmeta._get_batch_id_from_key(res_bk_timestamp), batch_id
        )
        self.assertEqual(
            self.queue.qmeta._get_batch_id_from_key(res_bk_values), batch_id
        )

    def test_check_expired_all_expired(self):
        '''
        Test RedisQueueMeta _check_expired with all expired batches
        '''
        # TODO: TIME DEPENDENT TEST! Can fix by seperating check timestamp into
        # function and mocking the output
        max_age_secs = 0.01
        batches = []
        batches.append([str(index) for index in range(0, 3)])
        batches.append([str(index) for index in range(3, 6)])
        batches.append([str(index) for index in range(6, 9)])
        all_values = []
        batch_ids = []
        for values in batches:
            batch_id = self.queue.qmeta.add_batch(values)
            batch_ids.append(batch_id)
            all_values.extend(values)
        time.sleep(max_age_secs * 2)
        # pylint: disable=protected-access
        res_readd_values = self.queue.qmeta._check_expired(
            max_age_secs,
        )
        res_readd_values.sort()
        all_values.sort()
        self.assertListEqual(res_readd_values, all_values)
        for batch_id in batch_ids:
            bk_expired, *_ = self.queue.qmeta._get_batch_keys_for_id(batch_id)
            expired_int = int(self.queue._client.get(bk_expired))
            self.assertEqual(1, expired_int)

    def test_check_expired_no_expired(self):
        '''
        Test RedisQueueMeta _check_expired with no expired batches
        '''
        max_age_secs = 1000
        batches = []
        batches.append([str(index) for index in range(0, 3)])
        batches.append([str(index) for index in range(3, 6)])
        batches.append([str(index) for index in range(6, 9)])
        batch_ids = []
        for values in batches:
            batch_id = self.queue.qmeta.add_batch(values)
            batch_ids.append(batch_id)
        # pylint: disable=protected-access
        res_readd_values = self.queue.qmeta._check_expired(max_age_secs)
        res_readd_values.sort()
        self.assertListEqual(res_readd_values, [])
        for batch_id in batch_ids:
            bk_expired, *_ = self.queue.qmeta._get_batch_keys_for_id(batch_id)
            expired_int = int(self.queue._client.get(bk_expired))
            self.assertEqual(0, expired_int)

    def test_check_expired_some_expired(self):
        '''
        Test RedisQueueMeta _check_expired with some expired batches
        '''
        # TODO: TIME DEPENDENT TEST! Can fix by seperating check timestamp into
        # function and mocking the output
        max_age_secs = 1
        batch_1 = [str(index) for index in range(0, 3)]
        batch_2 = [str(index) for index in range(3, 6)]
        batch_3 = [str(index) for index in range(6, 9)]
        batch_id_1 = self.queue.qmeta.add_batch(batch_1)
        time.sleep(max_age_secs * 10)
        batch_id_2 = self.queue.qmeta.add_batch(batch_2)
        batch_id_3 = self.queue.qmeta.add_batch(batch_3)
        # pylint: disable=protected-access
        res_readd_values = self.queue.qmeta._check_expired(max_age_secs)
        res_readd_values.sort()
        batch_1.sort()
        self.assertListEqual(res_readd_values, batch_1)
        bk_expired, *_ = self.queue.qmeta._get_batch_keys_for_id(batch_id_1)
        expired_int = int(self.queue._client.get(bk_expired))
        self.assertEqual(1, expired_int)
        bk_expired, *_ = self.queue.qmeta._get_batch_keys_for_id(batch_id_2)
        expired_int = int(self.queue._client.get(bk_expired))
        self.assertEqual(0, expired_int)
        bk_expired, *_ = self.queue.qmeta._get_batch_keys_for_id(batch_id_3)
        expired_int = int(self.queue._client.get(bk_expired))
        self.assertEqual(0, expired_int)

    def test_check_expired_already_expired(self):
        '''
        Test RedisQueueMeta _check_expired when already expired
        For Example, when remove_batch fails, the batch is expired immediatley.
        '''
        max_age_secs = 10
        batches = []
        batches.append([str(index) for index in range(0, 3)])
        batches.append([str(index) for index in range(3, 6)])
        batches.append([str(index) for index in range(6, 9)])
        all_values = []
        batch_ids = []
        # pylint: disable=protected-access
        for values in batches:
            batch_id = self.queue.qmeta.add_batch(values)
            batch_ids.append(batch_id)
            all_values.extend(values)
            bk_expired, *_ = self.queue.qmeta._get_batch_keys_for_id(batch_id)
            self.queue.qmeta._client.set(bk_expired, 1)
        res_readd_values = self.queue.qmeta._check_expired(
            max_age_secs,
        )
        res_readd_values.sort()
        all_values.sort()
        self.assertListEqual(res_readd_values, all_values)

    def test_check_expired_restart(self):
        '''
        Test RedisQueueMeta _check_expired when restarting
        For Example, when remove_batch fails, the batch is expired immediatley.
        '''
        max_age_secs = 10
        batches = []
        batches.append([str(index) for index in range(0, 3)])
        batches.append([str(index) for index in range(3, 6)])
        batches.append([str(index) for index in range(6, 9)])
        all_values = []
        batch_ids = []
        # pylint: disable=protected-access
        for values in batches:
            batch_id = self.queue.qmeta.add_batch(values)
            batch_ids.append(batch_id)
            all_values.extend(values)
            bk_expired, *_ = self.queue.qmeta._get_batch_keys_for_id(batch_id)
            self.queue.qmeta._client.set(bk_expired, 1)
        res_readd_values = self.queue.qmeta._check_expired(
            max_age_secs,
            listener_restarted=True,
        )
        res_readd_values.sort()
        all_values.sort()
        self.assertListEqual(res_readd_values, all_values)

    def test_check_expired_none(self):
        '''Test RedisQueueMeta _check_expired when no batches'''
        max_age_secs = 10
        # pylint: disable=protected-access
        res_readd_values = self.queue.qmeta._check_expired(max_age_secs)
        self.assertListEqual(res_readd_values, [])

    def test_is_server_running(self):
        '''Test RedisQueueMeta is_server_running'''
        # pylint: disable=protected-access
        key = self.queue.qmeta._key_isrunning
        self.queue._client.set(key, 'true')
        self.assertTrue(self.queue.qmeta.is_server_running())

    def test_is_server_not_running(self):
        '''Test RedisQueueMeta is_server_running when not running'''
        # pylint: disable=protected-access
        key = self.queue.qmeta._key_isrunning
        self.queue._client.set(key, 'false')
        self.assertFalse(self.queue.qmeta.is_server_running())
        self.queue._client.set(key, 'tru')
        self.assertFalse(self.queue.qmeta.is_server_running())
        self.queue._client.delete(key)
        self.assertFalse(self.queue.qmeta.is_server_running())

    def test_set_to_not_running(self):
        '''Test RedisQueueMeta set_to_not_running'''
        # pylint: disable=protected-access
        key = self.queue.qmeta._key_isrunning
        self.queue._client.set(key, 'true')
        self.queue.qmeta.set_to_not_running()
        self.assertEqual(
            'false',
            self.queue._client.get(key)
        )

    def test_purge_meta(self):
        '''Test RedisQueueMeta purge_meta'''
        # pylint: disable=protected-access
        key = self.queue.qmeta._key_metabase + '*'
        pre_keys = self.queue._client.keys(key)
        self.assertTrue(len(pre_keys) > 0)
        self.queue.qmeta.purge_meta()
        post_keys = self.queue._client.keys(key)
        self.assertTrue(len(post_keys) == 0)

    def test_add_batch(self):
        '''Test RedisQueueMeta add_batch'''
        batches = [
            [str(index) for index in range(0, 3)],
            [str(index) for index in range(3, 6)],
            [str(index) for index in range(6, 9)],
        ]
        batch_ids = []
        # pylint: disable=protected-access
        start_base_id = self.queue.qmeta._base_id
        for index, values in enumerate(batches):
            batch = {
                'batch_id': self.queue.qmeta.add_batch(values),
                'values': values,
            }
            self.assertTrue(batch['batch_id'])
            self.assertEqual(start_base_id + index, int(batch['batch_id']))
            batch_ids.append(batch)
        for batch in batch_ids:
            batch_id = batch['batch_id']
            values = batch['values']
            (bk_expired, bk_timestamp,
             bk_values) = self.queue.qmeta._get_batch_keys_for_id(batch_id)
            print(batch_id, bk_expired, bk_timestamp, bk_values)
            self.assertEqual(int(self.queue._client.get(bk_expired)), 0)
            self.assertTrue(int(self.queue._client.get(bk_timestamp)))
            res_values = self.queue._client.lrange(bk_values, 0, -1)
            res_values.sort()
            values.sort()
            self.assertListEqual(res_values, values)

    def test_add_batch_no_values(self):
        '''Test RedisQueueMeta add_batch with no values'''
        self.assertIsNone(self.queue.qmeta.add_batch(None))
        self.assertIsNone(self.queue.qmeta.add_batch([]))

    def test_remove_batch_successes(self):
        '''Test RedisQueueMeta remove_batch with all successes'''
        batches = [
            [str(index) for index in range(0, 3)],
            [str(index) for index in range(3, 6)],
            [str(index) for index in range(6, 9)],
        ]
        batch_ids_values = []
        all_values = []
        for values in batches:
            batch_id = self.queue.qmeta.add_batch(values)
            batch_ids_values.append((batch_id, values))
            all_values.extend(values)
        errors = []
        for batch_id, values in batch_ids_values:
            successes = len(values)
            self.queue.qmeta.remove_batch(batch_id, successes, errors)
        # pylint: disable=protected-access
        qmeta_successes = int(
            self.queue.qmeta._client.get(self.queue.qmeta._key_successescount)
        )
        self.assertEqual(len(all_values), qmeta_successes)
        for batch_id, values in batch_ids_values:
            bk_keys_tuple = self.queue.qmeta._get_batch_keys_for_id(str(batch_id))
            for bk_key in bk_keys_tuple:
                self.assertFalse(
                    self.queue._client.exists(bk_key)
                )
        got_errors, warn_msg = self.queue.qmeta.get_errors()
        self.assertIsNone(warn_msg)
        self.assertListEqual(got_errors, [])

    def test_remove_batch_with_errors(self):  # pylint: disable=too-many-locals
        '''
        Test RedisQueueMeta remove_batch with errors
        * This also tests _remove_batch
        '''
        batches = [
            [str(index) for index in range(0, 3)],
            [str(index) for index in range(3, 6)],
            [str(index) for index in range(6, 9)],
        ]
        batch_ids_values = []
        all_values = []
        for values in batches:
            batch_id = self.queue.qmeta.add_batch(values)
            batch_ids_values.append((batch_id, values))
            all_values.extend(values)
        errors = [
            {'uuid': value, 'msg': 'error' + str(value)}
            for value in batches[0]
        ]
        batch_id, values = batch_ids_values.pop()
        successes = len(values)
        self.queue.qmeta.remove_batch(batch_id, successes, errors)
        for batch_id, values in batch_ids_values:
            successes = len(values)
            self.queue.qmeta.remove_batch(batch_id, successes, [])
        # pylint: disable=protected-access
        qmeta_successes = int(
            self.queue.qmeta._client.get(self.queue.qmeta._key_successescount)
        )
        self.assertEqual(len(all_values) - len(errors), qmeta_successes)
        for batch_id, values in batch_ids_values:
            bk_keys_tuple = self.queue.qmeta._get_batch_keys_for_id(str(batch_id))
            for bk_key in bk_keys_tuple:
                self.assertFalse(
                    self.queue._client.exists(bk_key)
                )
        res_errors, warn_msg = self.queue.qmeta.get_errors()
        self.assertIsNone(warn_msg)
        for res_error in res_errors:
            found = False
            for error in errors:
                if error['uuid'] == res_error['uuid']:
                    found = True
                    break
            self.assertTrue(found)

    def test_remove_batch_with_expired(self):
        '''
        Test RedisQueueMeta remove_batch with expired
        * This also tests _remove_batch
        '''
        batch_values = [str(index) for index in range(0, 3)]
        batch_id = self.queue.qmeta.add_batch(batch_values)
        # pylint: disable=protected-access
        bk_expired, bk_timestamp, bk_values = self.queue.qmeta._get_batch_keys_for_id(
            batch_id
        )
        self.queue._client.set(bk_expired, 1)
        successes = len(batch_values)
        errors = []
        self.queue.qmeta.remove_batch(batch_id, successes, errors)
        qmeta_successes = int(
            self.queue.qmeta._client.get(self.queue.qmeta._key_successescount)
        )
        self.assertEqual(qmeta_successes, 0)
        self.assertTrue(self.queue._client.exists(bk_expired))
        self.assertTrue(self.queue._client.exists(bk_timestamp))
        self.assertTrue(self.queue._client.exists(bk_values))

    @data(
        # value_cnt, successes, error_cnt
        (3, 2, 0),
        (3, 0, 2),
        (3, 3, 1),
        (3, 1, 3),
    )
    @unpack
    def test_remove_batch_with_checkout(self, value_cnt, successes, error_cnt):
        '''
        Test RedisQueueMeta remove_batch with not did_check_out
        * This also tests _remove_batch
        '''
        batch_values = [str(index) for index in range(0, value_cnt)]
        batch_id = self.queue.qmeta.add_batch(batch_values)
        # pylint: disable=protected-access
        bk_expired, bk_timestamp, bk_values = self.queue.qmeta._get_batch_keys_for_id(
            batch_id
        )
        errors = [
            {'uuid': str(index), 'msg': 'error' + str(index)}
            for index in range(0, error_cnt)
        ]
        self.queue.qmeta.remove_batch(batch_id, successes, errors)
        qmeta_successes = int(
            self.queue.qmeta._client.get(self.queue.qmeta._key_successescount)
        )
        self.assertEqual(qmeta_successes, 0)
        self.assertTrue(self.queue._client.exists(bk_expired))
        self.assertTrue(self.queue._client.exists(bk_timestamp))
        self.assertTrue(self.queue._client.exists(bk_values))

    @data(
        # batch_by, restart, snapshot_id, uuid_len, xmin
        (4, False, 'some-str', 14, 1234567),
        (4, True, 'some-str', 14, 1234567),
    )
    def test_get_run_args(self, set_args_tuple):
        '''Test RedisQueueMeta get_run_args'''
        set_args = {
            'batch_by': set_args_tuple[0],
            'restart': set_args_tuple[1],
            'snapshot_id': set_args_tuple[2],
            'uuid_len': set_args_tuple[3],
            'xmin': set_args_tuple[4],
        }
        # pylint: disable=protected-access
        key_runargs = self.queue.qmeta._key_runargs
        self.queue._client.hmset(key_runargs, set_args)
        run_args = self.queue.qmeta.get_run_args()
        self.assertDictEqual(set_args, run_args)

    @data(
        # batch_by, restart, snapshot_id, uuid_len, xmin, extra
        (4, False, 'some-str', 14, 1234567, None),
        (4, True, 'some-str', 14, 1234567, 'extra'),
    )
    def test_set_run_args(self, set_args_tuple):
        '''Test RedisQueueMeta set_run_args'''
        run_args = {
            'batch_by': set_args_tuple[0],
            'restart': set_args_tuple[1],
            'snapshot_id': set_args_tuple[2],
            'uuid_len': set_args_tuple[3],
            'xmin': set_args_tuple[4],
        }
        if set_args_tuple[5]:
            run_args['extra'] = set_args_tuple[5]
        self.queue.qmeta.set_run_args(run_args)
        if set_args_tuple[5]:
            del run_args['extra']
        got_run_args = self.queue.qmeta.get_run_args()
        self.assertDictEqual(got_run_args, run_args)

    @data(
        # error_cnt
        (0), (3)
    )
    def test_get_errors(self, error_cnt):
        '''Test RedisQueueMeta get_errors'''
        errors = [
            {'uuid': str(value), 'msg': 'error' + str(value)}
            for value in range(error_cnt)
        ]
        # pylint: disable=protected-access
        error_key_base = self.queue.qmeta._key_errors
        for error in errors:
            error_key = error_key_base + ':' + error['uuid']
            self.queue._client.hmset(error_key, error)
        self.queue._client.set(self.queue.qmeta._key_errorscount, len(errors))
        got_errors, warn_msg = self.queue.qmeta.get_errors()
        self.assertIsNone(warn_msg)
        for got_error in got_errors:
            found = False
            for error in errors:
                if got_error['uuid'] == error['uuid']:
                    self.assertDictEqual(error, got_error)
                    found = True
                    break
            self.assertTrue(found)
        self.assertEqual(len(got_errors), len(errors))

    def test_get_errors_off(self):
        '''Test RedisQueueMeta get_errors when count is off'''
        error_cnt = 3
        errors = [
            {'uuid': str(value), 'msg': 'error' + str(value)}
            for value in range(error_cnt)
        ]
        # pylint: disable=protected-access
        error_key_base = self.queue.qmeta._key_errors
        for error in errors:
            error_key = error_key_base + ':' + error['uuid']
            self.queue._client.hmset(error_key, error)
        off_error_cnt = len(errors) + 1
        self.queue._client.set(
            self.queue.qmeta._key_errorscount,
            off_error_cnt,
        )
        got_errors, warn_msg = self.queue.qmeta.get_errors()
        self.assertEqual(
            warn_msg,
            'Redis Meta get_errors count is off: %d != %d' % (
                off_error_cnt,
                len(got_errors),
            )
        )
        for got_error in got_errors:
            found = False
            for error in errors:
                if got_error['uuid'] == error['uuid']:
                    self.assertDictEqual(error, got_error)
                    found = True
                    break
            self.assertTrue(found)
        self.assertEqual(len(got_errors), len(errors))

    @data(
        # errors_cnt, successes_cnt, uuids_added
        (0, 0, 0),
        (0, 13, 13),
        (7, 6, 13),
    )
    @unpack
    def test_is_finished(self, errors_cnt, successes_cnt, uuids_added):
        '''
        Test RedisQueueMeta is_finished
        '''
        max_age_secs = None
        # pylint: disable=protected-access
        self.queue._client.set(self.queue.qmeta._key_errorscount, errors_cnt)
        self.queue._client.set(self.queue.qmeta._key_successescount, successes_cnt)
        self.queue._client.set(self.queue.qmeta._key_addedcount, uuids_added)
        expired_values, did_finish = self.queue.qmeta.is_finished(
            max_age_secs=max_age_secs
        )
        self.assertEqual(len(expired_values), 0)
        self.assertTrue(did_finish)

    @data(
        # errors_cnt, successes_cnt, uuids_added
        (1, 0, 0),
        (0, 1, 0),
        (0, 0, 1),
    )
    @unpack
    def test_is_finished_cnt_off(self, errors_cnt, successes_cnt, uuids_added):
        '''
        Test RedisQueueMeta is_finished when the counts are off
        '''
        max_age_secs = None
        # pylint: disable=protected-access
        self.queue._client.set(self.queue.qmeta._key_errorscount, errors_cnt)
        self.queue._client.set(self.queue.qmeta._key_successescount, successes_cnt)
        self.queue._client.set(self.queue.qmeta._key_addedcount, uuids_added)
        expired_values, did_finish = self.queue.qmeta.is_finished(
            max_age_secs=max_age_secs
        )
        self.assertEqual(len(expired_values), 0)
        self.assertFalse(did_finish)

    def test_is_finished_expired(self):
        '''
        Test RedisQueueMeta is_finished with expired values
        - max_age_secs can be anything except None
        '''
        max_age_secs = 234039
        batch_values = [str(index) for index in range(0, 3)]
        batch_id = self.queue.qmeta.add_batch(batch_values)
        # pylint: disable=protected-access
        bk_expired, *_ = self.queue.qmeta._get_batch_keys_for_id(
            batch_id
        )
        self.queue._client.set(bk_expired, 1)
        expired_values, did_finish = self.queue.qmeta.is_finished(
            max_age_secs=max_age_secs
        )
        self.assertEqual(len(expired_values), len(batch_values))
        self.assertFalse(did_finish)

    @data(
        # len_values
        (0), (1), (7), (-4),
    )
    def test_values_added(self, len_values):
        '''
        Test RedisQueueMeta values_added increment output
        '''
        self.queue.qmeta.values_added(len_values)
        # pylint: disable=protected-access
        res = self.queue._client.get(self.queue.qmeta._key_addedcount)
        self.assertIsInstance(res, str)
        self.assertEqual(len_values, int(res))

    def test_is_useable(self):
        '''Test is_useable - not implemented yet'''
        try:
            self.queue.qmeta.is_useable()
        except NotImplementedError:
            pass
        else:
            self.fail('Should fail with NotImplementedError')
