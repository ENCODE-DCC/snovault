"""Test Redis Queue File"""
from unittest import (
    TestCase,
    mock,
)

from ..queues.redis_queues import (  # pylint: disable=unused-import
    REDIS_LIST,
    REDIS_LIST_PIPE,
    REDIS_SET,
    REDIS_SET_PIPE,
    REDIS_SET_PIPE_EXEC,
    RedisClient,
    RedisQueueMeta,
    RedisQueue,
    RedisListQueue,
    RedisListPipeQueue,
    RedisSetQueue,
    RedisSetPipeQueue,
    RedisSetPipeExecQueue,
)


MOCK_TIME = 123456.654321
REDIS_TEST_DB = 3
REDIS_QUEUES = [
    ('REDIS_LIST', REDIS_LIST),
    ('REDIS_LIST_PIPE', REDIS_LIST_PIPE),
    ('REDIS_SET', REDIS_SET),
    ('REDIS_SET_PIPE', REDIS_SET_PIPE),
    ('REDIS_SET_PIPE_EXEC', REDIS_SET_PIPE_EXEC),
]
REDIS_QUEUE_CLASSES = [
    (REDIS_LIST, RedisListQueue),
    (REDIS_LIST_PIPE, RedisListPipeQueue),
    (REDIS_SET, RedisSetQueue),
    (REDIS_SET_PIPE, RedisSetPipeQueue),
    (REDIS_SET_PIPE_EXEC, RedisSetPipeExecQueue),
]
DEFAULT_QUEUE_OPTS = {
    'queue_name': 'TestRedisClient',
    'db': REDIS_TEST_DB,
    'host': 'localhost',
    'port': 6379,
}


class TestRedisClient(TestCase):
    '''Test Redis Queue Client Class'''
    queue_options = DEFAULT_QUEUE_OPTS.copy()
    def test_get_queue(self):
        '''Test get_queue'''
        for queue_type, queue_class in REDIS_QUEUE_CLASSES:
            client = RedisClient(self.queue_options)
            queue = client.get_queue(self.queue_options['queue_name'], queue_type)
            self.assertIsInstance(queue, queue_class)

    def test_get_queue_bad_type(self):
        '''Test get_queue with bad queue type'''
        client = RedisClient(self.queue_options)
        try:
            queue_type = 'bad queue type'
            _ = client.get_queue(self.queue_options['queue_name'], queue_type)
        except ValueError:
            pass
        else:
            self.fail('ValueError expected')


class TestRedisQueueMeta(TestCase):
    '''Test Redis Queue Meta Class'''
    # pylint: disable=too-many-public-methods
    members = [
        ('_base_id', int),
        ('_client', RedisClient),
        ('queue_name', str),
        # run args
        ('_key_metabase', str),
        ('_key_runargs', str),
        ('_key_uuidcount', str),
        ('_key_addedcount', str),
        ('_key_errors', str),
        ('_key_uuid_errors', str),
        ('_key_errorscount', str),
        ('_key_successescount', str),
        ('_key_workers', str),
        ('_key_worker_conn', str),
        ('_key_worker_results', str),
    ]
    meth_func = [
        # Errors
        'add_errors',
        'has_errors',
        'pop_errors',
        # Worker
        'add_worker_conn',
        '_get_worker_conn',
        '_get_worker_ids',
        'get_worker_conns',
        'update_worker_conn',
        'save_work_results',
        # Uuids
        'get_uuid_count',
        'has_uuids',
        'update_uuid_count',
        'update_success_count',
        # Run Args
        '_setup_redis_keys',
        'set_args',
        'get_run_args',
        'set_run_args',
        # base - not used?
        'get_worker_conn_count',
        '_get_blank_worker',
        '_init_persistant_data', # not tested
        'get_server_restarts', # not tested
        'update_errors_count', # not tested
    ]

    @classmethod
    def start_redis_server(cls):
        '''Helper to start redis-server'''


    @classmethod
    def stop_redis_server(cls):
        '''Helper to stop redis-server'''


    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        cls.queue_type = REDIS_LIST
        cls.queue_options = DEFAULT_QUEUE_OPTS.copy()
        client = RedisClient(cls.queue_options)
        cls.mock_time_us = int(MOCK_TIME * 1000000)
        cls.queue_meta = RedisQueueMeta(
            cls.queue_options['queue_name'],
            client
        )
        cls.standard_errors = [
            {'uuid': str(cnt), 'error': 'e' + str(cnt)}
            for cnt in range(10)
        ]
        # pylint: disable=protected-access
        for key in cls.queue_meta._client.keys('*'):
            cls.queue_meta._client.delete(key)

    @classmethod
    def tearDownClass(cls):
        '''Clean up after running test class'''
        # pylint: disable=protected-access
        for key in cls.queue_meta._client.keys('*'):
            cls.queue_meta._client.delete(key)

    def setUp(self):
        '''Setup before each method call'''
        self.queue_meta.set_args()

    def tearDown(self):
        """Clean up after each method call"""
        # pylint: disable=protected-access
        self.queue_meta.pop_errors()
        self.queue_meta._client.delete(self.queue_meta._key_errorscount)
        worker_ids = self.queue_meta._client.lrange(
            self.queue_meta._key_workers, 0, -1
        )
        self.queue_meta._client.delete(self.queue_meta._key_workers)
        for worker_id in worker_ids:
            self.queue_meta._client.delete(
                self.queue_meta._key_worker_conn + ':' + worker_id
            )
        partial_keys = self.queue_meta._client.keys(
            self.queue_meta._key_worker_results + ':*'
        )
        for key in partial_keys:
            self.queue_meta._client.delete(key)
        self.queue_meta._client.delete(self.queue_meta._key_uuidcount)

    # private/protected
    def test_init_dir(self):
        '''Test RedisQueueMeta has expected function and variables'''
        dir_items = [
            item
            for item in dir(self.queue_meta)
            if item[0:2] != '__'
        ]
        member_names = [name for name, _ in self.members]
        for item in dir_items:
            if item in member_names:
                continue
            if item in self.meth_func:
                continue
            raise AssertionError('Member or method not being tested:%s' % item)
        for item, item_type in self.members:
            self.assertTrue(hasattr(self.queue_meta, item))
            self.assertIsInstance(
                getattr(self.queue_meta, item),
                item_type
            )
        for item in self.meth_func:
            self.assertTrue(hasattr(self.queue_meta, item))
            self.assertTrue(
                str(type(getattr(self.queue_meta, item))) in [
                    "<class 'method'>",
                    "<class 'function'>"
                ]
            )

    def test_init_basic(self):
        '''Test basic queue meta init'''
        # pylint: disable=protected-access
        self.assertEqual(self.mock_time_us, self.queue_meta._base_id)
        errors = self.queue_meta.pop_errors()
        self.assertListEqual([], errors)
        self.assertEqual(0, self.queue_meta.get_uuid_count())
        worker_conns = self.queue_meta.get_worker_conns()
        self.assertDictEqual({}, worker_conns)

    # Errors
    def test_add_errors(self):
        '''Test add_errors'''
        errors_to_add = self.standard_errors.copy()
        expected_result = len(errors_to_add)
        result = self.queue_meta.add_errors(errors_to_add)
        self.assertEqual(expected_result, result)
        exp_sorted_errors = sorted(errors_to_add, key=lambda k: k['uuid'])
        errors = self.queue_meta.pop_errors()
        res_sorted_errors = sorted(errors, key=lambda k: k['uuid'])
        self.assertListEqual(exp_sorted_errors, res_sorted_errors)

    def test_add_errors_none(self):
        '''Test add_errors when no errors'''
        errors_to_add = []
        expected_result = len(errors_to_add)
        result = self.queue_meta.add_errors(errors_to_add)
        self.assertEqual(expected_result, result)
        errors = self.queue_meta.pop_errors()
        self.assertListEqual([], errors)

    def test_has_errors(self):
        '''Test has_errors'''
        errors_to_add = self.standard_errors.copy()
        self.queue_meta.add_errors(errors_to_add)
        self.assertTrue(self.queue_meta.has_errors())

    def test_has_errors_none(self):
        '''Test has_errors when no errors'''
        self.assertFalse(self.queue_meta.has_errors())

    def test_pop_errors(self):
        '''Test pop_errors'''
        errors_to_add = self.standard_errors.copy()
        self.queue_meta.add_errors(errors_to_add)
        errors = self.queue_meta.pop_errors()
        # pylint: disable=protected-access
        err_cnt = int(
            self.queue_meta._client.get(self.queue_meta._key_errorscount)
        )
        self.assertEqual(err_cnt, 0)
        exp_sorted_errors = sorted(errors_to_add, key=lambda k: k['uuid'])
        res_sorted_errors = sorted(errors, key=lambda k: k['uuid'])
        self.assertListEqual(exp_sorted_errors, res_sorted_errors)

    # Workers
    def test_add_worker_conn(self):
        '''Test add_worker_conn'''
        worker_id = 'test-worker-id-a'
        self.queue_meta.add_worker_conn(worker_id)
        # pylint: disable=protected-access
        worker_ids = self.queue_meta._client.lrange(
            self.queue_meta._key_workers, 0, -1
        )
        self.assertListEqual(worker_ids, [worker_id])
        for worker_id in worker_ids:
            worker_conn_key = self.queue_meta._key_worker_conn + ':' + worker_id
            worker_hash = self.queue_meta._client.hgetall(worker_conn_key)
            worker_hash['uuid_cnt'] = int(worker_hash['uuid_cnt'])
            worker_hash['get_cnt'] = int(worker_hash['get_cnt'])
            self.assertDictEqual(worker_hash, self.queue_meta._get_blank_worker())

    def test_get_worker_conn(self):
        '''Test _get_worker_conn'''
        worker_id = 'test-worker-id-b'
        self.queue_meta.add_worker_conn(worker_id)
        # pylint: disable=protected-access
        worker_conn = self.queue_meta._get_worker_conn(worker_id)
        worker_conn['uuid_cnt'] = int(worker_conn['uuid_cnt'])
        worker_conn['get_cnt'] = int(worker_conn['get_cnt'])
        self.assertDictEqual(worker_conn, self.queue_meta._get_blank_worker())

    def test_get_worker_conns(self):
        '''Test _get_worker_conns'''
        worker_ids = ['test-worker-id-1', 'test-worker-id-2', 'test-worker-id-3']
        for worker_id in worker_ids:
            self.queue_meta.add_worker_conn(worker_id)
        worker_conns = self.queue_meta.get_worker_conns()
        self.assertEqual(len(worker_conns), len(worker_ids))
        for worker_conn in worker_conns.values():
            self.assertTrue(worker_conn['uuid_cnt'], '0')
            self.assertTrue(worker_conn['get_cnt'], '0')

    def test_update_worker_conn(self):
        '''Test update_worker_conn'''
        worker_id = 'test-worker-id-c'
        self.queue_meta.add_worker_conn(worker_id)
        uuid_cnt = 5
        get_cnt = 6
        self.queue_meta.update_worker_conn(worker_id, uuid_cnt, get_cnt)
        # pylint: disable=protected-access
        worker_conn = self.queue_meta._get_worker_conn(worker_id)
        self.assertEqual(int(worker_conn['uuid_cnt']), uuid_cnt)
        self.assertEqual(int(worker_conn['get_cnt']), get_cnt)

    def test_update_worker_conn_none(self):
        '''Test update_worker_conn when no worker'''
        worker_id = 'test-worker-id-d'
        # pylint: disable=protected-access
        uuid_cnt = 5
        get_cnt = 6
        self.queue_meta.update_worker_conn(worker_id, uuid_cnt, get_cnt)
        worker_conn = self.queue_meta._get_worker_conn(worker_id)
        self.assertDictEqual(worker_conn, {})

    def test_save_work_results(self):
        '''Test save_work_results'''
        worker_id = 'test-worker-id-e'
        self.queue_meta.add_worker_conn(worker_id)
        given_results = {'errors': ['e3', 'e4'], 'successes': 5}
        results_count = 1
        self.queue_meta.save_work_results(worker_id, given_results.copy())
        # pylint: disable=protected-access
        worker_res_key = '{}:{}:{}'.format(
            self.queue_meta._key_worker_results,
            worker_id,
            results_count,
        )
        got_results = self.queue_meta._client.hgetall(worker_res_key)
        got_results['successes'] = int(got_results['successes'])
        got_results['errors'] = int(got_results['errors'])
        given_results['errors'] = len(given_results['errors'])
        self.assertDictEqual(got_results, given_results)
        worker_conn = self.queue_meta._get_worker_conn(worker_id)
        self.assertEqual(int(worker_conn['results_count']), 1)

    def test_save_work_results_nowrk(self):
        '''Test save_work_results when no worker'''
        worker_id = 'test-worker-id-e'
        given_results = {'errors': ['e1', 'e2'], 'successes': 4}
        results_count = 1
        self.queue_meta.save_work_results(worker_id, given_results.copy())
        # pylint: disable=protected-access
        worker_res_key = '{}:{}:{}'.format(
            self.queue_meta._key_worker_results,
            worker_id,
            results_count,
        )
        got_results = self.queue_meta._client.hgetall(worker_res_key)
        self.assertDictEqual(got_results, {})
        worker_conn = self.queue_meta._get_worker_conn(worker_id)
        self.assertFalse('results_count' in worker_conn)

    def test_save_work_results_twice(self):
        '''Test save_work_results twice'''
        worker_id = 'test-worker-id-e'
        self.queue_meta.add_worker_conn(worker_id)
        given_results_1 = {'errors': ['1e1', '1e2'], 'successes': 14}
        given_results_2 = {'errors': ['2e1', '2e2'], 'successes': 3}
        results_count = 2
        self.queue_meta.save_work_results(worker_id, given_results_1.copy())
        self.queue_meta.save_work_results(worker_id, given_results_2.copy())
        # pylint: disable=protected-access
        worker_res_key = '{}:{}:{}'.format(
            self.queue_meta._key_worker_results,
            worker_id,
            results_count,
        )
        got_results = self.queue_meta._client.hgetall(worker_res_key)
        got_results['successes'] = int(got_results['successes'])
        got_results['errors'] = int(got_results['errors'])
        given_results_2['errors'] = len(given_results_2['errors'])
        self.assertDictEqual(got_results, given_results_2)
        worker_conn = self.queue_meta._get_worker_conn(worker_id)
        self.assertEqual(int(worker_conn['results_count']), results_count)

    # Uuids
    def test_get_uuid_count(self):
        '''Test get_uuid_count'''
        uuid_count = 1047
        # pylint: disable=protected-access
        self.queue_meta._client.set(self.queue_meta._key_uuidcount, uuid_count)
        self.assertTrue(self.queue_meta.get_uuid_count() == uuid_count)

    def test_get_uuid_count_empty(self):
        '''Test get_uuid_count with no uuids'''
        uuid_count = 0
        self.assertTrue(self.queue_meta.get_uuid_count() == uuid_count)

    def test_has_uuids_added(self):
        '''Test has_uuids when only added'''
        added_cnt = 11
        success_cnt = 0
        errors_cnt = 0
        # pylint: disable=protected-access
        self.queue_meta._client.set(self.queue_meta._key_addedcount, added_cnt)
        self.queue_meta._client.set(self.queue_meta._key_successescount, success_cnt)
        self.queue_meta._client.set(self.queue_meta._key_errorscount, errors_cnt)
        self.assertTrue(self.queue_meta.has_uuids())

    def test_has_uuids_success(self):
        '''Test has_uuids when all success'''
        added_cnt = 11
        success_cnt = added_cnt
        errors_cnt = 0
        # pylint: disable=protected-access
        self.queue_meta._client.set(self.queue_meta._key_addedcount, added_cnt)
        self.queue_meta._client.set(self.queue_meta._key_successescount, success_cnt)
        self.queue_meta._client.set(self.queue_meta._key_errorscount, errors_cnt)
        self.assertFalse(self.queue_meta.has_uuids())

    def test_has_uuids_errors(self):
        '''Test has_uuids when all errors'''
        added_cnt = 11
        success_cnt = 0
        errors_cnt = added_cnt
        # pylint: disable=protected-access
        self.queue_meta._client.set(self.queue_meta._key_addedcount, added_cnt)
        self.queue_meta._client.set(self.queue_meta._key_successescount, success_cnt)
        self.queue_meta._client.set(self.queue_meta._key_errorscount, errors_cnt)
        self.assertFalse(self.queue_meta.has_uuids())

    def test_has_uuids_parterr(self):
        '''Test has_uuids when some errors'''
        added_cnt = 11
        success_cnt = 5
        errors_cnt = 6
        # pylint: disable=protected-access
        self.queue_meta._client.set(self.queue_meta._key_addedcount, added_cnt)
        self.queue_meta._client.set(self.queue_meta._key_successescount, success_cnt)
        self.queue_meta._client.set(self.queue_meta._key_errorscount, errors_cnt)
        self.assertFalse(self.queue_meta.has_uuids())

    def test_has_uuids_parterr_run(self):
        '''Test has_uuids when some errors and still running'''
        added_cnt = 11
        success_cnt = 3
        errors_cnt = 5
        # pylint: disable=protected-access
        self.queue_meta._client.set(self.queue_meta._key_addedcount, added_cnt)
        self.queue_meta._client.set(self.queue_meta._key_successescount, success_cnt)
        self.queue_meta._client.set(self.queue_meta._key_errorscount, errors_cnt)
        self.assertTrue(self.queue_meta.has_uuids())

    def test_update_uuid_count(self):
        '''Test update_uuid_count'''
        update_cnt = 5
        self.queue_meta.update_uuid_count(update_cnt)
        # pylint: disable=protected-access
        self.assertTrue(
            int(self.queue_meta._client.get(self.queue_meta._key_uuidcount)) == 5
        )
        update_cnt = -3
        self.queue_meta.update_uuid_count(update_cnt)
        self.assertTrue(
            int(self.queue_meta._client.get(self.queue_meta._key_uuidcount)) == 2
        )
        update_cnt = 8
        self.queue_meta.update_uuid_count(update_cnt)
        self.assertTrue(
            int(self.queue_meta._client.get(self.queue_meta._key_uuidcount)) == 10
        )

    # Run Args
    @staticmethod
    def test_setup_redis_keys():
        '''
        Check all redis keys on class
        - Tested in init
        '''
        pass

    def test_set_args(self):
        '''test set_args'''
        # pylint: disable=protected-access
        self.queue_meta._client.set(self.queue_meta._key_addedcount, 10)
        errors_to_add = self.standard_errors.copy()
        _ = self.queue_meta.add_errors(errors_to_add)
        self.queue_meta._client.set(self.queue_meta._key_successescount, 10)
        worker_id = 'test-worker-id-a'
        self.queue_meta.add_worker_conn(worker_id)
        self.queue_meta.set_args()
        self.assertEqual(
            int(self.queue_meta._client.get(
                self.queue_meta._key_addedcount
            )),
            0
        )
        self.assertEqual(
            int(self.queue_meta._client.get(
                self.queue_meta._key_errorscount
            )),
            0
        )
        self.assertEqual(
            int(self.queue_meta._client.get(
                self.queue_meta._key_successescount
            )),
            0
        )
        self.assertEqual(
            len(self.queue_meta._client.lrange(
                self.queue_meta._key_workers, 0, -1
            )),
            1
        )

    def test_get_run_args(self):
        '''test get_run_args'''
        run_args = {
            'batch_by': 101,
            'restart': False,
            'snapshot_id': 'fake-snap-id',
            'uuid_len': 34,
            'xmin': 1234,
        }
        self.queue_meta.set_run_args(run_args)
        res_run_args = self.queue_meta.get_run_args()
        self.assertTrue('batch_by' in res_run_args)
        self.assertIsInstance(res_run_args['batch_by'], int)
        self.assertTrue('restart' in res_run_args)
        self.assertIsInstance(res_run_args['restart'], bool)
        self.assertTrue('uuid_len' in res_run_args)
        self.assertIsInstance(res_run_args['uuid_len'], int)
        self.assertTrue('xmin' in res_run_args)
        self.assertIsInstance(res_run_args['xmin'], int)
        self.assertDictEqual(res_run_args, run_args)

    @staticmethod
    def test_set_run_args():
        '''
        test set_run_args
        - done in test_get_run_args
        '''
        pass


class TestRedisQueue(TestCase):
    '''Test Redis Queue'''
    # pylint: disable=too-many-public-methods
    members = [
        ('add_str', str),
        ('get_str', str),
        ('len_str', str),
        ('max_value_size', int),
        ('queue_name', str),
        ('queue_type', str),
        ('_client', RedisClient),
        ('_qmeta', RedisQueueMeta),
    ]
    meth_func = [
        '_call_func',
        # Errors
        'add_errors',
        'has_errors',
        # Worker
        'add_worker_conn',
        'get_worker_conns',
        'get_worker_conn_count',
        'save_work_results',
        'update_worker_conn',
        # Uuids
        'has_uuids',
        '_get_uuid',
        'get_uuids',
        '_load_uuid',
        'load_uuids',
        'pop_errors',
        'update_uuid_count',
        # Run Args
        'update_finished',
        'close_indexing', # not tested
        'get_server_restarts', # not tested
        'update_errors_count', # not tested
        'update_success_count', # not tested
    ]

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        cls.queue_type = REDIS_LIST
        cls.queue_options = DEFAULT_QUEUE_OPTS.copy()
        cls.client = RedisClient(cls.queue_options)
        cls.mock_time_us = int(MOCK_TIME * 1000000)
        cls.queue = cls.client.get_queue(
            cls.queue_options['queue_name'],
            cls.queue_type
        )
        # pylint: disable=protected-access
        for key in cls.queue._qmeta._client.keys('*'):
            cls.queue._qmeta._client.delete(key)

    @classmethod
    def tearDownClass(cls):
        '''Clean up after running test class'''
        # pylint: disable=protected-access
        for key in cls.queue._qmeta._client.keys('*'):
            cls.queue._qmeta._client.delete(key)

    def setUp(self):
        '''Setup before each method call'''
        pass

    def tearDown(self):
        """Clean up after each method call"""
        # pylint: disable=protected-access
        self.queue._qmeta.pop_errors()
        self.queue._qmeta._client.delete(self.queue._qmeta._key_errorscount)
        worker_ids = self.queue._qmeta._client.lrange(
            self.queue._qmeta._key_workers, 0, -1
        )
        self.queue._qmeta._client.delete(self.queue._qmeta._key_workers)
        for worker_id in worker_ids:
            self.queue._qmeta._client.delete(
                self.queue._qmeta._key_worker_conn + ':' + worker_id
            )
        queue_meta_worker_keys = self.queue._qmeta._client.keys(
            self.queue._qmeta._key_worker_results + ':*'
        )
        for key in queue_meta_worker_keys:
            self.queue._qmeta._client.delete(key)
        self.queue._qmeta._client.delete(self.queue._qmeta._key_uuidcount)
        self.queue._client.delete(self.queue.queue_name)

    # private/protected
    def test_init_dir(self):
        '''Test RedisQueue has expected function and variables'''
        dir_items = [
            item
            for item in dir(self.queue)
            if item[0:2] != '__'
        ]
        member_names = [name for name, _ in self.members]
        for item in dir_items:
            if item in member_names:
                continue
            if item in self.meth_func:
                continue
            raise AssertionError('Member or method not being tested:%s' % item)
        for item, item_type in self.members:
            self.assertTrue(hasattr(self.queue, item))
            self.assertIsInstance(
                getattr(self.queue, item),
                item_type
            )
        for item in self.meth_func:
            self.assertTrue(hasattr(self.queue, item))
            self.assertTrue(
                str(type(getattr(self.queue, item))) in [
                    "<class 'method'>",
                    "<class 'function'>"
                ]
            )

    def test_init_basic(self):
        '''Test basic queue meta init'''
        self.assertEqual(
            self.queue.queue_name,
            self.queue_options['queue_name'] + '0'
        )
        # pylint: disable=protected-access
        self.assertTrue(self.queue._client is self.client)
        assert True

    def test_get_uuid(self):
        '''Test _get_uuid'''
        uuids_to_add = ['a', 'b', 'c']
        # pylint: disable=protected-access
        for uuid in uuids_to_add:
            self.queue._client.lpush(self.queue.queue_name, uuid)
        ret_uuids = self.queue._get_uuid()
        self.assertEqual(ret_uuids, uuids_to_add[-1])

    def test_load_uuid(self):
        '''Test _load_uuid'''
        uuids_to_add = ['a', 'b', 'c']
        res_bool = self.queue.load_uuids(uuids_to_add.copy())
        self.assertTrue(res_bool)
        # pylint: disable=protected-access
        ret_uuids = self.queue._client.lrange(self.queue.queue_name, 0, -1)
        ret_uuids.sort()
        uuids_to_add.sort()
        self.assertListEqual(ret_uuids, uuids_to_add)
