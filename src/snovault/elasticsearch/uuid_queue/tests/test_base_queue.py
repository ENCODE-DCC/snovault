"""Test Base Queue File"""
from unittest import (
    TestCase,
    mock,
)

from ..queues.base_queue import (
    BASE_QUEUE_TYPE,
    BaseQueueClient,
    BaseQueueMeta,
    BaseQueue,
)


MOCK_TIME = 123456.654321


class TestBaseClient(TestCase):
    '''Test Base Queue Client Class'''
    queue_options = {}
    queue_name = 'base-test-queue'
    queue_type = BASE_QUEUE_TYPE
    def test_get_queue(self):
        '''Test get_queue'''
        client = BaseQueueClient(self.queue_options)
        queue = client.get_queue(self.queue_name, self.queue_type)
        self.assertIsInstance(queue, BaseQueue)

    def test_get_queue_bad_type(self):
        '''Test get_queue with bad queue type'''
        client = BaseQueueClient(self.queue_options)
        try:
            _ = client.get_queue(self.queue_name, 'FakeType')
        except ValueError:
            pass
        else:
            self.fail('ValueError expected')


class TestBaseQueueMeta(TestCase):
    '''Test Base Queue Meta Class'''
    # pylint: disable=too-many-public-methods
    members = [
        ('_base_id', int),
        ('_errors', list),
        ('_uuid_count', int),
        ('_worker_conns', dict),
        ('_worker_results', dict),
        ('queue_name', str),
        ('_added_count', int), # not tested
        ('_errors_count', int), # not tested
        ('_success_count', int), # not tested
    ]
    meth_func = [
        # Errors
        'add_errors',
        'has_errors',
        'pop_errors',
        # Worker
        '_get_blank_worker',
        'add_worker_conn',
        'get_worker_conns',
        'get_worker_conn_count',
        'update_worker_conn',
        'save_work_results',
        # Uuids
        'has_uuids',
        'update_uuid_count',
        'get_server_restarts', # not tested
        'update_errors_count', # not tested
        'update_success_count', # not tested
    ]

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        cls.mock_time_us = int(MOCK_TIME * 1000000)
        cls.queue_name = 'test_base_queueb'
        cls.queue_meta = BaseQueueMeta(cls.queue_name)
        cls.standard_errors = ['e' + str(cnt) for cnt in range(10)]

    @classmethod
    def tearDownClass(cls):
        '''Clean up after running test class'''
        pass

    def setUp(self):
        '''Setup before each method call'''
        pass

    def tearDown(self):
        """Clean up after each method call"""
        # pylint: disable=protected-access
        self.queue_meta._errors = []
        self.queue_meta._worker_results = {}
        self.queue_meta._worker_conns = {}

    # private/protected
    def test_init_dir(self):
        '''Test SimpleUuidServer has expected function and variables'''
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
        self.assertListEqual([], self.queue_meta._errors)
        self.assertEqual(0, self.queue_meta._uuid_count)
        self.assertDictEqual({}, self.queue_meta._worker_conns)
        self.assertDictEqual({}, self.queue_meta._worker_results)

    # Errors
    def test_add_errors(self):
        '''Test add_errors'''
        errors_to_add = self.standard_errors.copy()
        expected_result = len(errors_to_add)
        result = self.queue_meta.add_errors(errors_to_add)
        self.assertEqual(expected_result, result)
        errors_to_add.sort()
        # pylint: disable=protected-access
        self.queue_meta._errors.sort()
        self.assertListEqual(errors_to_add, self.queue_meta._errors)

    def test_add_errors_none(self):
        '''Test add_errors when no errors'''
        errors_to_add = []
        expected_result = len(errors_to_add)
        result = self.queue_meta.add_errors(errors_to_add)
        self.assertEqual(expected_result, result)
        errors_to_add.sort()
        # pylint: disable=protected-access
        self.queue_meta._errors.sort()
        self.assertListEqual(errors_to_add, self.queue_meta._errors)

    def test_has_errors(self):
        '''Test has_errors'''
        # pylint: disable=protected-access
        self.queue_meta._errors = self.standard_errors.copy()
        self.assertTrue(self.queue_meta.has_errors())

    def test_has_errors_none(self):
        '''Test has_errors when no errors'''
        # pylint: disable=protected-access
        self.assertFalse(self.queue_meta.has_errors())

    def test_pop_errors(self):
        '''Test pop_errors'''
        # pylint: disable=protected-access
        errors_to_add = self.standard_errors.copy()
        self.queue_meta._errors = errors_to_add
        result = self.queue_meta.pop_errors()
        self.standard_errors.sort()
        result.sort()
        self.assertListEqual(self.standard_errors, result)
        self.assertListEqual([], self.queue_meta._errors)

    # Workers
    def test_get_blank_worker(self):
        '''Test _get_blank_worker'''
        # pylint: disable=protected-access
        self.assertDictEqual(
            {
                'uuid_cnt': 0,
                'get_cnt': 0,
            },
            self.queue_meta._get_blank_worker()
        )

    def test_add_worker_conn(self):
        '''Test add_worker_conn'''
        worker_id = 'test-worker-id'
        self.queue_meta.add_worker_conn(worker_id)
        # pylint: disable=protected-access
        self.assertTrue(worker_id in self.queue_meta._worker_results)
        self.assertListEqual([], self.queue_meta._worker_results[worker_id])
        self.assertTrue(worker_id in self.queue_meta._worker_conns)
        self.assertDictEqual(
            {
                'uuid_cnt': 0,
                'get_cnt': 0,
            },
            self.queue_meta._worker_conns[worker_id]
        )

    def test_get_worker_conns(self):
        '''Test get_worker_conns'''
        # pylint: disable=protected-access
        self.assertEqual(
            id(self.queue_meta._worker_conns),
            id(self.queue_meta.get_worker_conns())
        )

    def test_get_worker_conn_count(self):
        '''Test get_worker_conn_count'''
        # pylint: disable=protected-access
        self.assertEqual(
            len(self.queue_meta._worker_conns),
            self.queue_meta.get_worker_conn_count()
        )
        self.queue_meta._worker_conns['fake-id'] = {}
        self.assertEqual(
            len(self.queue_meta._worker_conns),
            self.queue_meta.get_worker_conn_count()
        )

    def test_update_worker_conn(self):
        '''Test update_worker_conn'''
        # pylint: disable=protected-access
        self.assertEqual(
            id(self.queue_meta._worker_conns),
            id(self.queue_meta.get_worker_conns())
        )


class TestBaseQueue(TestCase):
    '''Test Base Queue Class'''
    # No explict tests for the following since they pass directly to qmeta
    #  if that changes then add a test here.
    # - has_errors, pop_errors
    # - add_worker_conn, get_worker_conns, get_worker_conn_count,
    # update_worker_conn, save_work_results
    # - has_uuids, update_uuid_count

    # pylint: disable=too-many-public-methods
    members = [
        ('max_value_size', int),
        ('queue_type', str),
        ('_uuids', list),
        ('_qmeta', BaseQueueMeta),
        ('queue_name', str),
    ]
    meth_func = [
        # Errors
        'add_errors',
        'has_errors',
        'pop_errors',
        # Worker
        'add_worker_conn',
        'get_worker_conns',
        'get_worker_conn_count',
        'update_worker_conn',
        'save_work_results',
        # Uuids
        'has_uuids',
        'update_uuid_count',
        '_get_uuid',
        'get_uuids',
        '_load_uuid',
        'load_uuids',
        'update_finished',
        'close_indexing', # not tested
        'get_server_restarts', # not tested
        'update_errors_count', # not tested
        'update_success_count', # not tested
    ]

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        cls.queue_options = {}
        cls.queue_name = 'base-test-queue'
        cls.queue_type = BASE_QUEUE_TYPE
        cls.mock_time_us = int(MOCK_TIME * 1000000)
        cls.client = BaseQueueClient(cls.queue_options)
        cls.queue = cls.client.get_queue(cls.queue_name, cls.queue_type)
        cls.standard_errors = ['e' + str(cnt) for cnt in range(10)]

    @classmethod
    def tearDownClass(cls):
        '''Clean up after running test class'''
        pass

    def setUp(self):
        '''Setup before each method call'''
        pass

    def tearDown(self):
        """Clean up after each method call"""
        # pylint: disable=protected-access
        self.queue._qmeta._errors = []
        self.queue._qmeta._worker_results = {}
        self.queue._qmeta._worker_conns = {}
        self.queue._uuids = []

    # private/protected
    def test_init_dir(self):
        '''Test SimpleUuidServer has expected function and variables'''
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
        # pylint: disable=protected-access
        self.assertListEqual(self.queue._uuids, [])
        self.assertIsInstance(self.queue._qmeta, BaseQueueMeta)

    # Errors
    def test_add_errors(self):
        '''Test add_errors'''
        worker_id = 'wrkid-a'
        errors_to_add = [
            {'uuid': cnt, 'msg': 'err' + str(cnt)}
            for cnt in range(1, 4)
        ]
        res_err_cnt = self.queue.add_errors(worker_id, errors_to_add)
        self.assertEqual(res_err_cnt, len(errors_to_add))
        exp_sorted_errors = sorted(errors_to_add, key=lambda k: k['uuid'])
        # pylint: disable=protected-access
        qmeta_errors = self.queue._qmeta._errors
        res_sorted_errors = sorted(qmeta_errors, key=lambda k: k['uuid'])
        self.assertListEqual(exp_sorted_errors, res_sorted_errors)

    # Uuids
    def test_get_uuid(self):
        '''Test _get_uuid'''
        # pylint: disable=protected-access
        uuids_to_add = ['a', 'b', 'c']
        self.queue._uuids = uuids_to_add.copy()
        res_uuid = self.queue._get_uuid()
        self.assertEqual(res_uuid, uuids_to_add[-1])
        self.assertEqual(len(self.queue._uuids), len(uuids_to_add) - 1)

    def test_get_uuid_none(self):
        '''Test _get_uuid when no uuids'''
        # pylint: disable=protected-access
        res_uuid = self.queue._get_uuid()
        self.assertIsNone(res_uuid)

    def test_get_uuids_allshort(self):
        '''Test get_uuids all uuids with short cut'''
        uuids_to_add = ['a', 'b', 'c', 'd', 'e']
        # pylint: disable=protected-access
        self.queue._uuids = uuids_to_add.copy()
        res_uuids = self.queue.get_uuids(-10)
        uuids_to_add.sort()
        res_uuids.sort()
        self.assertListEqual(uuids_to_add, res_uuids)

    def test_get_uuids_overcnt(self):
        '''Test get_uuids all uuids over count'''
        uuids_to_add = ['a', 'b', 'c', 'd', 'e']
        # pylint: disable=protected-access
        self.queue._uuids = uuids_to_add.copy()
        res_uuids = self.queue.get_uuids(len(uuids_to_add) + 5)
        uuids_to_add.sort()
        res_uuids.sort()
        self.assertListEqual(uuids_to_add, res_uuids)

    def test_get_uuids_undercnt(self):
        '''Test get_uuids uuids with count'''
        uuids_to_add = ['a', 'b', 'c', 'd', 'e']
        # pylint: disable=protected-access
        self.queue._uuids = uuids_to_add.copy()
        get_cnt = len(uuids_to_add) - 2
        res_uuids = self.queue.get_uuids(get_cnt)
        self.assertEqual(get_cnt, len(res_uuids))
        exp_uuids = uuids_to_add[-1 * get_cnt:]
        exp_uuids.sort()
        res_uuids.sort()
        self.assertListEqual(exp_uuids, res_uuids)

    def test_get_uuids_none(self):
        '''Test get_uuids when no uuids'''
        res_uuids = self.queue.get_uuids(5)
        self.assertListEqual(res_uuids, [])

    def test_load_uuid(self):
        '''Test _load_uuid'''
        uuid_to_add = 'a'
        # pylint: disable=protected-access
        self.queue._load_uuid(uuid_to_add)
        self.assertListEqual(self.queue._uuids, [uuid_to_add])

    def test_load_uuid_many(self):
        '''Test _load_uuid many uuids, order should be reversed'''
        uuids_to_add = ['a', 'b', 'c', 'd', 'e']
        # pylint: disable=protected-access
        for uuid_to_add in uuids_to_add:
            self.queue._load_uuid(uuid_to_add)
        uuids_to_add.reverse()
        self.assertListEqual(self.queue._uuids, uuids_to_add)

    def test_load_uuids(self):
        '''Test load_uuids'''
        uuids_to_add = ['a', 'b', 'c', 'd', 'e']
        res_bytes_added, res_failed_uuids = self.queue.load_uuids(uuids_to_add)
        self.assertEqual(len(uuids_to_add), res_bytes_added)
        self.assertListEqual(res_failed_uuids, [])
        uuids_to_add.reverse()
        # pylint: disable=protected-access
        self.assertListEqual(self.queue._uuids, uuids_to_add)

    def test_load_uuids_none(self):
        '''Test load_uuids with none'''
        uuids_to_add = []
        res_bytes_added, res_failed_uuids = self.queue.load_uuids(uuids_to_add)
        self.assertEqual(len(uuids_to_add), res_bytes_added)
        self.assertListEqual(res_failed_uuids, [])
        uuids_to_add.reverse()
        # pylint: disable=protected-access
        self.assertListEqual(self.queue._uuids, uuids_to_add)

    def test_update_finished(self):
        '''Test update_finished'''
        worker_id = 'wrkid-b'
        uuid_cnt = 5
        get_cnt = 2
        self.queue.add_worker_conn(worker_id)
        self.queue.update_worker_conn(worker_id, uuid_cnt, get_cnt)
        results = {'errors': [], 'successes': uuid_cnt}
        res_msg = self.queue.update_finished(worker_id, results)
        exp_msg = 'Okay'
        self.assertEqual(res_msg, exp_msg)
        updated_worker_conn = self.queue.get_worker_conns()[worker_id]
        self.assertEqual(updated_worker_conn['uuid_cnt'], 0)
        self.assertEqual(updated_worker_conn['get_cnt'], get_cnt)
        # pylint: disable=protected-access
        worker_id_results = self.queue._qmeta._worker_results[worker_id]
        self.assertEqual(1, len(worker_id_results))
        self.assertDictEqual(results, worker_id_results[0])

    def test_update_finished_nowrks(self):
        '''Test update_finished with no workers'''
        worker_id = 'wrkid-c'
        results = {}
        res_msg = self.queue.update_finished(worker_id, results)
        exp_msg = 'Worker id(%s) DNE.' % worker_id
        self.assertEqual(res_msg, exp_msg)
        self.assertDictEqual(self.queue.get_worker_conns(), {})
        # pylint: disable=protected-access
        self.assertDictEqual(self.queue._qmeta._worker_results, {})

    def test_update_finished_errs(self):
        '''Test update_finished with errs'''
        worker_id = 'wrkid-d'
        uuid_cnt = 5
        get_cnt = 4
        errors_cnt = uuid_cnt - 3
        errors = [
            {'uuid': cnt, 'msg': 'err' + str(cnt)}
            for cnt in range(1, errors_cnt + 1)
        ]
        self.queue.add_worker_conn(worker_id)
        self.queue.update_worker_conn(worker_id, uuid_cnt, get_cnt)
        results = {'errors': errors, 'successes': uuid_cnt - errors_cnt}
        res_msg = self.queue.update_finished(worker_id, results)
        exp_msg = 'Okay'
        self.assertEqual(res_msg, exp_msg)
        updated_worker_conn = self.queue.get_worker_conns()[worker_id]
        self.assertEqual(updated_worker_conn['uuid_cnt'], 0)
        self.assertEqual(updated_worker_conn['get_cnt'], get_cnt)
        # pylint: disable=protected-access
        worker_id_results = self.queue._qmeta._worker_results[worker_id]
        self.assertEqual(1, len(worker_id_results))
        self.assertDictEqual(results, worker_id_results[0])

    def test_update_finished_noclose(self):
        '''Test update_finished cannot close worker'''
        worker_id = 'wrkid-d'
        uuid_cnt = 5
        get_cnt = 4
        errors_cnt = uuid_cnt - 3
        errors = [
            {'uuid': cnt, 'msg': 'err' + str(cnt)}
            for cnt in range(1, errors_cnt + 1)
        ]
        self.queue.add_worker_conn(worker_id)
        self.queue.update_worker_conn(worker_id, uuid_cnt, get_cnt)
        results = {'errors': errors, 'successes': uuid_cnt}
        res_msg = self.queue.update_finished(worker_id, results)
        exp_msg = 'Queue Server cannot close worker (%s)' % worker_id
        self.assertEqual(res_msg, exp_msg)
        updated_worker_conn = self.queue.get_worker_conns()[worker_id]
        self.assertEqual(
            updated_worker_conn['uuid_cnt'],
            uuid_cnt - len(errors) - uuid_cnt,  # final uuid_cnt is successes
        )
        self.assertEqual(updated_worker_conn['get_cnt'], get_cnt)
        # pylint: disable=protected-access
        worker_id_results = self.queue._qmeta._worker_results[worker_id]
        self.assertEqual(1, len(worker_id_results))
        self.assertDictEqual(results, worker_id_results[0])
