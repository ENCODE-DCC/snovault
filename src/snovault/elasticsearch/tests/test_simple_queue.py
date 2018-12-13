''''
Test the functionality of SimpleUuidServer and SimpleUuidWorker
in the context of Indexer and MPIndexer
'''
from unittest import (
    TestCase,
    mock,
)

from snovault.elasticsearch.simple_queue import (
    SimpleUuidServer,
    SimpleUuidWorker,
)


MOCK_TIME = 123456.654321
QUEUE_RESET_MSG_BASE = 'Queue Server cannot reset. '


class TestSimpleServer(TestCase):
    '''Test Simple Uuid Server'''
    # pylint: disable=too-many-public-methods
    members = [
        ('_queue_options', dict),
        ('_start_us', int),
        ('queue_id', str),
        ('_uuids', list),
        ('_uuid_count', int),
        ('_worker_conns', dict),
        ('_worker_results', dict),
        ('_errors', list),
    ]
    meth_func = [
        # Errors
        'add_errors',
        '_has_errors',
        'pop_errors',
        # Worker
        '_get_blank_worker',
        '_add_worker_conn',
        'get_worker_conns',
        'get_worker_conn_count',
        '_get_worker_id',
        '_get_worker_queue',
        'get_worker',
        'update_worker_conn',
        'save_work_results',
        # Uuids
        'has_uuids',
        '_get_uuid',
        'get_uuids',
        '_load_uuid',
        'load_uuids',
        # Run
        'is_indexing',
        'update_finished',
        'close_indexing',
    ]

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        '''Create a simple worker to be used most tests'''
        cls.load_uuids = list(range(10, 99))
        cls.queue_options = {
            'processes': 1,
            'chunk_size': 1,
            'batch_size': 10,
        }
        cls.mock_time_us = int(MOCK_TIME * 1000000)
        cls.queue_server = SimpleUuidServer(cls.queue_options)
        cls.queue_worker = None

    def setUp(self):
        '''Setup before each method call'''
        self.queue_worker = self.queue_server.get_worker()

    def tearDown(self):
        """Reset for each test"""
        self.queue_worker = None
        # pylint: disable=protected-access
        self.queue_server._uuids = []
        self.queue_server._uuid_count = 0
        self.queue_server._worker_conns = {}
        self.queue_server._worker_results = {}
        self.queue_server._errors = []

    # private/protected
    def test_init_dir(self):
        '''Test SimpleUuidServer has expected function and variables'''
        dir_items = [
            item
            for item in dir(self.queue_server)
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
            self.assertTrue(hasattr(self.queue_server, item))
            self.assertIsInstance(
                getattr(self.queue_server, item),
                item_type
            )
        for item in self.meth_func:
            self.assertTrue(hasattr(self.queue_server, item))
            self.assertTrue(
                str(type(getattr(self.queue_server, item))) in [
                    "<class 'method'>",
                    "<class 'function'>"
                ]
            )

    def test_init_basic(self):
        '''Test _get_worker_id'''
        self.assertEqual(str(self.mock_time_us), self.queue_server.queue_id)
        # pylint: disable=protected-access
        self.assertEqual(self.mock_time_us, self.queue_server._start_us)
        self.assertDictEqual(
            self.queue_options,
            self.queue_server._queue_options
        )

    # Errors
    def test_errors(self):
        '''Test add_errors'''
        worker_id = self.queue_worker.worker_id
        errors = ['e1', 'e2', 'e3']
        errors_to_add = [{'err': error} for error in errors]
        errors_added = self.queue_server.add_errors(worker_id, errors_to_add)
        self.assertEqual(errors_added, len(errors_to_add))
        error_msgs = []
        # pylint: disable=protected-access
        for error in self.queue_server._errors:
            self.assertTrue(error['worker_id'] == worker_id)
            error_msgs.append(error['err'])
        errors.sort()
        error_msgs.sort()
        self.assertListEqual(errors, error_msgs)

    def test_has_errors_true(self):
        '''Test _has_errors return true when not empty'''
        # pylint: disable=protected-access
        self.queue_server._errors = ['e']
        self.assertTrue(self.queue_server._has_errors())

    def test_has_errors_false(self):
        '''Test _has_errors returns false when empty'''
        # pylint: disable=protected-access
        self.assertFalse(self.queue_server._has_errors())

    def test_pop_errors(self):
        '''Test pop_errors'''
        # pylint: disable=protected-access
        self.assertFalse(self.queue_server._has_errors())

    # Worker
    def test_get_blank_worker(self):
        '''Test _get_blank_worker'''
        # pylint: disable=protected-access
        self.assertDictEqual(
            self.queue_server._get_blank_worker(),
            {'uuid_cnt': 0, 'get_cnt': 0}
        )

    def test_add_worker_conn(self):
        '''Test _add_worker_conn'''
        worker_id = 'some-id'
        # pylint: disable=protected-access
        self.queue_server._add_worker_conn(worker_id)
        self.assertTrue(worker_id in self.queue_server._worker_results)
        self.assertTrue(worker_id in self.queue_server._worker_conns)

    def test_get_worker_conns(self):
        '''Test get_worker_conns'''
        worker_id = self.queue_worker.worker_id
        worker_conns = self.queue_server.get_worker_conns()
        self.assertTrue(len(worker_conns) == 1)
        self.assertTrue(worker_id in worker_conns)
        # pylint: disable=protected-access
        self.assertDictEqual(
            worker_conns[worker_id],
            self.queue_server._get_blank_worker(),
        )

    def test_get_worker_conn_count(self):
        '''Test get_worker_conn_count'''
        self.assertTrue(
            self.queue_server.get_worker_conn_count(),
            1
        )

    def test_get_worker_id(self):
        '''Test _get_worker_id'''
        # Plus two since a worker is added in setUp
        id_time_us = self.mock_time_us + 2
        # pylint: disable=protected-access
        self.assertTrue(
            self.queue_server._get_worker_id() == str(id_time_us)
        )
        _ = self.queue_server._get_worker_id()
        # The function does not add the worker to the conn
        # so the id should not change
        self.assertTrue(
            self.queue_server._get_worker_id() == str(id_time_us)
        )

    def test_get_worker_queue(self):
        '''Test _get_worker_queue'''
        # pylint: disable=protected-access
        self.assertTrue(self.queue_server is self.queue_server._get_worker_queue())

    def test_get_worker_return(self):
        '''Test get_worker return value'''
        self.assertIsInstance(
            self.queue_server.get_worker(),
            SimpleUuidWorker
        )

    def test_get_worker_conn(self):
        '''Test get_worker worker connection entry'''
        worker = self.queue_server.get_worker()
        # pylint: disable=protected-access
        self.assertEqual(2, len(self.queue_server._worker_conns))
        worker_conn = self.queue_server._worker_conns.get(worker.worker_id)
        self.assertFalse(worker_conn is None)
        self.assertDictEqual(
            worker_conn,
            {
                'uuid_cnt': 0,
                'get_cnt': 0,
            }
        )

    def test_get_worker_many(self):
        '''Test get_worker with 2 workers'''
        worker_2 = self.queue_server.get_worker()
        worker_3 = self.queue_server.get_worker()
        # pylint: disable=protected-access
        self.assertEqual(len(self.queue_server._worker_conns), 3)
        self.assertTrue(worker_2.worker_id == str(self.mock_time_us + 2))
        self.assertTrue(worker_3.worker_id == str(self.mock_time_us + 3))

    def test_update_worker_conn(self):
        '''Test update_worker_conn'''
        update_uuid_cnt = 9
        update_get_cnt = 3
        worker_id = self.queue_worker.worker_id
        # pylint: disable=protected-access
        worker_conn = self.queue_server._worker_conns[worker_id]
        self.queue_server.update_worker_conn(
            worker_id,
            update_uuid_cnt,
            update_get_cnt,
        )
        self.assertDictEqual(
            worker_conn,
            {
                'uuid_cnt': update_uuid_cnt,
                'get_cnt': update_get_cnt
            }
        )

    def test_save_work_results(self):
        '''Test save_work_results'''
        worker_id = self.queue_worker.worker_id
        save_res = {'res': 'some-res'}
        self.queue_server.save_work_results(worker_id, save_res)
        # pylint: disable=protected-access
        self.assertTrue(len(self.queue_server._worker_results) == 1)
        self.assertTrue(worker_id in self.queue_server._worker_results)
        self.assertDictEqual(
            self.queue_server._worker_results[worker_id][0],
            save_res,
        )

    # Uuids
    def test_has_uuids(self):
        '''Test has_uuids'''
        # pylint: disable=protected-access
        self.queue_server._uuids = [1, 2, 3]
        self.assertTrue(self.queue_server.has_uuids())

    def test_has_uuids_empty(self):
        '''Test has_uuids when empty'''
        self.assertFalse(self.queue_server.has_uuids())

    def test_get_uuid(self):
        '''Test _get_uuid'''
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        uuid = self.queue_server._get_uuid()
        self.assertEqual(uuid, self.load_uuids[0])

    def test_get_uuid_empty(self):
        '''Test _get_uuid with empty'''
        # pylint: disable=protected-access
        self.assertIsNone(self.queue_server._get_uuid())

    def test_get_uuids(self):
        '''Test get_uuids'''
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        expected_uuids = self.load_uuids[0:uuid_cnt]
        uuids = self.queue_server.get_uuids(uuid_cnt)
        self.assertListEqual(expected_uuids, uuids)

    def test_get_uuids_all(self):
        '''Test get_uuids all with new worker connection'''
        uuid_cnt = -1
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        uuids = self.queue_server.get_uuids(uuid_cnt)
        self.load_uuids.sort()
        uuids.sort()
        self.assertListEqual(self.load_uuids, uuids)

    def test_load_uuid(self):
        '''Test _load_uuid'''
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        new_uuid = 'a-uuid'
        self.queue_server._load_uuid(new_uuid)
        self.assertEqual(new_uuid, self.queue_server._uuids[0])
        self.assertEqual(len(self.load_uuids) + 1, len(self.queue_server._uuids))

    def test_load_uuids(self):
        '''Test load_uuids'''
        len_load_uuids = self.queue_server.load_uuids(self.load_uuids.copy())
        self.assertEqual(len(self.load_uuids), len_load_uuids)
        # pylint: disable=protected-access
        self.assertListEqual(
            self.queue_server._uuids,
            list(reversed(self.load_uuids))
        )

    def test_load_uuids_full(self):
        '''Test load_uuids when has uuids already'''
        # pylint: disable=protected-access
        previous_uuids = ['1']
        self.queue_server._uuids = previous_uuids
        len_load_uuids = self.queue_server.load_uuids(self.load_uuids.copy())
        self.assertListEqual(
            previous_uuids,
            self.queue_server._uuids,
        )
        self.assertIsNone(len_load_uuids)

    # Run
    def test_is_indexing_false_all(self):
        '''
        Test is_indexing is false
        -No uuids, no errors, and worker uuid cnt is 0
        '''
        self.assertFalse(self.queue_server.is_indexing())

    def test_is_indexing_true_uuidcnt(self):
        '''
        Test is_indexing is true when work uuid cnt is not 0
        '''
        worker_id = self.queue_worker.worker_id
        # pylint: disable=protected-access
        self.queue_server._worker_conns[worker_id]['uuid_cnt'] = 1
        self.assertTrue(self.queue_server.is_indexing())

    def test_is_indexing_true_haserrors(self):
        '''
        Test is_indexing is true when has errors is true
        '''
        # pylint: disable=protected-access
        self.queue_server._errors = [1]
        self.assertTrue(self.queue_server.is_indexing())

    def test_is_indexing_true_hasuuids(self):
        '''
        Test is_indexing is true when has uuids is true
        '''
        # pylint: disable=protected-access
        self.queue_server._uuids = [1]
        self.assertTrue(self.queue_server.is_indexing())

    def test_update_finished_nowrkid(self):
        '''Test update_finished with bad worker id'''
        worker_id = 'bad-id'
        uuid_cnt = 11
        results = {'errors': [], 'successes': uuid_cnt}
        res_msg = self.queue_server.update_finished(worker_id, results)
        self.assertEqual(
            res_msg,
            "Worker id(%s) DNE." % worker_id
        )

    def test_update_finished_cntoffsucc(self):
        '''Test update_finished with success count off'''
        worker_id = self.queue_worker.worker_id
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._worker_conns[worker_id]['uuid_cnt'] = uuid_cnt
        results = {'errors': [], 'successes': uuid_cnt - 1}
        res_msg = self.queue_server.update_finished(worker_id, results)
        self.assertEqual(
            res_msg,
            'Queue Server cannot close worker (%s)' % worker_id
        )

    def test_update_finished_cntofferr(self):
        '''Test update_finished with errors count off'''
        worker_id = self.queue_worker.worker_id
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._worker_conns[worker_id]['uuid_cnt'] = uuid_cnt
        errors = [{'err': str(cnt)} for cnt in range(uuid_cnt + 1)]
        results = {'errors': errors, 'successes': 0}
        res_msg = self.queue_server.update_finished(worker_id, results)
        self.assertEqual(
            res_msg,
            'Queue Server cannot close worker (%s)' % worker_id
        )

    def test_update_finished_cntoffboth(self):
        '''Test update_finished with errors and success count off'''
        worker_id = self.queue_worker.worker_id
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._worker_conns[worker_id]['uuid_cnt'] = uuid_cnt
        errors = [{'err': str(cnt)} for cnt in range(uuid_cnt - 3)]
        results = {'errors': errors, 'successes': 0}
        res_msg = self.queue_server.update_finished(worker_id, results)
        self.assertEqual(
            res_msg,
            'Queue Server cannot close worker (%s)' % worker_id
        )

    def test_update_finished_okay_succ(self):
        '''Test update_finished with success okay msg'''
        worker_id = self.queue_worker.worker_id
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._worker_conns[worker_id]['uuid_cnt'] = uuid_cnt
        results = {'errors': [], 'successes': uuid_cnt}
        res_msg = self.queue_server.update_finished(worker_id, results)
        self.assertEqual(
            res_msg,
            'Okay'
        )

    def test_update_finished_okayerrs(self):
        '''Test update_finished with errors okay msg'''
        worker_id = self.queue_worker.worker_id
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._worker_conns[worker_id]['uuid_cnt'] = uuid_cnt
        errors = [{'err': str(cnt)} for cnt in range(uuid_cnt)]
        results = {'errors': errors, 'successes': 0}
        res_msg = self.queue_server.update_finished(worker_id, results)
        self.assertEqual(
            res_msg,
            'Okay'
        )

    def test_update_finished_okerrsucc(self):
        '''Test update_finished with errors and success okay msg'''
        worker_id = self.queue_worker.worker_id
        uuid_cnt = 11
        err_cnt = 4
        # pylint: disable=protected-access
        self.queue_server._worker_conns[worker_id]['uuid_cnt'] = uuid_cnt
        errors = [{'err': str(cnt)} for cnt in range(err_cnt)]
        results = {'errors': errors, 'successes': uuid_cnt - err_cnt}
        res_msg = self.queue_server.update_finished(worker_id, results)
        self.assertEqual(
            res_msg,
            'Okay'
        )


class TestSimpleUuidWorker(TestCase):
    '''Test Simple Uuid worker'''
    members = [
        ('queue_options', dict),
        ('worker_id', str),
        ('_queue', SimpleUuidServer),
        ('is_running', bool),
        ('get_cnt', int),
        ('uuid_cnt', int),
        ('processes', int),
        ('chunk_size', int),
    ]
    methods = [
        'get_uuids',
        'update_finished',
    ]

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        '''Create a simple worker to be used most tests'''
        queue_options = {
            'processes': 1,
            'chunk_size': 1,
            'batch_size': 10,
        }
        cls.queue_options = queue_options
        cls.mock_server = SimpleUuidServer(queue_options)
        uuids_loaded = [str(uuid) for uuid in range(100, 150)]
        cls.mock_return_uuids = uuids_loaded[0:queue_options['batch_size']]
        cls.mock_server.get_uuids = mock.MagicMock(
            return_value=cls.mock_return_uuids
        )
        cls.mock_server.update_finished = mock.MagicMock(return_value=None)
        cls.queue_worker = cls.mock_server.get_worker()

    def test_init_dir(self):
        '''Test SimpleUuidServer has expected function and variables'''
        dir_items = [
            item
            for item in dir(self.queue_worker)
            if item[0:2] != '__'
        ]
        member_names = [name for name, _ in self.members]
        for item in dir_items:
            if item in member_names:
                continue
            if item in self.methods:
                continue
            raise AssertionError('Member or method not being tested:%s' % item)
        for item, item_type in self.members:
            self.assertTrue(hasattr(self.queue_worker, item))
            self.assertIsInstance(
                getattr(self.queue_worker, item),
                item_type
            )
        for item in self.methods:
            self.assertTrue(hasattr(self.queue_worker, item))
            self.assertEqual(
                str(type(getattr(self.queue_worker, item))),
                "<class 'method'>"
            )

    def test_init_basic(self):
        '''Test a basic initialization and _get_worker_id'''
        self.assertEqual(
            self.queue_options['processes'],
            self.queue_worker.queue_options['processes']
        )
        self.assertEqual(
            self.queue_options['chunk_size'],
            self.queue_worker.queue_options['chunk_size']
        )
        self.assertEqual(
            self.queue_options['batch_size'],
            self.queue_worker.queue_options['batch_size']
        )
        # pylint: disable=protected-access
        self.assertTrue(self.mock_server is self.queue_worker._queue)
        worker_id = str(int(MOCK_TIME * 1000000) + 1)
        self.assertEqual(worker_id, self.queue_worker.worker_id)

    # Uuids
    def test_get_uuids_called(self):
        '''Test worker get_uuids calls server get_uuids'''
        self.queue_worker.get_uuids()
        self.assertEqual(1, self.mock_server.get_uuids.call_count)
        self.mock_server.get_uuids.assert_called_with(
            self.queue_options['batch_size'],
            get_all=False,
        )
        self.assertListEqual(
            self.mock_return_uuids,
            self.mock_server.get_uuids.return_value
        )

    # Run
    def test_update_finished_called(self):
        '''Test worker update_finished calls server update_finished'''
        results = {}
        self.queue_worker.update_finished(results)
        self.assertEqual(1, self.mock_server.update_finished.call_count)
        self.mock_server.update_finished.assert_called_with(
            self.queue_worker.worker_id,
            results,
        )
        self.assertIsNone(self.mock_server.update_finished.return_value)
