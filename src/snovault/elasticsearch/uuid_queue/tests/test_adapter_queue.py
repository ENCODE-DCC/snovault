"""
Test the uuid queue module adapter queue file
- _get_combined_uuids_gen
- _get_uncombined_uuids
- QueueTypes
- QueueAdapter
- WorkerAdapter
"""
import uuid

from unittest import (
    TestCase,
    mock,
)

import pytest

from ..adapter_queue import (
    _get_combined_uuids_gen,
    _get_uncombined_uuids,
    QueueTypes,
    QueueAdapter,
    WorkerAdapter,
)

from ..queues.base_queue import (
    BASE_QUEUE_TYPE,
    BaseQueueClient,
    BaseQueue,
)
from ..queues.redis_queues import (
    RedisClient,
)
from .test_redis_queue import (
    REDIS_QUEUES,
    REDIS_QUEUE_CLASSES,
)

MOCK_TIME = 123456.654321
REDIS_QUEUE_TYPES = [a for a, b in REDIS_QUEUES]
UUID_LENGTH = len('75659c39-eaea-47b7-ba26-92e9ff183e6c')


# Test Fixtures
# Test Helpers
def _get_uuids(cnt):
    """
    Return things like this 75659c39-eaea-47b7-ba26-92e9ff183e6c
    """
    uuids = set()
    while len(uuids) < cnt:
        uuids.add(str(uuid.uuid4()))
    return uuids


# _get_combined_uuids_gen
def test_getcombids_wrong_len():
    """Test _get_combined_uuids_gen with different uuid lengths"""
    uuids_count = 17
    uuids_set = _get_uuids(uuids_count)
    batch_by = 17
    uuid_len = UUID_LENGTH
    max_batch_size = batch_by * uuids_count * uuid_len
    uuids_list = []
    for uuid_str in uuids_set:
        uuids_list.append(uuid_str[:uuid_len - 1])
    with pytest.raises(ValueError):
        items = _get_combined_uuids_gen(
            batch_by,
            uuid_len,
            max_batch_size,
            uuids_list
        )
        # Must run generator
        for item in items:  # pylint: disable=unused-variable
            pass


def test_getcombids_one_batch():
    """Test _get_combined_uuids_gen one batch"""
    uuids_count = 17
    uuids_set = _get_uuids(uuids_count)
    batch_by = uuids_count
    uuid_len = UUID_LENGTH
    max_batch_size = batch_by * uuid_len
    combined_uuids = _get_combined_uuids_gen(
        batch_by,
        uuid_len,
        max_batch_size,
        uuids_set
    )
    expected_combined_uuids_count = 1
    expected_full_length = 1
    expected_partial_length = 0
    combined_uuids_count = 0
    full_length = 0
    partial_length = 0
    for comb_uuid_str in combined_uuids:
        combined_uuids_count += 1
        if len(comb_uuid_str) == uuid_len * batch_by:
            full_length += 1
        else:
            partial_length += 1
    assert combined_uuids_count == expected_combined_uuids_count
    assert expected_full_length == full_length
    assert expected_partial_length == partial_length


def test_getcombids_two_batch_max():
    """Test _get_combined_uuids_gen two batch via max size"""
    uuids_count = 17
    uuids_set = _get_uuids(uuids_count)
    batch_by = 17
    uuid_len = UUID_LENGTH
    max_batch_size = batch_by * uuid_len - 1
    combined_uuids = _get_combined_uuids_gen(
        batch_by,
        uuid_len,
        max_batch_size,
        uuids_set
    )
    expected_combined_uuids_count = 2
    expected_full_length = 1
    expected_partial_length = 1
    combined_uuids_count = 0
    full_length = 0
    partial_length = 0
    for comb_uuid_str in combined_uuids:
        combined_uuids_count += 1
        if len(comb_uuid_str) == uuid_len * (batch_by - 1):
            full_length += 1
        else:
            partial_length += 1
    assert combined_uuids_count == expected_combined_uuids_count
    assert expected_full_length == full_length
    assert expected_partial_length == partial_length


def test_getcombids_many_batch():
    """Test _get_combined_uuids_gen with many batches"""
    uuids_count = 47
    uuids_set = _get_uuids(uuids_count)
    batch_by = 6
    uuid_len = UUID_LENGTH
    max_batch_size = batch_by * uuid_len
    combined_uuids = _get_combined_uuids_gen(
        batch_by,
        uuid_len,
        max_batch_size,
        uuids_set
    )
    expected_combined_uuids_count = uuids_count//batch_by
    if uuids_count % batch_by > 0:
        expected_combined_uuids_count += 1
    expected_full_length = expected_combined_uuids_count - 1
    expected_partial_length = 1
    combined_uuids_count = 0
    full_length = 0
    partial_length = 0
    for comb_uuid_str in combined_uuids:
        combined_uuids_count += 1
        if len(comb_uuid_str) == uuid_len * batch_by:
            full_length += 1
        else:
            partial_length += 1
    assert combined_uuids_count == expected_combined_uuids_count
    assert expected_full_length == full_length
    assert expected_partial_length == partial_length


# _get_uncombined_uuids
def test_get_uncombined_uuids():
    """Test _get_uncombined_uuids"""
    uuids_count = 47
    uuids_list = list(_get_uuids(uuids_count))
    batch_by = 6
    uuid_len = UUID_LENGTH
    max_batch_size = batch_by * uuid_len
    combined_uuids = _get_combined_uuids_gen(
        batch_by,
        uuid_len,
        max_batch_size,
        uuids_list
    )
    uncombined_uuids = []
    for combined_uuid in combined_uuids:
        uncombined_uuids.extend(_get_uncombined_uuids(uuid_len, combined_uuid))
    uuids_list.sort()
    uncombined_uuids.sort()
    assert uuids_list == uncombined_uuids


class TestQueueTypes(TestCase):
    """
    Test QueueTypes class

    Everything is a classmethod or class var
    """
    @classmethod
    def setUpClass(cls):
        cls.queue_types = [
            queue_set[0]
            for queue_set in QueueTypes.QUEUES
        ]
        cls.queue_types.sort()

    def test_init(self):
        """
        Test available queue types and clients
        - This should failed when new queue types are added
        """
        for queue_set in QueueTypes.QUEUES:
            queue_type, queue_client = queue_set
            if queue_type == BASE_QUEUE_TYPE:
                self.assertTrue(queue_client is BaseQueueClient)
            elif queue_type in REDIS_QUEUE_TYPES:
                self.assertTrue(queue_client is RedisClient)
            else:
                print("New Queue(%s) not handled in test" % queue_type)
                assert False

    def test_check_queue_type_true(self):
        """Test check_queue_type with real queue type"""
        for queue_type in self.queue_types:
            self.assertTrue(QueueTypes.check_queue_type(queue_type))

    def test_check_queue_type_false(self):
        """Test check_queue_type with fake queue type"""
        queue_type = 'not a queue type'
        self.assertFalse(QueueTypes.check_queue_type(queue_type))

    def test_get_all(self):
        """Test get_all"""
        queue_types = QueueTypes.get_all()
        queue_types.sort()
        self.assertListEqual(queue_types, self.queue_types)

    def test_get_queue_client_class(self):
        """Test get_queue_client_class"""
        for queue_set in QueueTypes.QUEUES:
            queue_type, expected_queue_client = queue_set
            queue_client = QueueTypes.get_queue_client_class(queue_type)
            self.assertTrue(expected_queue_client is queue_client)

    def test_redis_queue_types(self):
        """
        Test that redis queue types exist
        """
        for queue_type in REDIS_QUEUE_TYPES:
            self.assertTrue(
                QueueTypes.check_queue_type(queue_type)
            )


class TestQueueAdapter(TestCase):
    """Test QueueAdapter - Uuid Server"""
    # pylint: disable=too-many-public-methods
    members = [
        ('_queue_name', str),
        ('_queue_type', str),
        ('_queue_options', dict),
        ('_start_us', int),
        ('queue_id', str),
        ('_queue', None), # depends on _queue_type
    ]
    meth_func = [
        # Errors
        '_has_errors',
        'pop_errors',
        # Worker
        '_get_worker_id',
        '_get_worker_queue',
        'get_worker',
        'update_worker_conn',
        # Uuids
        'has_uuids',
        '_load_uuids',
        'load_uuids',
        # Run
        'is_indexing',
        # Queue Client
        '_get_queue',
        'close_indexing', # not tested
    ]

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        '''Create a simple queue to be used most tests'''
        cls.load_uuids = [str(num) for num in range(10, 99)]
        cls.queue_name = 'test-queue-name'
        cls.queue_type = BASE_QUEUE_TYPE
        cls.queue_options = {
            'processes': 1,
            'chunk_size': 1,
            'batch_size': 10,
            'uuid_len': 2,
        }
        cls.mock_time_us = int(MOCK_TIME * 1000000)
        cls.queue_server = QueueAdapter(
            cls.queue_name,
            cls.queue_type,
            cls.queue_options
        )
        cls.queue_worker = None

    def setUp(self):
        """Setup for each test"""
        self.queue_worker = self.queue_server.get_worker()

    def tearDown(self):
        """Reset for each test"""
        self.queue_worker = None
        # pylint: disable=protected-access
        self.queue_server._queue._qmeta._errors = []
        self.queue_server._queue._qmeta._worker_conns = {}
        self.queue_server._queue._qmeta._worker_results = {}
        self.queue_server._queue._qmeta._uuid_count = 0
        self.queue_server._queue._uuids = []

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
            if item_type:
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
    def test_has_errors_false(self):
        '''Test _has_errors returns false when empty'''
        # pylint: disable=protected-access
        self.assertFalse(self.queue_server._has_errors())

    def test_has_errors_true(self):
        '''Test _has_errors return true when not empty'''
        # pylint: disable=protected-access
        self.queue_server._queue._qmeta._errors = ['e']
        self.assertTrue(self.queue_server._has_errors())

    def test_pop_errors(self):
        '''Test pop_errors'''
        given_errors = ['e1', 'e2', 'e3']
        # pylint: disable=protected-access
        self.queue_server._queue._qmeta._errors = given_errors
        errors = self.queue_server.pop_errors()
        given_errors.sort()
        self.assertListEqual(errors, given_errors)
        self.assertListEqual(
            [],
            self.queue_server._queue._qmeta._errors
        )

    def test_get_worker_id(self):
        '''Test _get_worker_id'''
        # Plus two since a worker is added in setUp
        id_time_us = self.mock_time_us + 2
        # pylint: disable=protected-access
        worker_id = self.queue_server._get_worker_id()
        self.assertTrue(
            worker_id == str(id_time_us)
        )
        _ = self.queue_server._get_worker_id()
        # The function does not add the worker to the conn
        # so the id should not change
        worker_id = self.queue_server._get_worker_id()
        self.assertTrue(
            worker_id == str(id_time_us)
        )

    def test_get_worker_queue_base(self):
        '''Test _get_worker_queue for base queue type'''
        queue_name = 'test-wrk-queue-name'
        queue_type = BASE_QUEUE_TYPE
        queue_options = {
            'processes': 1,
            'chunk_size': 1,
            'batch_size': 10,
            'uuid_len': 2,
        }
        tmp_queue_server = QueueAdapter(
            queue_name,
            queue_type,
            queue_options
        )
        # pylint: disable=protected-access
        tmp_worker_queue = tmp_queue_server._get_worker_queue()
        self.assertTrue(
            tmp_queue_server._queue is tmp_worker_queue
        )

    def test_get_worker_queue_redis(self):
        '''Test _get_worker_queue for redis queue types'''
        for queue_type, queue_class in REDIS_QUEUE_CLASSES:
            queue_name = queue_type.lower()
            queue_type = queue_type
            queue_options = {
                'processes': 1,
                'chunk_size': 1,
                'batch_size': 10,
                'uuid_len': 2,
                'host': 'localhost',
                'port': 6379,
            }
            tmp_queue_server = QueueAdapter(
                queue_name,
                queue_type,
                queue_options
            )
            # pylint: disable=protected-access
            self.assertIsInstance(tmp_queue_server._queue, queue_class)

    def test_get_worker_base(self):
        '''Test get_worker with base queue'''
        queue_name = 'test-wrk-queue-name'
        queue_type = BASE_QUEUE_TYPE
        queue_options = {
            'processes': 1,
            'chunk_size': 1,
            'batch_size': 10,
            'uuid_len': 2,
        }
        tmp_queue_server = QueueAdapter(
            queue_name,
            queue_type,
            queue_options
        )
        tmp_worker_queue = tmp_queue_server.get_worker()
        self.assertIsInstance(tmp_worker_queue, WorkerAdapter)
        # pylint: disable=protected-access
        ref_worker_id = str(tmp_queue_server._start_us + 1)
        self.assertEqual(ref_worker_id, tmp_worker_queue.worker_id)
        self.assertTrue(
            ref_worker_id in tmp_queue_server._queue._qmeta._worker_conns
        )

    def test_get_worker_redis(self):
        '''Test get_worker for redis queues'''
        for queue_type, queue_class in REDIS_QUEUE_CLASSES:
            queue_name = queue_type.lower()
            queue_type = queue_type
            queue_options = {
                'processes': 1,
                'chunk_size': 1,
                'batch_size': 10,
                'uuid_len': 2,
                'host': 'localhost',
                'port': 6379,
            }
            tmp_queue_server = QueueAdapter(
                queue_name,
                queue_type,
                queue_options
            )
            # pylint: disable=protected-access
            self.assertIsInstance(tmp_queue_server._queue, queue_class)

    def test_update_worker_conn(self):
        '''Test update_worker_conn'''
        update_uuid_cnt = 9
        update_get_cnt = 3
        worker_id = self.queue_worker.worker_id
        # pylint: disable=protected-access
        worker_conn = self.queue_server._queue._qmeta._worker_conns[worker_id]
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

    # Uuids
    def test_has_uuids(self):
        '''Test has_uuids'''
        # pylint: disable=protected-access
        self.queue_server._queue._qmeta._uuid_count = 4
        self.assertTrue(self.queue_server.has_uuids())

    def test_has_uuids_empty(self):
        '''Test has_uuids when empty'''
        self.assertFalse(self.queue_server.has_uuids())

    def test_load_uuids_batch_one(self):
        '''Test _load_uuids with batch size 1'''
        queue_name = 'test-wrk-queue-name'
        queue_type = BASE_QUEUE_TYPE
        queue_options = {
            'processes': 1,
            'chunk_size': 1,
            'batch_size': 1,
            'uuid_len': 2,
        }
        tmp_queue_server = QueueAdapter(
            queue_name,
            queue_type,
            queue_options
        )
        load_uuids_len = len(self.load_uuids)
        # pylint: disable=protected-access
        success_cnt = tmp_queue_server._load_uuids(self.load_uuids.copy())
        self.assertEqual(load_uuids_len, success_cnt)
        self.assertEqual(
            len(tmp_queue_server._queue._uuids),
            load_uuids_len
        )

    def test_load_uuids_batch_many(self):
        '''Test _load_uuids with batch size greater than 1'''
        load_uuids_len = len(self.load_uuids.copy())
        batch_size = self.queue_options['batch_size']
        expected_batches = load_uuids_len//batch_size
        if load_uuids_len % batch_size:
            expected_batches += 1
        # pylint: disable=protected-access
        success_cnt = self.queue_server._load_uuids(self.load_uuids.copy())
        self.assertEqual(load_uuids_len, success_cnt)
        self.assertEqual(
            len(self.queue_server._queue._uuids),
            expected_batches
        )

    def test_load_uuids_indexing(self):
        '''Test load_uuids when already indexing'''
        # pylint: disable=protected-access
        self.queue_server._queue._qmeta._uuid_count = 583
        self.assertIsNone(self.queue_server.load_uuids(self.load_uuids.copy()))

    def test_load_uuids(self):
        '''Test load_uuids'''
        copy_load_uuids = self.load_uuids.copy()
        load_uuid_len = len(self.load_uuids)
        _ = self.queue_server.load_uuids(copy_load_uuids)
        # pylint: disable=protected-access
        self.assertEqual(
            self.queue_server._queue._qmeta._uuid_count,
            load_uuid_len
        )

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
        self.queue_server._queue._qmeta._worker_conns[worker_id]['uuid_cnt'] = 1
        self.assertTrue(self.queue_server.is_indexing())

    def test_is_indexing_true_haserrors(self):
        '''
        Test is_indexing is true when has errors is true
        '''
        # pylint: disable=protected-access
        self.queue_server._queue._qmeta._errors = [1]
        self.assertTrue(self.queue_server.is_indexing())

    def test_is_indexing_true_hasuuids(self):
        '''
        Test is_indexing is true when has uuids is true
        '''
        # pylint: disable=protected-access
        self.queue_server._queue._qmeta._uuid_count = 130
        self.assertTrue(self.queue_server.is_indexing())

    # Queue Client
    def test_get_queue_base(self):
        '''Test _get_queue for base queue'''
        # pylint: disable=protected-access
        self.assertIsInstance(self.queue_server._get_queue(self.queue_name), BaseQueue)

    def test_get_queue_redis(self):
        '''Test _get_queue for redis queues'''
        for queue_type, queue_class in REDIS_QUEUE_CLASSES:
            queue_name = 'test-wrk-queue-name'
            queue_type = queue_type
            queue_options = {
                'processes': 1,
                'chunk_size': 1,
                'batch_size': 1,
                'uuid_len': 2,
                'host': 'localhost',
                'port': 6379,
            }
            tmp_queue_server = QueueAdapter(
                queue_name,
                queue_type,
                queue_options
            )
            # pylint: disable=protected-access
            self.assertIsInstance(tmp_queue_server._queue, queue_class)
            self.assertIsInstance(tmp_queue_server._get_queue(queue_name), queue_class)

    def test_get_queue_bad_type(self):
        '''Test _get_queue'''
        queue_name = 'test-wrk-queue-name'
        queue_type = 'bad-queue-type'
        queue_options = {
            'processes': 1,
            'chunk_size': 1,
            'batch_size': 1,
            'uuid_len': 2,
        }
        tmp_queue_server = QueueAdapter(
            queue_name,
            queue_type,
            queue_options
        )
        # pylint: disable=protected-access
        self.assertIsNone(tmp_queue_server._queue)
        self.assertIsNone(tmp_queue_server._get_queue(queue_name))


class TestWorkerAdapater(TestCase):
    """Test WorkerAdapater - Uuid Worker"""
    # pylint: disable=too-many-public-methods
    members = [
        ('_queue_name', str),
        ('_queue_options', dict),
        ('worker_id', str),
        ('_queue', None), # depends on _queue_type
        ('is_running', bool),
        ('get_cnt', int),
        ('uuid_cnt', int),
        ('chunk_size', int),
        ('processes', int),
    ]
    meth_func = [
        # Uuids
        '_get_uncombined_uuids',
        '_get_uuids',
        'get_uuids',
        # Run
        'update_finished',
    ]

    @staticmethod
    def _get_worker(*args):
        (queue_name, queue_type,
         queue_options, worker_id) = args
        client_class = QueueTypes.get_queue_client_class(queue_type)
        client = client_class(queue_options)
        queue = client.get_queue(queue_name, queue_type)
        return WorkerAdapter(
            queue_name,
            queue_options,
            worker_id,
            queue,
        )

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        '''Create a simple worker to be used most tests'''
        cls.base_queue_name = 'base-queue-name'
        cls.base_queue_options = {
            'processes': 1,
            'chunk_size': 1,
            'batch_size': 10,
            'uuid_len': 4,
            'get_size': 1,
        }
        cls.base_worker_id = 'base-worker-id'
        cls.base_worker = cls._get_worker(
            cls.base_queue_name,
            BASE_QUEUE_TYPE,
            cls.base_queue_options,
            cls.base_worker_id,
        )

    def setUp(self):
        """Setup for each test"""
        pass

    def tearDown(self):
        """Reset for each test"""
        pass

    # private/protected
    def test_init_dir(self):
        '''Test SimpleUuidServer has expected function and variables'''
        dir_items = [
            item
            for item in dir(self.base_worker)
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
            self.assertTrue(hasattr(self.base_worker, item))
            if item_type:
                self.assertIsInstance(
                    getattr(self.base_worker, item),
                    item_type
                )
        for item in self.meth_func:
            self.assertTrue(hasattr(self.base_worker, item))
            self.assertTrue(
                str(type(getattr(self.base_worker, item))) in [
                    "<class 'method'>",
                    "<class 'function'>",
                    "<class 'unittest.mock.MagicMock'>",
                ]
            )

    def test_init_basic(self):
        '''Test _get_worker_id'''
        # pylint: disable=protected-access
        self.assertEqual(self.base_queue_name, self.base_worker._queue_name)
        self.assertDictEqual(
            self.base_queue_options,
            self.base_worker._queue_options
        )
        self.assertEqual(self.base_worker_id, self.base_worker.worker_id)
        self.assertIsInstance(self.base_worker._queue, BaseQueue)
        self.assertFalse(self.base_worker.is_running)
        self.assertEqual(0, self.base_worker.get_cnt)
        self.assertEqual(0, self.base_worker.uuid_cnt)


    def test_get_uuids_batchsize_one(self):
        '''Test _get_uuids when batch size is one'''
        # pylint: disable=protected-access
        self.base_worker._queue_options['batch_size'] = 1
        org_queue_get_uuids = self.base_worker._queue.get_uuids
        org_get_uncombined_uuids = self.base_worker._get_uncombined_uuids
        self.base_worker._queue.get_uuids = mock.MagicMock()
        self.base_worker._get_uncombined_uuids = mock.MagicMock()
        _ = self.base_worker._get_uuids()
        self.assertEqual(1, self.base_worker._queue.get_uuids.call_count)
        self.base_worker._queue.get_uuids.assert_called_with(1)
        self.assertEqual(0, self.base_worker._get_uncombined_uuids.call_count)
        self.base_worker._queue.get_uuids = org_queue_get_uuids
        self.base_worker._get_uncombined_uuids = org_get_uncombined_uuids


    def test_get_uuids_batchsize_many(self):
        '''Test _get_uuids when batch size is greater than one'''
        # pylint: disable=protected-access
        self.base_worker._queue_options['batch_size'] = 2
        return_uuids = ['u01xu02x']
        expected_result = ['u01x', 'u02x']
        org_queue_get_uuids = self.base_worker._queue.get_uuids
        org_get_uncombined_uuids = self.base_worker._get_uncombined_uuids
        self.base_worker._queue.get_uuids = mock.MagicMock(
            return_value=return_uuids
        )
        self.base_worker._get_uncombined_uuids = mock.MagicMock(
            return_value=expected_result
        )
        result = self.base_worker._get_uuids()
        self.assertEqual(1, self.base_worker._queue.get_uuids.call_count)
        self.base_worker._queue.get_uuids.assert_called_with(1)
        self.assertEqual(1, self.base_worker._get_uncombined_uuids.call_count)
        self.base_worker._get_uncombined_uuids.assert_called_with(
            4,
            return_uuids[0]
        )
        result.sort()
        expected_result.sort()
        self.assertListEqual(result, expected_result)
        self.base_worker._queue.get_uuids = org_queue_get_uuids
        self.base_worker._get_uncombined_uuids = org_get_uncombined_uuids

    def test_get_uuids_uuid_cnt_zero(self):
        '''Test get_uuids when uuid cnt is zero'''
        get_return_value = ['u01x', 'u02x']
        queue_options = self.base_queue_options.copy()
        base_worker = self._get_worker(
            self.base_queue_name,
            BASE_QUEUE_TYPE,
            queue_options,
            self.base_worker_id,
        )
        # pylint: disable=protected-access
        base_worker._get_uuids = mock.MagicMock(
            return_value=get_return_value
        )
        base_worker._queue.update_uuid_count = mock.MagicMock()
        base_worker._queue.update_worker_conn = mock.MagicMock()
        base_worker.get_uuids()
        self.assertEqual(base_worker.uuid_cnt, len(get_return_value))
        self.assertEqual(1, base_worker._queue.update_uuid_count.call_count)
        base_worker._queue.update_uuid_count.assert_called_with(-1 * len(get_return_value))
        self.assertEqual(1, base_worker._queue.update_worker_conn.call_count)
        base_worker._queue.update_worker_conn.assert_called_with(
            base_worker.worker_id,
            len(get_return_value),
            1,
        )

    def test_get_uuids_uuid_cnt(self):
        '''Test get_uuids when uuid cnt is not zero'''
        queue_options = self.base_queue_options.copy()
        base_worker = self._get_worker(
            self.base_queue_name,
            BASE_QUEUE_TYPE,
            queue_options,
            self.base_worker_id,
        )
        base_worker.get_cnt = 1
        base_worker.uuid_cnt = 14
        # pylint: disable=protected-access
        base_worker._get_uuids = mock.MagicMock(return_value=[])
        base_worker._queue.update_uuid_count = mock.MagicMock()
        base_worker._queue.update_worker_conn = mock.MagicMock()
        base_worker.get_uuids()
        self.assertEqual(base_worker.uuid_cnt, 14)
        self.assertEqual(0, base_worker._queue.update_uuid_count.call_count)
        self.assertEqual(1, base_worker._queue.update_worker_conn.call_count)
        base_worker._queue.update_worker_conn.assert_called_with(
            base_worker.worker_id,
            14,
            2,
        )

    def test_update_finished_okay(self):
        '''Test update_finished when Okay finish'''
        base_worker = self._get_worker(
            self.base_queue_name,
            BASE_QUEUE_TYPE,
            self.base_queue_options,
            self.base_worker_id,
        )
        base_worker.uuid_cnt = 4
        return_msg = 'Okay'
        # pylint: disable=protected-access
        base_worker._queue.update_finished = mock.MagicMock(
            return_value=return_msg
        )
        exepcted_result_msg = None  # None when 'Okay'
        given_results = {'test': 1, 'successes': 1, 'errors': []}
        result_msg = base_worker.update_finished(given_results)
        self.assertEqual(1, base_worker._queue.update_finished.call_count)
        base_worker._queue.update_finished.assert_called_with(
            base_worker.worker_id,
            given_results,
        )
        self.assertEqual(exepcted_result_msg, result_msg)
        self.assertEqual(0, base_worker.uuid_cnt)

    def test_update_finished_not_okay(self):
        '''Test update_finished when not Okay finish'''
        base_worker = self._get_worker(
            self.base_queue_name,
            BASE_QUEUE_TYPE,
            self.base_queue_options,
            self.base_worker_id,
        )
        base_worker.uuid_cnt = 4
        return_msg = 'Not Okay'
        # pylint: disable=protected-access
        base_worker._queue.update_finished = mock.MagicMock(
            return_value=return_msg
        )
        exepcted_result_msg = 'Update finished could not reset worker: %s' % return_msg
        given_results = {'test': 1, 'successes': 1, 'errors': []}
        result_msg = base_worker.update_finished(given_results)
        self.assertEqual(1, base_worker._queue.update_finished.call_count)
        base_worker._queue.update_finished.assert_called_with(
            base_worker.worker_id,
            given_results,
        )
        self.assertEqual(exepcted_result_msg, result_msg)
        self.assertEqual(4, base_worker.uuid_cnt)
