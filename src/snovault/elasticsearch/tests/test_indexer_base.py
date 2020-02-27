"""Tests the uuid queue base queue with a mocked indexer"""
from unittest import (
    TestCase,
    mock,
)

from snovault.elasticsearch.indexer import Indexer
from snovault.elasticsearch.uuid_queue import (
    QueueAdapter,
    QueueTypes,
)
from snovault.elasticsearch.uuid_queue.adapter_queue import (
    WorkerAdapter,
)
from snovault.elasticsearch.uuid_queue.queues.base_queue import (
    BaseQueue,
    BASE_QUEUE_TYPE,
)

from .test_indexer_simple import (
    _get_uuids,
    MockRegistry,
    MockRequest,
)


class TestIndexerBaseQueue(TestCase):
    """Test Indexer in indexer.py with base queue set"""
    @classmethod
    def setUpClass(cls):
        # Request
        cls.embed_wait = 0.01
        # Server Objedcts
        cls.invalidated = _get_uuids(2500)
        cls.xmin = None
        cls.snapshot_id = 'Some-Snapshot-Id'
        cls.restart = False
        # Indexer
        batch_size = 100
        processes = 1
        registry = MockRegistry(batch_size, processes=processes)
        queue_type = BASE_QUEUE_TYPE
        registry.settings['queue_type'] = queue_type
        registry['available_queues'].append(queue_type)
        registry['UuidQueue'] = QueueAdapter
        cls.indexer = Indexer(registry)

    def tearDown(self):
        """Reset the queue and queue meta data"""
        # pylint: disable=protected-access
        # Queue
        self.indexer.queue_server._queue._uuids = []
        # Queue Meta Data
        self.indexer.queue_server._queue._qmeta._errors = []
        self.indexer.queue_server._queue._qmeta._uuid_count = 0
        q_wrk_id = self.indexer.queue_worker.worker_id
        self.indexer.queue_server._queue._qmeta._worker_conns[q_wrk_id] = {
            'uuid_cnt': 0,
            'get_cnt': 0,
        }
        self.indexer.queue_server._queue._qmeta._worker_results[q_wrk_id] = []

    def test_init(self):
        """Test Indexer Init"""
        q_srv = self.indexer.queue_server
        q_wrk = self.indexer.queue_worker
        self.assertIsInstance(q_srv, QueueAdapter)
        self.assertIsInstance(q_wrk, WorkerAdapter)
        # pylint: disable=protected-access
        self.assertIsInstance(q_srv._queue, BaseQueue)
        self.assertIsInstance(self.indexer.queue_worker._queue, BaseQueue)
        self.assertTrue(q_srv._queue is q_wrk._queue)

    def test_serve_objects_no_uuids(self):
        """Test serve objects with no_uuids"""
        expected_err_msg = 'Cannot initialize indexing process: No uuids given to Indexer.serve_objects'
        request = MockRequest(embed_wait=self.embed_wait)
        uuids = []
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=100,
        )
        self.assertEqual(err_msg, expected_err_msg)
        self.assertListEqual(errors, [])

    def test_serve_objects_noload(self):
        """Test serve objects when fails to load any uuids"""
        uuids = self.invalidated
        org_load_uuids = self.indexer.queue_server.load_uuids
        self.indexer.queue_server.load_uuids = mock.MagicMock(return_value=None)
        expected_err_msg = 'Uuids given to Indexer.serve_objects failed to load'
        request = MockRequest(embed_wait=self.embed_wait)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=100,
        )
        self.assertEqual(err_msg, expected_err_msg)
        self.assertListEqual(errors, [])
        self.indexer.queue_server.load_uuids = org_load_uuids

    def test_serve_objects_partload(self):
        """Test serve objects when fails to load all uuids"""
        uuids_loaded_cnt = 1
        uuids = self.invalidated
        org_load_uuids = self.indexer.queue_server.load_uuids
        self.indexer.queue_server.load_uuids = mock.MagicMock(
            return_value=uuids_loaded_cnt
        )
        expected_err_msg = (
            'Uuids given to Indexer.serve_objects '
            'failed to all load. {} of {} only'.format(
                uuids_loaded_cnt,
                len(uuids),
            )
        )
        request = MockRequest(embed_wait=self.embed_wait)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=100,
        )
        self.assertEqual(err_msg, expected_err_msg)
        self.assertListEqual(errors, [])
        self.indexer.queue_server.load_uuids = org_load_uuids

    def test_serve_objects_isindexing(self):
        """Test serve objects when already indexing"""
        uuids = self.invalidated
        org_is_indexing = self.indexer.queue_server.is_indexing
        self.indexer.queue_server.is_indexing = mock.MagicMock(return_value=True)
        expected_err_msg = 'Cannot initialize indexing process: Already Indexing'
        request = MockRequest(embed_wait=self.embed_wait)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=100,
        )
        self.assertEqual(err_msg, expected_err_msg)
        self.assertListEqual(errors, [])
        self.indexer.queue_server.is_indexing = org_is_indexing

    def test_serve_objects_noworker(self):
        """
        Test serve objects when no worker.  Should timeout for simple queue

        Since worker cannot finish we manually reset
            - self.indexer.queue_server._uuids = []
        """
        uuids = self.invalidated
        org_queue_worker = self.indexer.queue_worker
        self.indexer.queue_worker = None
        expected_err_msg = 'Indexer sleep timeout'
        request = MockRequest(embed_wait=self.embed_wait)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=1,
        )
        self.assertEqual(err_msg, expected_err_msg)
        self.assertListEqual(errors, [])
        self.indexer.queue_worker = org_queue_worker

    def test_serve_objects_timeout(self):
        """
        Test serve objects timeout

        Since worker cannot finish we manually reset
            - self.indexer.queue_server._uuids = []
            - serve objects can only timeout after worker finishes
        """
        uuids = self.invalidated
        embed_wait = 0.01
        expected_err_msg = 'Indexer sleep timeout'
        request = MockRequest(embed_wait=embed_wait)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=0.1,
        )
        # pylint: disable=protected-access
        self.indexer.queue_server._uuids = []
        self.assertEqual(err_msg, expected_err_msg)
        self.assertListEqual(errors, [])

    def test_serve_objects_batcherrs(self):
        """
        Test serve objects returns batch errors
        """
        uuids = list(self.invalidated)[0:100]
        embed_wait = 0.01
        return_errors = ['e1', 'e2', 'e3']
        org_pop_errors = self.indexer.queue_server.pop_errors
        self.indexer.queue_server.pop_errors = mock.MagicMock(
            return_value=return_errors
        )
        request = MockRequest(embed_wait=embed_wait)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=10,
        )
        self.indexer.queue_server.pop_errors = org_pop_errors
        # pylint: disable=protected-access
        self.indexer.queue_server._uuids = []
        self.assertIsNone(err_msg)
        errors.sort()
        return_errors.sort()
        self.assertListEqual(errors, return_errors)

    def test_serve_objects_run(self):
        """
        Test serve objects run
        """
        uuids = self.invalidated
        embed_wait = 0.001
        batch_size = 1024
        # pylint: disable=protected-access
        self.indexer.queue_worker._queue_options['batch_size'] = batch_size
        request = MockRequest(embed_wait=embed_wait)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=100,
        )
        self.assertListEqual(errors, [])
        self.assertIsNone(err_msg)

    def test_serve_objects_runerrs(self):
        """
        Test serve objects run with errors
        """
        uuids = self.invalidated
        embed_wait = 0.001
        batch_size = 1024
        # pylint: disable=protected-access
        self.indexer.queue_worker._queue_options['batch_size'] = batch_size
        request = MockRequest(embed_wait=embed_wait)
        request.set_embed_errors(10)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=100,
        )
        self.assertEqual(len(errors), 10)
        self.assertIsNone(err_msg)

    def test_serve_objects_twiceerrs(self):
        """
        Test serve objects running twice with first run having errors
        """
        uuids = self.invalidated
        embed_wait = 0.001
        batch_size = 1024
        # pylint: disable=protected-access
        self.indexer.queue_worker._queue_options['batch_size'] = batch_size
        # Error Run
        request = MockRequest(embed_wait=embed_wait)
        request.set_embed_errors(10)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=100,
        )
        self.assertEqual(len(errors), 10)
        self.assertIsNone(err_msg)
        # Clean Run
        request = MockRequest(embed_wait=embed_wait)
        _, errors, err_msg = self.indexer.serve_objects(
            request,
            uuids,
            self.xmin,
            snapshot_id=self.snapshot_id,
            restart=self.restart,
            timeout=100,
        )
        self.assertListEqual(errors, [])
        self.assertIsNone(err_msg)
