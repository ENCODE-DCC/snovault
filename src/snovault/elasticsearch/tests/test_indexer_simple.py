"""Tests the simple queue with a mocked indexer"""
import time
import uuid

from unittest import (
    TestCase,
    mock,
)

import pytest

from snovault import STORAGE
from snovault.app import main
from snovault.elasticsearch.indexer import Indexer
from snovault.elasticsearch.mpindexer import MPIndexer
from snovault.elasticsearch.interfaces import (
    APP_FACTORY,
    ELASTIC_SEARCH,
)
from snovault.elasticsearch.simple_queue import (
    SimpleUuidServer,
    SimpleUuidWorker,
)


SMALL_REQ_EMBED_WAIT = 0.01
SMALL_UUIDS_CNT = 20
SMALL_SERVE_TIMEOUT = SMALL_UUIDS_CNT * SMALL_REQ_EMBED_WAIT + 5.0
SMALL_BATCH_DIV = 3

def _get_uuids(cnt):
    """
    Return things like this 75659c39-eaea-47b7-ba26-92e9ff183e6c
    """
    uuids = set()
    while len(uuids) < cnt:
        uuids.add(str(uuid.uuid4()))
    return uuids


class MockES(object):  # pylint: disable=too-few-public-methods
    """
    Temp mock es

    There is a pytest module for this!
    https://pypi.org/project/pytest-elasticsearch/
    """
    @staticmethod
    def index(
            index=None,
            doc_type=None,
            body=None,
            id=None,
            version=None,
            version_type=None,
            request_timeout=None,
            raise_ecp=None,
        ):  # pylint: disable=unused-argument, too-many-arguments, invalid-name, redefined-builtin
        '''Fake index'''
        if raise_ecp:
            raise raise_ecp('Fake es index exception.')


class MockRegistry(dict):
    """
    Temp mock registry

    There is probably a better way to do this!
    """

    def __init__(
            self,
            batch_size,
            processes=1,
            queue_name='mockindxQ',
            queue_host=None,
            queue_port=None,
            queue_db=2,
        ):
        super().__init__()
        queue_type = 'Simple'
        self[ELASTIC_SEARCH] = MockES()
        self[STORAGE] = None
        self[APP_FACTORY] = main
        self['available_queues'] = [queue_type]
        self.settings = {
            'indexer': True,
            'snovault.elasticsearch.index': 'index-name',
            'queue_name': queue_name,
            'queue_type': queue_type,
            'queue_server': 'true',
            'queue_worker': 'true',
            'queue_worker_processes': processes,
            'queue_worker_chunk_size': 2,
            'queue_worker_batch_size': batch_size,
            'queue_worker_get_size': batch_size,
            'queue_host': queue_host,
            'queue_port': queue_port,
            'queue_db': queue_db,
        }


class MockRequest(object):  # pylint: disable=too-few-public-methods
    """
    Temp mock request

    There is probably a better way to do this!
    """
    def __init__(self, embed_wait=None):
        self.embeded_uuids = []
        self._embed_wait = embed_wait
        self._embed_errors = 0

    def set_embed_errors(self, cnt):
        '''Raise a given number of errors during embeds'''
        self._embed_errors = cnt

    def embed(self, url, as_user=None):  # pylint: disable=unused-argument
        '''Fake embed'''
        doc = {
            'item_type': 'fake item type',
        }
        if self._embed_wait:
            time.sleep(self._embed_wait)
        if self._embed_errors:
            self._embed_errors -= 1
            raise ValueError('Fake request embed exception.')
        self.embeded_uuids.append(url.split('/')[1])
        return doc


@pytest.fixture()
def small_index_objs():
    """Simple indexing objects from small vars"""
    batch_size = SMALL_UUIDS_CNT // SMALL_BATCH_DIV
    registry = MockRegistry(batch_size)
    indexer = Indexer(registry)
    invalidated = _get_uuids(SMALL_UUIDS_CNT)
    request = MockRequest(embed_wait=SMALL_REQ_EMBED_WAIT)
    return indexer, request, invalidated


# pylint: disable=redefined-outer-name
def test_smsimp_indexinit(small_index_objs):
    """Test simple indexer init with small vars"""
    indexer, _, invalidated = small_index_objs
    assert isinstance(indexer.queue_server, SimpleUuidServer)
    assert isinstance(indexer.queue_worker, SimpleUuidWorker)
    assert len(invalidated) == SMALL_UUIDS_CNT


def test_smsimp_indexserve(small_index_objs):
    """Test simple indexer serve with small vars"""
    indexer, request, invalidated = small_index_objs
    _, errors, err_msg = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=SMALL_SERVE_TIMEOUT,
    )
    assert err_msg is None
    assert isinstance(errors, list)
    assert not errors
    assert len(request.embeded_uuids) == len(invalidated)


def test_smsimp_indextimeout(small_index_objs):
    """test simple indexer serve timeout with small vars"""
    indexer, request, invalidated = small_index_objs
    _, errors, err_msg = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=0.01,
    )
    assert err_msg == 'Indexer sleep timeout'
    assert not errors


def test_smsimp_indexrun(small_index_objs):
    """test simple indexer run  with small vars"""
    indexer, request, invalidated = small_index_objs
    request.set_embed_errors(0)
    worker_runs_expected = SMALL_BATCH_DIV
    if SMALL_UUIDS_CNT % SMALL_BATCH_DIV:
        worker_runs_expected += 1
    _, errors, err_msg = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=SMALL_SERVE_TIMEOUT,
    )
    assert err_msg is None
    assert not errors
    list_invalidated = list(invalidated)
    list_invalidated.sort()
    request.embeded_uuids.sort()
    assert request.embeded_uuids == list_invalidated
    print(indexer.worker_runs)
    assert len(indexer.worker_runs) == worker_runs_expected
    uuids_ran = 0
    for worker_run in indexer.worker_runs:
        uuids_ran += worker_run['uuids']
    assert uuids_ran == SMALL_UUIDS_CNT

def _test_smsimp_indexrun_emberr(small_index_objs):
    """test simple indexer run  with small vars with embed errors"""
    indexer, request, invalidated = small_index_objs
    embed_errors = 3
    request.set_embed_errors(embed_errors)
    worker_runs_expected = SMALL_BATCH_DIV
    if SMALL_UUIDS_CNT % SMALL_BATCH_DIV:
        worker_runs_expected += 1
    _, errors, err_msg = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=SMALL_SERVE_TIMEOUT,
    )
    assert err_msg is None
    assert len(errors) == embed_errors
    list_invalidated = list(invalidated)
    list_invalidated.sort()
    request.embeded_uuids.sort()
    assert len(indexer.worker_runs) == worker_runs_expected
    uuids_ran = 0
    for worker_run in indexer.worker_runs:
        uuids_ran += worker_run['uuids']
    assert uuids_ran == SMALL_UUIDS_CNT


def test_simple_mpindexinit():
    """test simple mpindexer"""
    batch_size = SMALL_UUIDS_CNT // SMALL_BATCH_DIV
    registry = MockRegistry(batch_size)
    processes = registry.settings['queue_worker_processes']
    # pylint: disable=unused-variable
    mpindexer = MPIndexer(registry, processes=processes)
    invalidated = _get_uuids(SMALL_UUIDS_CNT)
    request = MockRequest(embed_wait=SMALL_REQ_EMBED_WAIT)
    assert True


class TestIndexer(TestCase):
    """Test Indexer in indexer.py"""
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
        cls.indexer = Indexer(registry)

    def test_init(self):
        """Test Indexer Init"""
        self.assertIsInstance(self.indexer.queue_server, SimpleUuidServer)
        self.assertIsInstance(self.indexer.queue_worker, SimpleUuidWorker)

    def test_serve_objects_no_uuids(self):
        """Test serve objects with no_uuids"""
        expected_err_msg = (
            'Cannot initialize indexing process: '
            'No uuids given to Indexer.serve_objects'
        )
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
        self.indexer.queue_server.load_uuids = mock.MagicMock(return_value=uuids_loaded_cnt)
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
        Test serve objects when no work.  Should timeout for simple queue

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
        # pylint: disable=protected-access
        self.indexer.queue_server._uuids = []
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
        self.indexer.queue_server.pop_errors = mock.MagicMock(return_value=return_errors)
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
        self.indexer.queue_worker.queue_options['batch_size'] = batch_size
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
        self.indexer.queue_worker.queue_options['batch_size'] = batch_size
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
        self.indexer.queue_worker.queue_options['batch_size'] = batch_size
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
