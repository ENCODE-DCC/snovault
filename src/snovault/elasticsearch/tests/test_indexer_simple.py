"""Tests the simple queue with a mocked indexer"""
import random
import string
import time
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
    char_pool = string.ascii_lowercase + string.digits
    uuids = set()
    while len(uuids) < cnt:
        uuids.add(
            "{}-{}-{}-{}-{}".format(
                ''.join([random.choice(char_pool) for _ in range(8)]),
                ''.join([random.choice(char_pool) for _ in range(4)]),
                ''.join([random.choice(char_pool) for _ in range(4)]),
                ''.join([random.choice(char_pool) for _ in range(4)]),
                ''.join([random.choice(char_pool) for _ in range(12)]),
            )
        )
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

    def __init__(self, batch_size, processes=1):
        super().__init__()
        queue_type = 'Simple'
        self[ELASTIC_SEARCH] = MockES()
        self[STORAGE] = None
        self[APP_FACTORY] = main
        self['available_queues'] = [queue_type]
        self.settings = {
            'snovault.elasticsearch.index': 'index-name',
            'queue_type': queue_type,
            'queue_server': 'true',
            'queue_worker': 'true',
            'queue_worker_processes': processes,
            'queue_worker_chunk_size': 2,
            'queue_worker_batch_size': batch_size,
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
    errors = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=SMALL_SERVE_TIMEOUT,
    )
    assert isinstance(errors, list)
    assert not errors
    assert len(request.embeded_uuids) == len(invalidated)


def test_smsimp_indextimeout(small_index_objs):
    """test simple indexer serve timeout with small vars"""
    indexer, request, invalidated = small_index_objs
    errors = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=0.01,
    )
    assert len(errors) == 1
    assert errors[0] == 'Indexer sleep timeout'


def test_smsimp_indexrun(small_index_objs):
    """test simple indexer run  with small vars"""
    indexer, request, invalidated = small_index_objs
    request.set_embed_errors(0)
    worker_runs_expected = SMALL_BATCH_DIV
    if SMALL_UUIDS_CNT % SMALL_BATCH_DIV:
        worker_runs_expected += 1
    errors = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=SMALL_SERVE_TIMEOUT,
    )
    assert not errors
    list_invalidated = list(invalidated)
    list_invalidated.sort()
    request.embeded_uuids.sort()
    assert request.embeded_uuids == list_invalidated
    assert len(indexer.worker_runs) == worker_runs_expected
    uuids_ran = 0
    for worker_run in indexer.worker_runs:
        uuids_ran += worker_run['uuids']
    assert uuids_ran == SMALL_UUIDS_CNT

def test_smsimp_indexrun_emberr(small_index_objs):
    """test simple indexer run  with small vars with embed errors"""
    indexer, request, invalidated = small_index_objs
    embed_errors = 3
    request.set_embed_errors(embed_errors)
    worker_runs_expected = SMALL_BATCH_DIV
    if SMALL_UUIDS_CNT % SMALL_BATCH_DIV:
        worker_runs_expected += 1
    errors = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=SMALL_SERVE_TIMEOUT,
    )
    assert len(errors) == embed_errors
    list_invalidated = list(invalidated)
    list_invalidated.sort()
    request.embeded_uuids.sort()
    assert len(indexer.worker_runs) == worker_runs_expected
    uuids_ran = 0
    for worker_run in indexer.worker_runs:
        uuids_ran += worker_run['uuids']
    assert uuids_ran == SMALL_UUIDS_CNT

def test_smsimp_indexrun_doubleerrs(small_index_objs):
    """test 2 simple indexer runs with small vars"""
    indexer, request, invalidated = small_index_objs
    embed_errors = 3
    request.set_embed_errors(embed_errors)
    worker_runs_expected = SMALL_BATCH_DIV
    if SMALL_UUIDS_CNT % SMALL_BATCH_DIV:
        worker_runs_expected += 1
    # First Run
    errors = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=SMALL_SERVE_TIMEOUT,
    )
    assert len(errors) == embed_errors
    list_invalidated = list(invalidated)
    list_invalidated.sort()
    request.embeded_uuids.sort()
    assert len(indexer.worker_runs) == worker_runs_expected
    uuids_ran = 0
    for worker_run in indexer.worker_runs:
        uuids_ran += worker_run['uuids']
    assert uuids_ran == SMALL_UUIDS_CNT
    # Second Run
    request.embeded_uuids = []
    request.set_embed_errors(0)
    errors = indexer.serve_objects(
        request,
        invalidated,
        None,  # xmin
        snapshot_id=None,
        restart=False,
        timeout=SMALL_SERVE_TIMEOUT,
    )
    assert not errors
    list_invalidated = list(invalidated)
    list_invalidated.sort()
    request.embeded_uuids.sort()
    assert request.embeded_uuids == list_invalidated
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
