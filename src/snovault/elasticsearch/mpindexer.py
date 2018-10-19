'''Multi Processing Indexer'''
import atexit
import logging
import time

from contextlib import contextmanager
from multiprocessing import get_context
from multiprocessing.pool import Pool
from pyramid.decorator import reify
from pyramid.request import apply_request_extensions
from pyramid.threadlocal import (
    get_current_request,
    manager,
)

import transaction

from snovault import DBSESSION

from .interfaces import (
    APP_FACTORY,
    INDEXER,
)
from .primary_indexer import (
    PrimaryIndexer,
    IndexItem,
)


log = logging.getLogger('snovault.elasticsearch.es_index_listener')  # pylint: disable=invalid-name
# Running in subprocess
app = None  # pylint: disable=invalid-name
current_xmin_snapshot_id = None  # pylint: disable=invalid-name


def initializer(app_factory, settings):
    '''Multi Process Function'''
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    global app  # pylint: disable=global-statement, invalid-name
    atexit.register(clear_snapshot)
    app = app_factory(settings, indexer_worker=True, create_tables=False)
    signal.signal(signal.SIGALRM, clear_snapshot)


def set_snapshot(xmin, snapshot_id):
    '''Multi Process Function'''
    global current_xmin_snapshot_id  # pylint: disable=global-statement, invalid-name
    if current_xmin_snapshot_id == (xmin, snapshot_id):
        return
    clear_snapshot()
    current_xmin_snapshot_id = (xmin, snapshot_id)

    while True:
        txn = transaction.begin()
        txn.doom()
        if snapshot_id is not None:
            txn.setExtendedInfo('snapshot_id', snapshot_id)
        session = app.registry[DBSESSION]()
        connection = session.connection()
        db_xmin = connection.execute(
            "SELECT txid_snapshot_xmin(txid_current_snapshot());").scalar()
        if db_xmin >= xmin:
            break
        transaction.abort()
        log.info('Waiting for xmin %r to reach %r', db_xmin, xmin)
        time.sleep(0.1)

    registry = app.registry
    request = app.request_factory.blank('/_indexing_pool')
    request.registry = registry
    request.datastore = 'database'
    apply_request_extensions(request)
    request.invoke_subrequest = app.invoke_subrequest
    request.root = app.root_factory(request)
    request._stats = {}  #  pylint: disable=protected-access
    manager.push({'request': request, 'registry': registry})


def clear_snapshot(signum=None, frame=None):
    '''Multi Process Function'''
    # pylint: disable=unused-argument
    global current_xmin_snapshot_id  # pylint: disable=global-statement, invalid-name
    if current_xmin_snapshot_id is None:
        return
    transaction.abort()
    manager.pop()
    current_xmin_snapshot_id = None


@contextmanager
def snapshot(xmin, snapshot_id):
    '''Multi Process Function'''
    import signal
    signal.alarm(0)
    set_snapshot(xmin, snapshot_id)
    yield
    signal.alarm(5)


def update_object_in_snapshot(args):
    '''Multi Process Function'''
    uuid, xmin, snapshot_id, restart = args
    with snapshot(xmin, snapshot_id):
        request = get_current_request()
        index_item = IndexItem({}, uuid=uuid, xmin=xmin)
        indexer = request.registry[INDEXER]
        start_time = time.time()
        index_item_dict = index_item.get_pickable()
        index_item_dict = indexer.update_object(
            request,
            index_item_dict,
            restart=restart,
        )
        index_item.update_from_dict(index_item_dict)
        index_item.total_time = time.time() - start_time
        return index_item


# Running in main process
class MPIndexer(PrimaryIndexer):
    '''Multi Processing indexer class'''
    #   pooled processes will exit and be replaced after this
    # many tasks are completed.
    maxtasks = 1

    def __init__(self, registry, processes=None):
        super(MPIndexer, self).__init__(registry)
        self.processes = processes
        # in production.ini (via buildout.cfg) as 1024
        self.chunksize = int(registry.settings.get('indexer.chunk_size', 1024))
        self.initargs = (registry[APP_FACTORY], registry.settings,)

    @reify
    def pool(self):
        '''Multi Process Function'''
        return Pool(
            processes=self.processes,
            initializer=initializer,
            initargs=self.initargs,
            maxtasksperchild=self.maxtasks,
            context=get_context('forkserver'),
        )

    def update_objects(self, request, uuids, xmin, snapshot_id=None, restart=False):
        '''Overide udpate objects'''
        # pylint: disable=too-many-arguments
        chunkiness = 1 + ((len(uuids) - 1)//self.processes)
        if chunkiness > self.chunksize:
            chunkiness = self.chunksize
        tasks = [(uuid, xmin, snapshot_id, restart) for uuid in uuids]
        errors = []
        try:
            for index_item in self.pool.imap_unordered(
                    update_object_in_snapshot,
                    tasks,
                    chunkiness
                ):
                if index_item.error is not None:
                    errors.append(index_item.error)
                self._log_index_item(index_item)
        except:
            self.shutdown()
            raise
        return errors

    def shutdown(self):
        if 'pool' in self.__dict__:
            self.pool.terminate()
            self.pool.join()
            del self.pool
