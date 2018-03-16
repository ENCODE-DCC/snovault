from snovault import DBSESSION
from contextlib import contextmanager
from multiprocessing import get_context
from multiprocessing.pool import Pool
from pyramid.decorator import reify
from pyramid.request import apply_request_extensions
from pyramid.threadlocal import (
    get_current_request,
    manager,
)
import atexit
import logging
import time
import transaction
from .indexer import (
    INDEXER,
    Indexer,
)
from .interfaces import (
    APP_FACTORY,
    INDEXER_QUEUE
)

log = logging.getLogger(__name__)


def includeme(config):
    if config.registry.settings.get('indexer_worker'):
        return
    processes = config.registry.settings.get('indexer.processes')
    try:
        processes = int(processes)
    except:
        processes = None
    config.registry[INDEXER] = MPIndexer(config.registry, processes=processes)


### Running in subprocess

app = None


def initializer(app_factory, settings):
    """
    Need to initialize the app for the subprocess
    """
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    global app
    atexit.register(clear_manager)
    app = app_factory(settings, indexer_worker=True, create_tables=False)
    signal.signal(signal.SIGALRM, clear_manager)


@contextmanager
def threadlocal_manager():
    """
    Set registry and request attributes using the global app within the
    subprocess
    """
    import signal
    signal.alarm(0)
    clear_manager()
    registry = app.registry
    request = app.request_factory.blank('/_indexing_pool')
    request.registry = registry
    request.datastore = 'database'
    apply_request_extensions(request)
    request.invoke_subrequest = app.invoke_subrequest
    request.root = app.root_factory(request)
    request._stats = {}
    manager.push({'request': request, 'registry': registry})
    yield
    signal.alarm(5)


def clear_manager(signum=None, frame=None):
    manager.pop()

### These helper functions are needed for multiprocessing

def sync_update_helper(uuid):
    with threadlocal_manager():
        request = get_current_request()
        indexer = request.registry[INDEXER]
        return indexer.update_object(request, uuid)


def queue_update_helper():
    with threadlocal_manager:
        request = get_current_request()
        indexer = request.registry[INDEXER]
        return indexer.update_objects_queue(request)

### Running in main process

class MPIndexer(Indexer):
    def __init__(self, registry, processes=None):
        super(MPIndexer, self).__init__(registry)
        self.chunksize = int(registry.settings.get('indexer.chunk_size', 1024))
        self.processes = processes
        self.initargs = (registry[APP_FACTORY], registry.settings,)
        # workers in the pool will be replaced after finishing one task
        # to free memory
        self.maxtasks = 1

    @reify
    def pool(self):
        return Pool(
            processes=self.processes,
            initializer=initializer,
            initargs=self.initargs,
            maxtasksperchild=self.maxtasks,
            context=get_context('forkserver'),
        )

    def update_objects(self, request):
        sync_uuids = request.json.get('uuids', None)
        if sync_uuids:
            # determine how many uuids should be used for each process
            workers = self.pool._processes if self.processes is None else self.processes
            chunkiness = int((len(sync_uuids) - 1) / workers) + 1
            if chunkiness > self.chunksize:
                chunkiness = self.chunksize
            errors = []
            # imap_unordered to hopefully shuffle item types and come up with
            # a more or less equal workload for each process
            for error in self.pool.imap_unordered(
                sync_update_helper, sync_uuids, chunkiness):
                if error is not None:
                    errors.append(error)
            log.warn('___ERRORS (ASYNC): %s' % str(errors))
        else:
            errors = [self.pool.apply_async(queue_update_helper).get()]
        self.pool.close()
        self.pool.join()
        return errors
