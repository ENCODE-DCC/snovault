from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    TransportError,
)
from pyramid.view import view_config
from pyramid.settings import asbool
from sqlalchemy.exc import StatementError
from snovault import (
    DBSESSION,
    STORAGE
)
from snovault.storage import (
    TransactionRecord,
)
from urllib3.exceptions import ReadTimeoutError
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER,
    RESOURCES_INDEX,
)
from .indexer_state import (
    IndexerState,
    all_uuids,
    SEARCH_MAX
)
from .simple_queue import SimpleUuidServer

import datetime
import logging
import pytz
import time
import copy

es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.ERROR)
log = logging.getLogger('snovault.elasticsearch.es_index_listener')
MAX_CLAUSES_FOR_ES = 8192


f = open('/srv/encoded/indexing.log', 'a')


def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    available_queues = ['Simple']
    registry['available_queues'] = available_queues
    log.info('Indexer Queues Available: %s' % ','.join(available_queues))
    registry[INDEXER] = Indexer(registry)

def get_related_uuids(request, es, updated, renamed):
    '''Returns (set of uuids, False) or (list of all uuids, True) if full reindex triggered'''

    updated_count = len(updated)
    renamed_count = len(renamed)
    if (updated_count + renamed_count) > MAX_CLAUSES_FOR_ES:
        return (list(all_uuids(request.registry)), True)  # guaranteed unique
    elif (updated_count + renamed_count) == 0:
        return (set(), False)

    es.indices.refresh(RESOURCES_INDEX)

    # TODO: batching may allow us to drive a partial reindexing much greater than 99999
    #BATCH_COUNT = 100  # NOTE: 100 random uuids returned > 99999 results!
    #beg = 0
    #end = BATCH_COUNT
    #related_set = set()
    #updated_list = list(updated)  # Must be lists
    #renamed_list = list(renamed)
    #while updated_count > beg or renamed_count > beg:
    #    if updated_count > end or beg > 0:
    #        log.error('Indexer looking for related uuids by BATCH[%d,%d]' % (beg, end))
    #
    #    updated = []
    #    if updated_count > beg:
    #        updated = updated_list[beg:end]
    #    renamed = []
    #    if renamed_count > beg:
    #        renamed = renamed_list[beg:end]
    #
    #     search ...
    #     accumulate...
    #
    #    beg += BATCH_COUNT
    #    end += BATCH_COUNT


    res = es.search(index=RESOURCES_INDEX, size=SEARCH_MAX, request_timeout=60, body={
        'query': {
            'bool': {
                'should': [
                    {
                        'terms': {
                            'embedded_uuids': updated,
                            '_cache': False,
                        },
                    },
                    {
                        'terms': {
                            'linked_uuids': renamed,
                            '_cache': False,
                        },
                    },
                ],
            },
        },
        '_source': False,
    })

    if res['hits']['total'] > SEARCH_MAX:
        return (list(all_uuids(request.registry)), True)  # guaranteed unique

    related_set = {hit['_id'] for hit in res['hits']['hits']}

    return (related_set, False)



@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    INDEX = request.registry.settings['snovault.elasticsearch.index']
    # Setting request.datastore here only works because routed views are not traversed.
    request.datastore = 'database'
    record = request.json.get('record', False)
    dry_run = request.json.get('dry_run', False)
    recovery = request.json.get('recovery', False)
    es = request.registry[ELASTIC_SEARCH]
    indexer = request.registry[INDEXER]
    session = request.registry[DBSESSION]()
    connection = session.connection()
    first_txn = None
    snapshot_id = None
    restart=False
    invalidated = []
    xmin = -1

    # Currently 2 possible followup indexers (base.ini [set stage_for_followup = vis_indexer, region_indexer])
    stage_for_followup = list(request.registry.settings.get("stage_for_followup", '').replace(' ','').split(','))

    # May have undone uuids from prior cycle
    state = IndexerState(es, INDEX, followups=stage_for_followup)

    (xmin, invalidated, restart) = state.priority_cycle(request)
    state.log_reindex_init_state()
    # OPTIONAL: restart support
    if restart:  # Currently not bothering with restart!!!
        xmin = -1
        invalidated = []
    # OPTIONAL: restart support

    result = state.get_initial_state()  # get after checking priority!

    if xmin == -1 or len(invalidated) == 0:
        xmin = get_current_xmin(request)

        last_xmin = None
        if 'last_xmin' in request.json:
            last_xmin = request.json['last_xmin']
        else:
            status = es.get(index=INDEX, doc_type='meta', id='indexing', ignore=[400, 404])
            if status['found'] and 'xmin' in status['_source']:
                last_xmin = status['_source']['xmin']
        if last_xmin is None:  # still!
            if 'last_xmin' in result:
                last_xmin = result['last_xmin']
            elif 'xmin' in result and result['xmin'] < xmin:
                last_xmin = result['state']

        result.update(
            xmin=xmin,
            last_xmin=last_xmin,
        )

    if len(invalidated) > SEARCH_MAX:  # Priority cycle already set up
        flush = True
    else:

        flush = False
        if last_xmin is None:
            result['types'] = types = request.json.get('types', None)
            invalidated = list(all_uuids(request.registry, types))
            flush = True
        else:
            txns = session.query(TransactionRecord).filter(
                TransactionRecord.xid >= last_xmin,
            )

            invalidated = set(invalidated)  # not empty if API index request occurred
            updated = set()
            renamed = set()
            max_xid = 0
            txn_count = 0
            for txn in txns.all():
                txn_count += 1
                max_xid = max(max_xid, txn.xid)
                if first_txn is None:
                    first_txn = txn.timestamp
                else:
                    first_txn = min(first_txn, txn.timestamp)
                renamed.update(txn.data.get('renamed', ()))
                updated.update(txn.data.get('updated', ()))

            if invalidated:        # reindex requested, treat like updated
                updated |= invalidated

            result['txn_count'] = txn_count
            if txn_count == 0 and len(invalidated) == 0:
                state.send_notices()
                return result

            (related_set, full_reindex) = get_related_uuids(request, es, updated, renamed)
            if full_reindex:
                invalidated = related_set
                flush = True
            else:
                invalidated = related_set | updated
                result.update(
                    max_xid=max_xid,
                    renamed=renamed,
                    updated=updated,
                    referencing=len(related_set),
                    invalidated=len(invalidated),
                    txn_count=txn_count
                )
                if first_txn is not None:
                    result['first_txn_timestamp'] = first_txn.isoformat()

            if invalidated and not dry_run:
                # Exporting a snapshot mints a new xid, so only do so when required.
                # Not yet possible to export a snapshot on a standby server:
                # http://www.postgresql.org/message-id/CAHGQGwEtJCeHUB6KzaiJ6ndvx6EFsidTGnuLwJ1itwVH0EJTOA@mail.gmail.com
                if snapshot_id is None and not recovery:
                    snapshot_id = connection.execute('SELECT pg_export_snapshot();').scalar()

    if invalidated and not dry_run:
        if len(stage_for_followup) > 0:
            # Note: undones should be added before, because those uuids will (hopefully) be indexed in this cycle
            state.prep_for_followup(xmin, invalidated)

        result = state.start_cycle(invalidated, result)

        # Do the work...

        errors = indexer.update_objects(request, invalidated, xmin, snapshot_id, restart)

        result = state.finish_cycle(result,errors)

        if errors:
            result['errors'] = errors

        if record:
            try:
                es.index(index=INDEX, doc_type='meta', body=result, id='indexing')
            except Exception as exc:
                error_messages = copy.deepcopy(result['errors'])
                del result['errors']
                es.index(index=INDEX, doc_type='meta', body=result, id='indexing')
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error for {}, error message: {}'.format(item['uuid'], item['error_message']))
                        item['error_message'] = "Error occured during indexing, check the logs"
                result['errors'] = error_messages


        es.indices.refresh(RESOURCES_INDEX)
        if flush:
            try:
                es.indices.flush_synced(index=RESOURCES_INDEX)  # Faster recovery on ES restart
            except ConflictError:
                pass

    if first_txn is not None:
        result['txn_lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)

    state.send_notices()
    return result


def get_current_xmin(request):
    session = request.registry[DBSESSION]()
    connection = session.connection()
    recovery = request.json.get('recovery', False)

    # http://www.postgresql.org/docs/9.3/static/functions-info.html#FUNCTIONS-TXID-SNAPSHOT
    if recovery:
        query = connection.execute(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;"
            "SELECT txid_snapshot_xmin(txid_current_snapshot());"
        )
    else:
        query = connection.execute(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;"
            "SELECT txid_snapshot_xmin(txid_current_snapshot());"
        )
    # DEFERRABLE prevents query cancelling due to conflicts but requires SERIALIZABLE mode
    # which is not available in recovery.
    xmin = query.scalar()  # lowest xid that is still in progress
    return xmin


class Indexer(object):
    def __init__(self, registry):
        self.es = registry[ELASTIC_SEARCH]
        self.esstorage = registry[STORAGE]
        self.index = registry.settings['snovault.elasticsearch.index']
        self.queue_server = None
        self.queue_worker = None
        self.worker_runs = []
        available_queues = registry['available_queues']
        queue_type = registry.settings.get('queue_type', None)
        is_queue_server = asbool(registry.settings.get('queue_server'))
        is_queue_worker = asbool(registry.settings.get('queue_worker'))
        queue_worker_processes = int(
            registry.settings.get('queue_worker_processes', 1)
        )
        queue_worker_chunk_size = int(
            registry.settings.get('queue_worker_chunk_size', 1024)
        )
        queue_worker_batch_size = int(
            registry.settings.get('queue_worker_batch_size', 1024)
        )
        queue_options = {
            'processes': queue_worker_processes,
            'chunk_size': queue_worker_chunk_size,
            'batch_size': queue_worker_batch_size,
        }
        if is_queue_server and queue_type in available_queues:
            if not queue_type or queue_type == 'Simple':
                self.queue_server = SimpleUuidServer(queue_options)
            else:
                log.error('No queue available for Indexer')
            if self.queue_server and is_queue_worker:
                self.queue_worker = self.queue_server.get_worker()

    def serve_objects(
            self,
            request,
            uuids,
            xmin,
            snapshot_id=None,
            restart=False,
            timeout=None,
        ):
        '''Run indexing process with queue server and optional worker'''
        # pylint: disable=too-many-arguments
        errors = []
        err_msg = None
        if self.queue_server.is_indexing():
            err_msg = 'Already Indexing'
        elif not uuids:
            err_msg = 'No uuids given to Indexer.serve_objects'
        else:
            uuids_loaded_len = self.queue_server.load_uuids(uuids)
            if not uuids_loaded_len:
                err_msg = 'Uuids given to Indexer.serve_objects failed to load'
            elif uuids_loaded_len != len(uuids):
                err_msg = (
                    'Uuids given to Indexer.serve_objects '
                    'failed to all load. {} of {} only'.format(
                        uuids_loaded_len,
                        len(uuids),
                    )
                )
        if err_msg is None:
            start_time = time.time()
            self.worker_runs = []
            while self.queue_server.is_indexing():
                if self.queue_worker and not self.queue_worker.is_running:
                    # Server Worker
                    uuids_ran = self.run_worker(request, xmin, snapshot_id, restart)
                    self.worker_runs.append({
                        'worker_id':self.queue_worker,
                        'uuids': uuids_ran,
                    })
                # Handling Errors must happen or queue will not stop
                batch_errors = self.queue_server.pop_errors()
                for error in batch_errors:
                    errors.append(error)
                time.sleep(0.01)
                if timeout and time.time() - start_time > timeout:
                    err_msg = 'Indexer sleep timeout'
                    break
        return errors, err_msg

    def run_worker(self, request, xmin, snapshot_id, restart):
        '''Run the uuid queue worker'''
        batch_uuids = self.queue_worker.get_uuids()
        log.warning(
            'running %s with %d',
            self.queue_worker.worker_id,
            len(batch_uuids),
        )
        if batch_uuids:
            self.queue_worker.is_running = True
            if batch_uuids:
                batch_errors = self.update_objects(
                    request,
                    batch_uuids,
                    xmin,
                    snapshot_id=snapshot_id,
                    restart=restart,
                )
                batch_results = {
                    'errors': batch_errors,
                    'successes': len(batch_uuids) - len(batch_errors),
                }
            err_msg = self.queue_worker.update_finished(batch_results)
            if err_msg:
                log.warning('Issue closing worker: %s', err_msg)
            self.queue_worker.is_running = False
            return len(batch_uuids)
        else:
            log.warning('No uudis to run %d', self.queue_worker.get_cnt)
        return None

    def update_objects(
            self,
            request,
            uuids,
            xmin,
            snapshot_id=None,
            restart=False,
        ):
        # pylint: disable=too-many-arguments, unused-argument
        '''Run indexing process on uuids'''
        errors = []
        for i, uuid in enumerate(uuids):
            error = self.update_object(request, uuid, xmin)
            if error is not None:
                errors.append(error)
            if (i + 1) % 1000 == 0:
                log.info('Indexing %d', i + 1)

        return errors

    def update_object(self, request, uuid, xmin, restart=False):
        start = time.time()
        request.datastore = 'database'  # required by 2-step indexer

        # OPTIONAL: restart support
        # If a restart occurred in the middle of indexing, this uuid might have already been indexd, so skip redoing it.
        # if restart:
        #     try:
        #         #if self.es.exists(index=self.index, id=str(uuid), version=xmin, version_type='external_gte'):  # couldn't get exists to work.
        #         result = self.es.get(index=self.index, id=str(uuid), _source_include='uuid', version=xmin, version_type='external_gte')
        #         if result.get('_source') is not None:
        #             return
        #     except:
        #         pass
        # OPTIONAL: restart support

        last_exc = None

        try:
            doc = request.embed('/%s/@@index-data/' % uuid, as_user='INDEXER')
        except StatementError:
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as e:
            log.error('Error rendering /%s/@@index-data', uuid, exc_info=True)
            last_exc = repr(e)

        es_start = time.time()
        py_elapsed = es_start - start

        if last_exc is None:
            for backoff in [0, 10, 20, 40, 80]:
                time.sleep(backoff)
                try:
                    self.es.index(
                        index=doc['item_type'], doc_type=doc['item_type'], body=doc,
                        id=str(uuid), version=xmin, version_type='external_gte',
                        request_timeout=30,
                    )
                except StatementError:
                    # Can't reconnect until invalid transaction is rolled back
                    raise
                except ConflictError:
                    log.warning('Conflict indexing %s at version %d', uuid, xmin)
                    return
                except (ConnectionError, ReadTimeoutError, TransportError) as e:
                    log.warning('Retryable error indexing %s: %r', uuid, e)
                    last_exc = repr(e)
                except Exception as e:
                    log.error('Error indexing %s', uuid, exc_info=True)
                    last_exc = repr(e)
                    break
                else:
                    es_elapsed = time.time() - es_start
                    f.write('Indexed {} {} {} {} {} {} {}\n'.format(
                        result['paths'][0],
                        result['item_type'],
                        start,
                        py_elapsed,
                        es_elapsed,
                        len(result['embedded_uuids']),
                        len(result['linked_uuids']),
                    ))
                    f.sync()
                    # Get here on success and outside of try
                    return

        timestamp = datetime.datetime.now().isoformat()
        return {'error_message': last_exc, 'timestamp': timestamp, 'uuid': str(uuid)}

    def shutdown(self):
        pass
