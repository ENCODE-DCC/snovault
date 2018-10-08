'''Indexer Listener Callback'''
import datetime
import logging
import copy
import itertools

import pytz

from elasticsearch.exceptions import ConflictError as ESConflictError
from pyramid.view import view_config

from snovault import DBSESSION
from snovault.storage import TransactionRecord

from .indexer_state import (
    IndexerState,
    all_uuids,
    SEARCH_MAX
)
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER
)
from .primary_indexer import PrimaryIndexer

log = logging.getLogger('snovault.elasticsearch.es_index_listener')  # pylint: disable=invalid-name
MAX_CLAUSES_FOR_ES = 8192
SHORT_INDEXING = None  # Falsey value will turn off short
PY2 = sys.version_info.major == 2


def includeme(config):
    '''Initialize ES Indexers'''
    config.add_route('index', '/index')
    config.scan(__name__)
    is_indexer = asbool(
        config.registry.settings.get(INDEXER, False)
    )
    processes = get_processes(config.registry)
    if is_indexer and not config.registry.get(INDEXER):
        if processes == 1 or PY2:
            log.info('Initialized Single %s', INDEXER)
            config.registry[INDEXER] = PrimaryIndexer(config.registry)
        else:
            log.info('Initialized Multi %s', INDEXER)
            config.registry[INDEXER] = MPIndexer(
                config.registry,
                processes=processes
            )


def get_processes(registry):
    '''Get indexer processes as integer'''
    processes = registry.settings.get('indexer.processes')
    try:
        processes = int(processes)
    except [TypeError, ValueError]:
        processes = None
    return processes


def _run_index(index_listener, indexer_state, result, restart, snapshot_id):
    '''
    Helper for index view_config function
    Runs the indexing processes for index listener
    '''
    if indexer_state.followups:
        indexer_state.prep_for_followup(index_listener.xmin, index_listener.invalidated)
    result = indexer_state.start_cycle(index_listener.invalidated, result)
    errors = index_listener.request.registry[INDEXER].update_objects(
        index_listener.request,
        index_listener.invalidated,
        index_listener.xmin,
        snapshot_id,
        restart
    )
    result = indexer_state.finish_cycle(result, errors)
    if errors:
        result['errors'] = errors


def _do_record(index_listener, result):
    '''
    Helper for index view_config function
    Runs after _run_index if record parameter was in request
    '''
    try:
        index_listener.registry_es.index(
            index=index_listener.index_registry_key,
            doc_type='meta',
            body=result,
            id='indexing'
        )
    except Exception as ecp:  # pylint: disable=broad-except
        log.warning('Index listener: %r', ecp)
        error_messages = copy.deepcopy(result['errors'])
        del result['errors']
        index_listener.registry_es.index(
            index=index_listener.index_registry_key,
            doc_type='meta',
            body=result,
            id='indexing'
        )
        for item in error_messages:
            if 'error_message' in item:
                log.error(
                    'Indexing error for %s, error message: %s',
                    item['uuid'],
                    item['error_message']
                )
                item['error_message'] = "Error occured during indexing, check the logs"
        result['errors'] = error_messages


def get_current_xmin(request):
    '''Determine Postgres minimum transaction'''
    session = request.registry[DBSESSION]()
    connection = session.connection()
    recovery = request.json.get('recovery', False)
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
    xmin = query.scalar()  # lowest xid that is still in progress
    return xmin


def get_related_uuids(request, registry_es, updated, renamed):
    '''
    Returns (set of uuids, False) or
    (list of all uuids, True) if full reindex triggered
    '''
    updated_count = len(updated)
    renamed_count = len(renamed)
    if (updated_count + renamed_count) > MAX_CLAUSES_FOR_ES:
        return (list(all_uuids(request.registry)), True)
    elif (updated_count + renamed_count) == 0:
        return (set(), False)
    registry_es.indices.refresh('_all')
    res = registry_es.search(
        index='_all',
        size=SEARCH_MAX,
        request_timeout=60,
        body={
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
        }
    )
    if res['hits']['total'] > SEARCH_MAX:
        return (list(all_uuids(request.registry)), True)
    related_set = {hit['_id'] for hit in res['hits']['hits']}
    return (related_set, False)


class IndexListener(object):
    '''Encapsulated index view config functionality'''
    def __init__(self, request):
        self.session = request.registry[DBSESSION]()
        self.dry_run = request.json.get('dry_run', False)
        self.index_registry_key = request.registry.settings['snovault.elasticsearch.index']
        self.invalidated = []
        self.registry_es = request.registry[ELASTIC_SEARCH]
        self.request = request
        self.xmin = -1

    def _get_transactions(self, last_xmin, first_txn=None):
        'Check postgres transaction with last xmin'''
        txns = self.session.query(TransactionRecord).filter(
            TransactionRecord.xid >= last_xmin,
        )
        self.invalidated = set(self.invalidated)
        updated = set()
        renamed = set()
        max_xid = 0
        txn_count = 0
        for txn in txns.all():
            txn_count += 1
            max_xid = max(max_xid, txn.xid)
            if not first_txn:
                first_txn = txn.timestamp
            else:
                first_txn = min(first_txn, txn.timestamp)
            renamed.update(txn.data.get('renamed', ()))
            updated.update(txn.data.get('updated', ()))
        return renamed, updated, txn_count, max_xid, first_txn

    def get_current_last_xmin(self, result):
        '''Handle xmin and last_xmin'''
        xmin = get_current_xmin(self.request)
        last_xmin = None
        if 'last_xmin' in self.request.json:
            last_xmin = self.request.json['last_xmin']
        else:
            status = self.registry_es.get(
                index=self.index_registry_key,
                doc_type='meta',
                id='indexing',
                ignore=[400, 404]
            )
            if status['found'] and 'xmin' in status['_source']:
                last_xmin = status['_source']['xmin']
        if last_xmin is None:
            if 'last_xmin' in result:
                last_xmin = result['last_xmin']
            elif 'xmin' in result and result['xmin'] < xmin:
                last_xmin = result['state']
        return xmin, last_xmin

    def get_txns_and_update(self, last_xmin, result):
        '''Get PG Transaction and check again invalidated'''
        (renamed, updated, txn_count,
         max_xid, first_txn) = self._get_transactions(last_xmin)
        result['txn_count'] = txn_count
        if txn_count == 0 and not self.invalidated:
            return None, txn_count
        flush = None
        if self.invalidated:
            updated |= self.invalidated
        related_set, full_reindex = get_related_uuids(
            self.request,
            self.registry_es,
            updated,
            renamed
        )
        if full_reindex:
            self.invalidated = related_set
            flush = True
        else:
            self.invalidated = related_set | updated
            result.update(
                max_xid=max_xid,
                renamed=renamed,
                updated=updated,
                referencing=len(related_set),
                invalidated=len(self.invalidated),
            )
            if first_txn is not None:
                result['first_txn_timestamp'] = first_txn.isoformat()
        return flush, txn_count

    def set_priority_cycle(self, indexer_state):
        '''Call priority cycle and update self'''
        (xmin, invalidated, restart) = indexer_state.priority_cycle(self.request)
        indexer_state.log_reindex_init_state()
        # Currently not bothering with restart!!!
        if restart:
            xmin = -1
            invalidated = []
        self.invalidated = invalidated
        self.xmin = xmin
        return restart

    def short_uuids(self, short_to=100):
        '''
        Limit uuids to index for debugging
        '''
        if short_to <= 0:
            short_to = 100
        self.invalidated = set(
            itertools.islice(self.invalidated, short_to)
        )

    def try_set_snapshot_id(self, recovery, snapshot_id):
        '''Check for snapshot_id in postgres under certain conditions'''
        if self.invalidated and not self.dry_run:
            if snapshot_id is None and not recovery:
                connection = self.session.connection()
                snapshot_id = connection.execute(
                    'SELECT pg_export_snapshot();'
                ).scalar()
        return snapshot_id


@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    '''Index listener for main indexer'''
    request.datastore = 'database'
    followups = list(
        request.registry.settings.get(
            "stage_for_followup",
            ''
        ).replace(' ', '').split(',')
    )
    index_listener = IndexListener(request)
    indexer_state = IndexerState(
        index_listener.registry_es,
        index_listener.index_registry_key,
        followups=followups
    )
    restart = index_listener.set_priority_cycle(indexer_state)
    result = indexer_state.get_initial_state()
    snapshot_id = None
    first_txn = None
    last_xmin = None
    if index_listener.xmin == -1 or not index_listener.invalidated:
        tmp_xmin, last_xmin = index_listener.get_current_last_xmin(result)
        result.update(
            xmin=tmp_xmin,
            last_xmin=last_xmin,
        )
        index_listener.xmin = tmp_xmin
    flush = False
    if len(index_listener.invalidated) > SEARCH_MAX:
        flush = True
    elif last_xmin is None:
        result['types'] = types = request.json.get('types', None)
        index_listener.invalidated = list(all_uuids(request.registry, types))
        flush = True
    else:
        tmp_flush, txn_count = index_listener.get_txns_and_update(last_xmin, result)
        if txn_count == 0 and not index_listener.invalidated:
            indexer_state.send_notices()
            return result
        if tmp_flush:
            flush = tmp_flush
        snapshot_id = index_listener.try_set_snapshot_id(
            request.json.get('recovery', False),
            snapshot_id
        )
    if index_listener.invalidated and not index_listener.dry_run:
        if SHORT_INDEXING:
            # If value is truthly then uuids will be limited.
            index_listener.short_uuids(SHORT_INDEXING)
        _run_index(index_listener, indexer_state, result, restart, snapshot_id)
        if request.json.get('record', False):
            _do_record(index_listener, result)
        index_listener.registry_es.indices.refresh('_all')
        if flush:
            try:
                index_listener.registry_es.indices.flush_synced(index='_all')
            except ESConflictError as ecp:
                log.warning('Index listener ESConflictError: %r', ecp)
    if first_txn is not None:
        result['txn_lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)
    indexer_state.send_notices()
    return result
