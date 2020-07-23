import os
import boto3

from botocore.config import Config
from botocore.exceptions import ClientError
from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    TransportError,
)
from pyramid.view import view_config
from pyramid.settings import asbool
from sqlalchemy.exc import StatementError
from snovault import (
    COLLECTIONS,
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
    all_types,
    SEARCH_MAX,
    setup_indexing_nodes,
    HEAD_NODE_INDEX,
    INDEXING_NODE_INDEX,
    AWS_REGION,
)
from .simple_queue import SimpleUuidServer

import datetime
import logging
import pytz
import time
import copy
import json
import requests

es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.ERROR)
log = logging.getLogger('snovault.elasticsearch.es_index_listener')
MAX_CLAUSES_FOR_ES = 8192
DEFAULT_QUEUE = 'Simple'
# Allow three minutes for indexer to stop before head indexer process continues
_REMOTE_INDEXING_SHUTDOWN_SLEEP = 3*60
# Allow indexer to remain up for 30 minutes after indexing before being shutdown
_REMOTE_INDEXING_SHUTDOWN = 15*60
# Allow indexer 20 minutes to startup before head indexer thread takes over indexing
_REMOTE_INDEXING_STARTUP = 10*60
# The number of uuids needed to start a remote indexer
_REMOTE_INDEXING_THRESHOLD = 10000


def _update_for_uuid_queues(registry):
    """
    Update registry with uuid queue module if it exists
    """
    extra_queues = []
    try:
        import snovault.elasticsearch.uuid_queue as queue_adapter
    except ImportError as ecp:
        log.info('No uuid_queue package in elasticsearch module: %s', repr(ecp))
    else:
        registry['UuidQueue'] = queue_adapter.QueueAdapter
        extra_queues = queue_adapter.QueueTypes.get_all()
        log.info('Extra Indexer Queues Available: %s', ','.join(extra_queues))
    registry['available_queues'].extend(extra_queues)


def includeme(config):
    """Add index listener endpoint and setup Indexer"""
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    processes = registry.settings.get('indexer.processes')
    is_indexer = registry.settings.get('indexer')
    if is_indexer:
        available_queues = [DEFAULT_QUEUE]
        registry['available_queues'] = available_queues
        _update_for_uuid_queues(registry)
        if not processes:
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

    query = {
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
    res = es.search(index=RESOURCES_INDEX, size=SEARCH_MAX, request_timeout=60, body=query)

    if res['hits']['total'] > SEARCH_MAX:
        return (list(all_uuids(request.registry)), True)  # guaranteed unique

    related_set = {hit['_id'] for hit in res['hits']['hits']}

    return (related_set, False)


def _determine_indexing_protocol(request, uuid_count):
    remote_indexing = asbool(request.registry.settings.get('remote_indexing', False))
    if not remote_indexing:
        return False
    try:
        indexer_short_uuids = int(request.registry.settings.get('indexer_short_uuids', 0))
        remote_indexing_threshold = int(
            request.registry.settings.get(
                'remote_indexing_threshold',
                _REMOTE_INDEXING_THRESHOLD,
            )
        )
        if indexer_short_uuids > 0:
            uuid_count = indexer_short_uuids
    except ValueError:
        log.warning('ValueError casting remote indexing threshold to an int')
        remote_indexing_threshold = _REMOTE_INDEXING_THRESHOLD
    if uuid_count < remote_indexing_threshold:
        return False
    return True


def _send_sqs_msg(
            cluster_name,
            remote_indexing,
            body,
            delay_seconds=0,
            save_event=False,
        ):
    did_fail = True
    msg_attrs = {
        'cluster_name': cluster_name,
        'remote_indexing': '0' if remote_indexing else '1',
        'save_event': '0' if save_event else '1',
    }
    msg_attrs = {
        key: {'DataType': 'String', 'StringValue': val}
        for key, val in msg_attrs.items()
    }
    boto_session = boto3.Session(region_name=AWS_REGION)
    sqs_resource = boto_session.resource('sqs')
    watch_dog = 3
    attempts = 0
    while attempts < watch_dog:
        attempts += 1
        try: 
            queue_arn = sqs_resource.get_queue_by_name(QueueName='ec2_scheduler_input')
            if queue_arn:
                response = sqs_resource.meta.client.send_message(
                    QueueUrl=queue_arn.url,
                    DelaySeconds=delay_seconds,
                    MessageAttributes=msg_attrs,
                    MessageBody=body,
                )
                if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
                    log.warning(f"Remote indexing _send_sqs_msg: {body}")
                    did_fail = False
                    break
        except ClientError as ecp:
            error_str = e.response.get('Error', {}).get('Code', 'Unknown')
            log.warning(f"({attempts} of {watch_dog}) Indexer could not send message to queue: {error_str}")
    return did_fail


def _get_nodes(request, indexer_state):
    label = ''
    continue_on = False
    did_timeout = False
    # Get local/remote indexing state
    head_node, indexing_node, time_now, did_fail, is_indexing_node = setup_indexing_nodes(
        request, indexer_state
    )
    this_node = head_node
    other_node = indexing_node
    if is_indexing_node:
        this_node = indexing_node
        other_node = head_node
    if did_fail:
        did_timeout = True
        return this_node, other_node, continue_on, did_timeout
    this_time_in_state = time_now - float(this_node['last_run_time'])
    other_time_in_state = time_now - float(other_node['last_run_time'])
    # Check for early return if indexing is running
    if this_node['node_index'] == HEAD_NODE_INDEX:
        if other_node['done_indexing']:
            label = 'Head node with remote finished indexing.  Reset both es node states.'
            this_node, other_node, time_now, did_fail, is_indexing_node = setup_indexing_nodes(
                request,
                indexer_state,
                reset=True
            )
        elif other_node['started_indexing']:
            # not catching if instance is on but app not running?  maybe the indexer state
            #  needs update a flag saying it is still running
            if other_node['instance_state'] in ['running', 'pending']:
                # label = 'Head node with remote indexing running'
                pass
            else:
                label = 'Head node with remote indexing running, permature shutdown'
                log.warning(f"Indexer permature shutdown: sleep {_REMOTE_INDEXING_SHUTDOWN_SLEEP} seconds")
                time.sleep(_REMOTE_INDEXING_SHUTDOWN_SLEEP)
                continue_on = True
        elif this_node['waiting_on_remote']:
            label = 'Head node waiting on remote indexing to start'
            if this_time_in_state > _REMOTE_INDEXING_STARTUP:
                this_node, other_node, time_now, did_fail, is_indexing_node = setup_indexing_nodes(
                    request,
                    indexer_state,
                    reset=True
                )
                if not other_node['instance_state'] in ['stopped', 'stopping']:
                    instance_name = this_node['instance_name']
                    remote_indexing = False
                    _ = _send_sqs_msg(
                        f"{instance_name}-indexer",
                        remote_indexing,
                        'Turn OFF remote indexer for failed start',
                        delay_seconds=0,
                        save_event=False
                    )
                    log.warning(f"Indexer failed start shutdown sleep: {_REMOTE_INDEXING_SHUTDOWN_SLEEP} seconds")
                    time.sleep(_REMOTE_INDEXING_SHUTDOWN_SLEEP)
                else:
                    # log.warning('Indexer failed start shutdown: Already stopped')
                    pass
                did_timeout = True
        else:
            # label = 'Head node says no indexing going on'
            if this_time_in_state > _REMOTE_INDEXING_SHUTDOWN:
                this_node, other_node, time_now, did_fail, is_indexing_node = setup_indexing_nodes(
                    request,
                    indexer_state,
                    reset=True
                )
                if not other_node['instance_state'] in ['stopped', 'stopping']:
                    label = 'Head node says no indexing going on - and shutting down indexer'
                    instance_name = this_node['instance_name']
                    remote_indexing = False
                    _ = _send_sqs_msg(
                        f"{instance_name}-indexer",
                        remote_indexing,
                        'Turn OFF remote indexer for shutdown',
                        delay_seconds=0,
                        save_event=False
                    )
                    log.warning(f"Indexer timeout shutdown: sleep {_REMOTE_INDEXING_SHUTDOWN_SLEEP} seconds")
                    time.sleep(_REMOTE_INDEXING_SHUTDOWN_SLEEP)
                else:
                    # log.warning('Indexer timeout shutdown: Already stopped')
                    pass
            continue_on = True
    elif this_node['node_index'] == INDEXING_NODE_INDEX:
        if other_node['waiting_on_remote']:
            if not this_node['started_indexing']:
                label = 'Index node received signal to start indexing'
                continue_on = True
            elif this_node['done_indexing']:
                label = 'Index node finished indexing.  Waiting on head node to pick up status.'
            elif this_node['started_indexing']:
                label = 'Index node has just finished indexing'
                this_node['done_indexing'] = True
                this_node['last_run_time'] = str(float(time.time()))
                indexer_state.put_obj(this_node['node_index'], this_node)
            else:
                label = 'Index node is in limbo with head node waiting_on_remote.  May correct on next loop.'
        else:
            label = 'Index node finished indexing and has been reset by head node.  Waiting to be shutdown by head node.'
    if label:
        if other_node['instance_state'] == 'stopped':
            log.warning(f"Remote indexing _get_nodes: {label}. this_time: {this_time_in_state:0.6f}, other_time: stopped")
        else:
            log.warning(f"Remote indexing _get_nodes: {label}. this_time: {this_time_in_state:0.6f}, other_time: {other_time_in_state:0.6f}")
    return this_node, other_node, continue_on, did_timeout


@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    # Setting request.datastore here only works because routed views are not traversed.
    request.datastore = 'database'
    session = request.registry[DBSESSION]()
    connection = session.connection()
    # Currently 2 possible followup indexers (base.ini [set stage_for_followup = vis_indexer, region_indexer])
    stage_for_followup = list(request.registry.settings.get("stage_for_followup", '').replace(' ','').split(','))
    # May have undone uuids from prior cycle
    indexer_state = IndexerState(
        request.registry[ELASTIC_SEARCH],
        request.registry.settings['snovault.elasticsearch.index'],
        followups=stage_for_followup,
    )
    # Check remote indexing and set head/indexing nodes
    this_node = indexer_state.get_obj(INDEXING_NODE_INDEX)
    other_node = indexer_state.get_obj(HEAD_NODE_INDEX)
    remote_indexing = asbool(request.registry.settings.get('remote_indexing', False))
    did_timeout = False
    if remote_indexing:
        this_node, other_node, continue_on, did_timeout = _get_nodes(request, indexer_state)
        if not did_timeout and not continue_on:
            # Node state implies we do not need to run any code right now
            return {'this_node': this_node, 'other_node': other_node}
    elif this_node and other_node:
        # if remote node states exists and remote_indexing false then this is the remote indexer
        this_node, other_node, continue_on, did_timeout = _get_nodes(request, indexer_state)
        if not did_timeout and not continue_on:
            # Node state implies we do not need to run any code right now
            return {'this_node': this_node, 'other_node': other_node}

    def _load_indexing(request, session, connection, indexer_state):
        first_txn = None
        snapshot_id = None
        (xmin, invalidated, restart) = indexer_state.priority_cycle(request)
        indexer_state.log_reindex_init_state()
        # OPTIONAL: restart support
        if restart:  # Currently not bothering with restart!!!
            xmin = -1
            invalidated = []
        # OPTIONAL: restart support

        result = indexer_state.get_initial_state()  # get after checking priority!

        if xmin == -1 or len(invalidated) == 0:
            xmin = get_current_xmin(request)

            last_xmin = None
            if 'last_xmin' in request.json:
                last_xmin = request.json['last_xmin']
            else:
                status = request.registry[ELASTIC_SEARCH].get(
                    index=request.registry.settings['snovault.elasticsearch.index'],
                    doc_type='meta',
                    id='indexing',
                    ignore=[400, 404]
                )
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
                    indexer_state.send_notices()
                    return_now = True
                    return result, return_now, flush, invalidated, first_txn, snapshot_id, restart, xmin

                is_testing = asbool(request.registry.settings.get('testing', False))
                is_testing_full = request.json.get('is_testing_full', False)
                if is_testing and is_testing_full:
                    full_reindex = False
                    related_set = set(all_uuids(request.registry))
                else:
                    (related_set, full_reindex) = get_related_uuids(request, request.registry[ELASTIC_SEARCH], updated, renamed)
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

                if invalidated and not request.json.get('dry_run', False):
                    # Exporting a snapshot mints a new xid, so only do so when required.
                    # Not yet possible to export a snapshot on a standby server:
                    # http://www.postgresql.org/message-id/CAHGQGwEtJCeHUB6KzaiJ6ndvx6EFsidTGnuLwJ1itwVH0EJTOA@mail.gmail.com
                    if snapshot_id is None and not request.json.get('recovery', False):
                        snapshot_id = connection.execute('SELECT pg_export_snapshot();').scalar()
        return_now = False
        return result, invalidated, flush, first_txn, snapshot_id, restart, xmin, return_now


    def _run_indexing(
            request,
            indexer_state,
            result,
            invalidated,
            stage_for_followup,
            flush,
            snapshot_id,
            restart,
            xmin,
        ):
        if len(stage_for_followup) > 0:
            # Note: undones should be added before, because those uuids will (hopefully) be indexed in this cycle
            indexer_state.prep_for_followup(xmin, invalidated)

        result = indexer_state.start_cycle(invalidated, result)

        # Do the work...

        indexing_update_infos, errors, err_msg = request.registry[INDEXER].serve_objects(
            request,
            invalidated,
            xmin,
            snapshot_id=snapshot_id,
            restart=restart,
        )
        if err_msg:
            log.warning('Could not start indexing: %s', err_msg)
        result = indexer_state.finish_cycle(result,errors)

        if errors:
            result['errors'] = errors

        if request.json.get('record', False):
            try:
                request.registry[ELASTIC_SEARCH].index(
                    index=request.registry.settings['snovault.elasticsearch.index'],
                    doc_type='meta',
                    body=result,
                    id='indexing'
                )
            except:
                error_messages = copy.deepcopy(result['errors'])
                del result['errors']
                request.registry[ELASTIC_SEARCH].index(
                    index=request.registry.settings['snovault.elasticsearch.index'],
                    doc_type='meta',
                    body=result,
                    id='indexing'
                )
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error for {}, error message: {}'.format(item['uuid'], item['error_message']))
                        item['error_message'] = "Error occured during indexing, check the logs"
                result['errors'] = error_messages


        request.registry[ELASTIC_SEARCH].indices.refresh(RESOURCES_INDEX)
        if flush:
            try:
                request.registry[ELASTIC_SEARCH].indices.flush_synced(index=RESOURCES_INDEX)  # Faster recovery on ES restart
            except ConflictError:
                pass
        return result, indexing_update_infos

    # Load indexing
    output_tuple = _load_indexing(request, session, connection, indexer_state)
    result, invalidated, flush, first_txn, snapshot_id, restart, xmin, return_now = output_tuple
    if return_now:
        return result
    indexing_update_infos = []
    dry_run = request.json.get('dry_run', False)
    if invalidated and not dry_run:
        run_indexing_vars = (
            request,
            indexer_state,
            result,
            invalidated,
            stage_for_followup,
            flush,
            snapshot_id,
            restart,
            xmin,
        )
        if this_node:
            if this_node['node_index'] == HEAD_NODE_INDEX:
                # Check remote indexing
                uuids_count = len(invalidated)
                do_remote_indexing = _determine_indexing_protocol(request, uuids_count)
                if did_timeout:
                    log.warning(f"Remote indexing index: did_timeout.  Run on head node.")
                    head_node, indexing_node, time_now, did_fail, is_indexing_node  = setup_indexing_nodes(
                        request,
                        indexer_state,
                        reset=True
                    )
                    result, indexing_update_infos = _run_indexing(*run_indexing_vars)
                elif do_remote_indexing:
                    log.warning(f"Remote indexing run indexing: Defer to remote.")
                    instance_name = this_node['instance_name']
                    did_fail = False
                    if not other_node['instance_state'] in ['running', 'pending']:
                        did_fail = _send_sqs_msg(
                            f"{instance_name}-indexer",
                            remote_indexing,
                            'Turn ON remote indexer',
                            delay_seconds=0,
                            save_event=False
                        )
                    else:
                        log.warning(f"Remote indexing run indexing: Already on.")
                    if not did_fail:
                        this_node['waiting_on_remote'] = True
                        this_node['last_run_time'] = str(float(time.time()))
                        indexer_state.put_obj(this_node['node_index'], this_node)
                    else:
                        log.warning(f"Remote indexing index: Failed sqs queue.  Run on head node.")
                        result, indexing_update_infos = _run_indexing(*run_indexing_vars)
                else:
                    log.warning(f"Remote indexing index: Below threshold. Run on head node.")
                    result, indexing_update_infos = _run_indexing(*run_indexing_vars)
                    # Do not reset state for local indexing below threshold.
            elif this_node['node_index'] == INDEXING_NODE_INDEX:
                log.warning(f"Remote indexing indexing node: Start indexing")
                this_node['started_indexing'] = True
                this_node['last_run_time'] = str(float(time.time()))
                indexer_state.put_obj(this_node['node_index'], this_node)
                result, indexing_update_infos = _run_indexing(*run_indexing_vars)
                this_node['done_indexing'] = True
                this_node['last_run_time'] = str(float(time.time()))
                indexer_state.put_obj(this_node['node_index'], this_node)
                log.warning(f"Remote indexing indexing node: Done indexing")
        else:
            log.warning(f"Non remote indexing")
            result, indexing_update_infos = _run_indexing(*run_indexing_vars)
    elif invalidated:
        log.warning(f"Load indexing have uuids but dry_run")
    elif dry_run:
        log.warning(f"Load indexing no uuids on dry_run")
    # Add transaction lag to result
    if first_txn is not None:
        result['txn_lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)
    # Send slack notifications
    indexer_state.send_notices()
    # Log indexing times
    if indexing_update_infos:
        # Check for logging of intitial indexing info here,
        #  opposed to in the indexer or just after serve_objects,
        #  so a crash in logging does not interupt indexing complietion
        request.registry[INDEXER].check_log_indexing_times(indexing_update_infos)
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
        self.queue_server_backup = None
        self.queue_worker = None
        self.chunk_size = None
        self.batch_size = None
        self.worker_runs = []
        if registry.settings.get('indexer'):
            self._setup_queues(registry)

    def check_log_indexing_times(self, update_infos):
        if self.indexer_initial_log and not os.path.exists(self.indexer_initial_log_path):
            log.warning('Logging indexing data to %s', self.indexer_initial_log_path)
            counter = 0
            with open(self.indexer_initial_log_path, 'w', encoding='utf-8') as file_handler:
                for update_info in update_infos:
                    str_update_info = json.dumps(update_info, ensure_ascii=False)
                    file_handler.write(str_update_info + '\n')
                    counter += 1
            log.warning('Logged %d uuids.  One per line' % counter)

    def _setup_queues(self, registry):
        '''Init helper - Setup server and worker queues'''
        self.index = registry.settings['snovault.elasticsearch.index']
        self.indexer_initial_log = asbool(registry.settings.get('indexer_initial_log', False))
        self.indexer_initial_log_path = registry.settings.get('indexer_initial_log_path')
        try:
            self.indexer_short_uuids = int(registry.settings.get('indexer_short_uuids'))
        except Exception as ecp:
            log.warning('indexer_short_uuids could not be cast to int.  Defaulting to all.')
            self.indexer_short_uuids = 0
        queue_type = registry.settings.get('queue_type', None)
        is_queue_server = asbool(registry.settings.get('queue_server'))
        is_queue_worker = asbool(registry.settings.get('queue_worker'))
        queue_options = self._get_queue_options(registry)
        self.chunk_size = queue_options['chunk_size']
        self.batch_size = queue_options['batch_size']
        if is_queue_server:
            cp_q_ops = queue_options.copy()
            cp_q_ops['batch_size'] = cp_q_ops['get_size']
            self.queue_server_backup = SimpleUuidServer(cp_q_ops)
            if (
                    not queue_type or
                    queue_type == DEFAULT_QUEUE or
                    queue_type not in registry['available_queues']
                ):
                self.queue_type = DEFAULT_QUEUE
                self.queue_server = self.queue_server_backup
                self.queue_server_backup = None
            elif 'UuidQueue' in registry:
                try:
                    queue_options['uuid_len'] = 36
                    self.queue_server = registry['UuidQueue'](
                        queue_options['queue_name'],
                        queue_type,
                        queue_options,
                    )
                except Exception as exp:  # pylint: disable=broad-except
                    log.warning(repr(exp))
                    log.warning(
                        'Failed to initialize UuidQueue. Switching to backup.'
                    )
                    self._serve_object_switch_queue()
                else:
                    self.queue_type = queue_type
            else:
                log.error('No queue available for Indexer')
            if self.queue_server and is_queue_worker:
                self.queue_worker = self.queue_server.get_worker()
            log.warning('Primary indexer queue type: %s', self.queue_type)

    @staticmethod
    def _get_queue_options(registry):
        '''Init helper - Extract queue options from registry settings'''
        queue_name = registry.settings.get('queue_name', 'indxQ')
        queue_worker_processes = int(
            registry.settings.get('queue_worker_processes', 1)
        )
        queue_worker_chunk_size = int(
            registry.settings.get('queue_worker_chunk_size', 1024)
        )
        queue_worker_batch_size = int(
            registry.settings.get('queue_worker_batch_size', 5000)
        )
        queue_worker_get_size = int(
            registry.settings.get('queue_worker_get_size', 2000000)
        )
        # Only Used for Redis Queues
        queue_host = registry.settings.get('queue_host', 'localhost')
        queue_port = registry.settings.get('queue_port', 6379)
        queue_db = registry.settings.get('queue_db', 2)
        return {
            'queue_name': queue_name,
            'processes': queue_worker_processes,
            'chunk_size': queue_worker_chunk_size,
            'batch_size': queue_worker_batch_size,
            'get_size': queue_worker_get_size,
            'host': queue_host,
            'port': queue_port,
            'db': queue_db,
        }

    def _serve_object_switch_queue(self, set_worker=False):
        # If a non simple queue server fails we end up back here
        # on the next pass of the index listenter with the original
        # list of uuids(given the functionality of the indexer_state)
        # For any failure we switch to simple backup server permanently.
        self.queue_server = self.queue_server_backup
        self.queue_server_backup = None
        self.queue_type = DEFAULT_QUEUE
        if set_worker:
            self.queue_worker = self.queue_server.get_worker()
    

    def _serve_objects_init(self, uuids):
        err_msg = 'Cannot initialize indexing process: '
        try:
            is_indexing = self.queue_server.is_indexing()
            if is_indexing:
                return err_msg + 'Already Indexing'
            elif not uuids:
                return err_msg + 'No uuids given to Indexer.serve_objects'
        except Exception as exp:  # pylint: disable=broad-except
            log.warning(repr(exp))
            if self.queue_server_backup:
                log.warning('uuid init issue:  Switching to simple server.')
                self._serve_object_switch_queue(set_worker=True)
            else:
                return err_msg + 'Cannot failover to simple queue'
        return None

    def _serve_objects_load_uuids(self, uuids):
        err_msg = None
        try:
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
        except Exception as exp:  # pylint: disable=broad-except
            log.warning(repr(exp))
            err_msg = 'Indexer load uuids failed.'
            if self.queue_server_backup:
                log.warning('uuid load issue:  Switching to simple server')
                self._serve_object_switch_queue(set_worker=True)
        return err_msg

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
        err_msg = self._serve_objects_init(uuids)
        if err_msg:
            return None, errors, err_msg
        # Check for shorting uuids
        if self.indexer_short_uuids:
            short_uuids = []
            for count, uuid in enumerate(uuids, 1):
                if count > self.indexer_short_uuids:
                    break
                short_uuids.append(uuid)
            log.warning(
                'Shorting %d uuids to %d.  New list is %d uuids.',
                len(uuids),
                self.indexer_short_uuids,
                len(short_uuids)
            )
            uuids = short_uuids
        err_msg = self._serve_objects_load_uuids(uuids)
        if err_msg:
            return None, errors, err_msg
        # Run Process Loop
        start_time = time.time()
        self.worker_runs = []
        update_infos = []
        while self.queue_server.is_indexing(errs_cnt=len(errors)):
            if self.queue_worker and not self.queue_worker.is_running:
                # Server Worker
                update_infos, uuids_ran = self.run_worker(
                    request, xmin, snapshot_id, restart
                )
                if not uuids_ran:
                    break
                self.worker_runs.append({
                    'worker_id':self.queue_worker.worker_id,
                    'uuids': uuids_ran,
                })
            # Handling Errors must happen or queue will not stop
            batch_errors = self.queue_server.pop_errors()
            for error in batch_errors:
                errors.append(error)
            if timeout and time.time() - start_time > timeout:
                err_msg = 'Indexer sleep timeout'
                break
        self.queue_server.close_indexing()
        return update_infos, errors, err_msg

    def run_worker(self, request, xmin, snapshot_id, restart):
        '''Run the uuid queue worker'''
        batch_uuids = self.queue_worker.get_uuids(get_all=False)
        log.warning(
            'running %s with %d',
            self.queue_worker.worker_id,
            len(batch_uuids),
        )
        update_infos = []
        if batch_uuids:
            self.queue_worker.is_running = True
            batch_update_infos, batch_errors = self.update_objects(
                request,
                batch_uuids,
                xmin,
                snapshot_id=snapshot_id,
                restart=restart,
            )
            update_infos.extend(batch_update_infos)
            batch_results = {
                'errors': batch_errors,
                'successes': len(batch_uuids) - len(batch_errors),
            }
            err_msg = self.queue_worker.update_finished(batch_results)
            if err_msg:
                log.warning('Issue closing worker: %s', err_msg)
            self.queue_worker.is_running = False
            return update_infos, len(batch_uuids)
        else:
            log.warning('No uudis to run %d', self.queue_worker.get_cnt)
        return update_infos, None

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
        update_infos = []
        for i, uuid in enumerate(uuids):
            update_info = self.update_object(self.es, request, uuid, xmin)
            update_info['return_time'] = time.time()
            update_infos.append(update_info)
            error = update_info.get('error')
            if error is not None:
                print('Error', error)
                errors.append(error)
            if (i + 1) % 1000 == 0:
                log.info('Indexing %d', i + 1)
        return update_infos, errors

    @staticmethod
    def update_object(encoded_es, request, uuid, xmin, restart=False):
        update_info = {
            'uuid': uuid,
            'xmin': xmin,
            'start_time': time.time(),
            'end_time': None,
            'run_time': None,
            'error': None,
            'return_time': None,
        }
        req_info = {
            'start_time': None,
            'end_time': None,
            'run_time': None,
            'errors': [],
            'url': None
        }
        es_info = {
            'start_time': None,
            'end_time': None,
            'run_time': None,
            'backoffs': {},
            'item_type': None,
        }
        request.datastore = 'database'
        last_exc = None
        req_info['start_time'] = time.time()
        backoff = 0
        try:
            req_info['url'] ='/%s/@@index-data/' % uuid
            doc = request.embed(req_info['url'], as_user='INDEXER')
        except StatementError:
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as e:
            msg = 'Error rendering /%s/@@index-data' % uuid
            log.error(msg, exc_info=True)
            last_exc = repr(e)
            req_info['errors'].append(
                {
                    'backoff': backoff,
                    'msg': msg,
                    'last_exc': last_exc,
                }
            )
        req_info['end_time'] = time.time()
        req_info['run_time'] = req_info['end_time'] - req_info['start_time']
        if last_exc is None:
            es_info['start_time'] = time.time()
            es_info['item_type'] = doc['item_type']
            do_break = False
            for backoff in [0, 10, 20, 40, 80]:
                time.sleep(backoff)
                backoff_info = {
                    'start_time': time.time(),
                    'end_time': None,
                    'run_time': None,
                    'error': None,
                }
                try:
                    encoded_es.index(
                        index=doc['item_type'], doc_type=doc['item_type'], body=doc,
                        id=str(uuid), version=xmin, version_type='external_gte',
                        request_timeout=30,
                    )
                except StatementError:
                    # Can't reconnect until invalid transaction is rolled back
                    raise
                except ConflictError:
                    msg = 'Conflict indexing %s at version %d' % (uuid, xmin)
                    log.warning(msg)
                    backoff_info['error'] = {
                        'msg': msg,
                        'last_exc': None,
                    }
                    do_break = True
                except (ConnectionError, ReadTimeoutError, TransportError) as e:
                    msg = 'Retryable error indexing %s: %r' % (uuid, e)
                    log.warning(msg)
                    last_exc = repr(e)
                    backoff_info['error'] = {
                        'msg': msg,
                        'last_exc': last_exc,
                    }
                except Exception as e:
                    msg = 'Error indexing %s' % (uuid)
                    log.error(msg, exc_info=True)
                    last_exc = repr(e)
                    backoff_info['error'] = {
                        'msg': msg,
                        'last_exc': None,
                    }
                    do_break = True
                else:
                    # Get here on success and outside of try
                    do_break = True
                end_time = time.time()
                backoff_info['end_time'] = end_time
                backoff_info['run_time'] = end_time - backoff_info['start_time']
                es_info['backoffs'][str(backoff)] = backoff_info
                if do_break:
                    break
            es_info['end_time'] = time.time()
            es_info['run_time'] = es_info['end_time'] - es_info['start_time']
        update_info['req_info'] = req_info
        update_info['es_info'] = es_info
        if last_exc:
            update_info['error'] = {
                'error_message': last_exc,
                'timestamp': datetime.datetime.now().isoformat(),
                'uuid': str(uuid)
            }
        end_time = time.time()
        update_info['end_time'] = end_time
        update_info['run_time'] = end_time - update_info['start_time']
        return update_info

    def shutdown(self):
        pass
