from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    TransportError,
)
from pyramid.view import view_config
from sqlalchemy.exc import StatementError
from snovault import (
    COLLECTIONS,
    DBSESSION,
)
from snovault.storage import (
    TransactionRecord,
)
from urllib3.exceptions import ReadTimeoutError
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER
)
from snovault import CONNECTION
import datetime
import logging
import pytz
import time
import copy
import json
from redis import Redis

log = logging.getLogger(__name__)
SEARCH_MAX = 99999  # OutOfMemoryError if too high

def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    registry[INDEXER] = Indexer(registry)


class IndexerState(object):
    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        if self.redis_client is None:
            self.redis_client = Redis(charset="utf-8", decode_responses=True)
        self.redis_pipe = self.redis_client.pipeline()
        self.persistent_key  = 'primary-state'         # State of the current or last cycle
        self.todo_set        = 'primary-invalidated'   # one cycle of uuids, sent to the Secondary Indexer
        self.in_progress_set = 'primary-in-progress'
        self.failed_set      = 'primary-failed'
        self.done_set        = 'primary-done'          # Trying to get all uuids from 'todo' to this set
        self.done_list       = 'primary-done-list'     # Try lpushing to a list to speed things along
        self.troubled_set    = 'primary-troubled'      # uuids that failed to index in any cycle
        self.last_set        = 'primary-last-cycle'    # uuids in the most recent finished cycle
        self.followup_prep_list = 'primary-followup-prep-list' # Setting up the uuids to be handled by a followup process
        self.followup_ready_list = 'staged-for-secondary-list'  # Followup list is added to here to pass baton
        # DO NOT INHERIT! All keys that are cleaned up at the start and fully finished end of indexing
        self.cleanup_keys      = [self.todo_set,self.in_progress_set,self.failed_set,self.done_set]
        self.cleanup_last_keys = [self.last_set]  # ,self.audited_set] cleaned up only when new indexing occurs
        self.cache = {}
        # desired:
        # 1) Hand off to secondary.  Will work fine
        # 2) Record failures and consider blacklisting them
        # 3) Detect and recover from abreviated cycle - probably have to settle for batches
        # 4) Identify what is being currently worked on.  Probably have to accept batches

    def get(self):
        try:
            persistent_state = json.loads(self.redis_client.get(self.persistent_key))
        except:
            persistent_state = {}
        return persistent_state

    def set(self,persistent_state):
        try:
            self.redis_client.set(self.persistent_key,json.dumps(persistent_state))
        except:
            log.warn("Failed to save to redis: " +str(persistent_state))
            pass  # What sould be done?

    def prep_for_followup(self,xmin,uuids):
        self.redis_pipe.rpush(self.followup_prep_list, "xmin:%s" % xmin).rpush(self.followup_prep_list, *list(uuids)).execute()

    def subtract_undone_uuids(self,xmin,uuids):
        # If there are dones from the last cycle that match the xmin, then remove them
        persistent_state = this.get()
        if xmin != int(persistent_state.get('xmin',-1)):
            return uuids
        if not isinstance(uuids,set):
            uuids = set(uuids)
        dones = set(self.redis_client.smembers(self.done_set))
        uuids = uuids - dones
        # TODO: could blacklist uuids with troubled_set or twice_troubled set
        return uuids

    def add_undone_uuids(self,uuids):
        # Adds undone from last cycle and returns set
        if not isinstance(uuids,set):
            uuids = set(uuids)
        undones = list(self.redis_client.sdiff(self.todo_set, self.done_set))
        uuids.update(undones)
        # TODO: could blacklist uuids with troubled_set or twice_troubled set
        return uuids

    def start_cycle(self,uuids,persistent_state=None):
        if persistent_state is not None:
            persistent_state['state'] = 'indexing'
            self.set(persistent_state)
        self.redis_pipe.delete(*self.cleanup_last_keys)
        self.redis_pipe.delete(*self.cleanup_keys).sadd(self.todo_set,*set(uuids)).execute()

    def successes_this_cycle(self):
        # Could be overwritten to change definition of success
        return self.redis_client.scard(self.done_set)

    def finish_cycle(self,persistent_state=None):
        # Convert indexed list to a set
        #dones = set(self.redis_client.lrange(self.done_list, 0, -1))
        #if dones:
        #    self.redis_pipe.sadd(self.done_set, *dones).delete(self.done_list).execute()
        success_count = self.successes_this_cycle()

        # handle troubled uuids:
        troubled_uuids = self.redis_client.smembers(self.failed_set)
        if len(troubled_uuids):
            self.redis_pipe.sadd(self.troubled_set, *troubled_uuids).execute()
        # TODO: Could add in_progress_set as well though it should be assertably empty
        # TODO: could make twice_troubled set and use it to blacklist uuids

        if persistent_state is not None:
            persistent_state['successful'] = success_count
            persistent_state['indexed']    = self.uuids_done()     # may be different from 'successful'

        # If nothing left to do?
        if len(self.redis_client.sdiff(self.todo_set, self.done_set)) <= 0: # bek says 1
            # pass any staged items to followup
            if self.followup_prep_list is not None:
                hand_off_list = self.redis_client.lrange(self.followup_prep_list, 0, -1)
                if len(hand_off_list) > 0:  # Have to push because ready_list may still have previous cycles in it
                    self.redis_pipe.rpush(self.followup_ready_list, *hand_off_list).delete(self.followup_prep_list).execute()
            self.redis_pipe.rename(self.done_set, self.last_set)
            self.redis_pipe.delete(*self.cleanup_keys).execute()
            if persistent_state is not None:
                persistent_state['state'] = 'done'
        else:
            if persistent_state is not None:
                persistent_state['state'] = 'need to cycle'

        if persistent_state is not None:
            persistent_state['cycles'] = persistent_state.get('cycles',0) + 1
            self.set(persistent_state)

        # returns successful count
        return success_count

    def uuids_in_progress(self,uuids=[]):
        if len(uuids):
            self.redis_pipe.sadd(self.in_progress_set, *uuids).execute()
        else:
            return self.redis_client.scard(self.in_progress_set)

    def uuids_done(self,uuids=[]):
        if len(uuids):
            self.redis_pipe.sadd(self.done_set, *uuids).srem(self.in_progress_set,*uuids).execute()
        else:
            return self.redis_client.scard(self.done_set)

    def start_uuid(self,uuid):
        # TODO: get rid of this after seeing times improve
        #self.redis_pipe.sadd(self.in_progress_set, uuid).execute()
        # Don't bother since it ain't THAT useful
        return


    def failed_uuid(self,uuid):
        #self.redis_pipe.sadd(self.failed_set, uuid).srem(self.in_progress_set, uuid).execute()
        self.redis_pipe.sadd(self.failed_set, uuid).execute()  # Hopefully these are rare so just redis it

    def indexed_uuid(self,uuid):
        #self.redis_pipe.sadd(self.done_set, uuid).srem(self.in_progress_set, uuid).execute()
        self.redis_pipe.lpush(self.done_list, uuid).execute()


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
    secondary_indexer = request.registry["secondaryindexer"]

    session = request.registry[DBSESSION]()
    connection = session.connection()
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

    first_txn = None
    last_xmin = None
    if 'last_xmin' in request.json:
        last_xmin = request.json['last_xmin']
    else:
        try:
            status = es.get(index=INDEX, doc_type='meta', id='indexing')
        except NotFoundError:
            interval_settings = {"index": {"refresh_interval": "30s"}}
            es.indices.put_settings(index=INDEX, body=interval_settings)
            pass
        else:
            last_xmin = status['_source']['xmin']

    result = {
        'xmin': xmin,
        'last_xmin': last_xmin,
    }

    flush = False
    if last_xmin is None:
        result['types'] = types = request.json.get('types', None)
        invalidated = list(all_uuids(request.registry, types))
        flush = True
    else:
        txns = session.query(TransactionRecord).filter(
            TransactionRecord.xid >= last_xmin,
        )

        invalidated = set()
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

        result['txn_count'] = txn_count
        if txn_count == 0:
            return result

        es.indices.refresh(index=INDEX)
        res = es.search(index=INDEX, size=SEARCH_MAX, body={
            'filter': {
                'or': [
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
            '_source': False,
        })
        if res['hits']['total'] > SEARCH_MAX:
            invalidated = list(all_uuids(request.registry))
            flush = True
        else:
            referencing = {hit['_id'] for hit in res['hits']['hits']}
            invalidated = referencing | updated
            result.update(
                max_xid=max_xid,
                renamed=renamed,
                updated=updated,
                referencing=len(referencing),
                invalidated=len(invalidated),
                txn_count=txn_count,
                first_txn_timestamp=first_txn.isoformat(),
            )

    # May have undone uuids from prior cycle
    state = IndexerState()
    invalidated = state.add_undone_uuids(invalidated)

    if invalidated and not dry_run:
        # Exporting a snapshot mints a new xid, so only do so when required.
        # Not yet possible to export a snapshot on a standby server:
        # http://www.postgresql.org/message-id/CAHGQGwEtJCeHUB6KzaiJ6ndvx6EFsidTGnuLwJ1itwVH0EJTOA@mail.gmail.com
        snapshot_id = None
        if not recovery:
            snapshot_id = connection.execute('SELECT pg_export_snapshot();').scalar()
        # Store unfinished set after updating it

        if secondary_indexer:
            # Note: undones should be added before, because those uuids will (hopefully) be indexed in this cycle
            state.prep_for_followup(xmin,invalidated)

        state.start_cycle(invalidated,result)

        # Do the work...
        errors = indexer.update_in_batches(request, invalidated, xmin, snapshot_id)
        if errors:
            result['errors'] = errors

        result['successful'] = state.finish_cycle(result)

        if record:
            try:
                es.index(index=INDEX, doc_type='meta', body=result, id='indexing')
            except:
                error_messages = copy.deepcopy(result['errors'])
                del result['errors']
                es.index(index=INDEX, doc_type='meta', body=result, id='indexing')
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error for {}, error message: {}'.format(item['uuid'], item['error_message']))
                        item['error_message'] = "Error occured during indexing, check the logs"
                result['errors'] = error_messages

            if es.indices.get_settings(index=INDEX)[INDEX]['settings']['index'].get('refresh_interval', '') != '1s':
                interval_settings = {"index": {"refresh_interval": "1s"}}
                es.indices.put_settings(index=INDEX, body=interval_settings)

        es.indices.refresh(index=INDEX)

        if flush:
            try:
                es.indices.flush_synced(index=INDEX)  # Faster recovery on ES restart
            except ConflictError:
                pass

    if first_txn is not None:
        result['lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)
        state.set(result)

    return result


def all_uuids(registry, types=None):
    # First index user and access_key so people can log in
    collections = registry[COLLECTIONS]
    initial = ['user', 'access_key']
    for collection_name in initial:
        collection = collections.by_item_type.get(collection_name, [])
        # for snovault test application, there are no users or keys
        if types is not None and collection_name not in types:
            continue
        for uuid in collection:
            yield str(uuid)
    for collection_name in sorted(collections.by_item_type):
        if collection_name in initial:
            continue
        if types is not None and collection_name not in types:
            continue
        collection = collections.by_item_type[collection_name]
        for uuid in collection:
            yield str(uuid)


class Indexer(object):
    def __init__(self, registry):
        self.es = registry[ELASTIC_SEARCH]
        self.index = registry.settings['snovault.elasticsearch.index']
        self.redis_client = Redis(connection_pool=registry[CONNECTION].redis_pool, charset="utf-8", decode_responses=True)
        self.state = IndexerState(self.redis_client)
        try:
            self.batch_size = int(registry.settings["indexer.batch_size"])  # found in buildout.cfg for production
        except:
            self.batch_size = 100

    def update_in_batches(self, request, uuids, xmin, snapshot_id):
        errors = []
        batch_count = 0
        while uuids:
            batch_uuids = []
            batch_started = datetime.datetime.now(pytz.utc)
            while uuids:
                batch_uuids.append(uuids.pop())
                if len(batch_uuids) >= self.batch_size:
                    break
            if len(batch_uuids) > 0:
                batch_count += 1
                self.state.uuids_in_progress(batch_uuids)
                batch_of_errors = self.update_objects(request, batch_uuids, xmin, snapshot_id)
                self.state.uuids_done(batch_uuids)
                if len(batch_of_errors) > 0:
                    errors.extend(batch_of_errors)
                log.error('Indexed batch: %d time: %s' % (batch_count,str(datetime.datetime.now(pytz.utc) - batch_started)))

        return errors

    def update_objects(self, request, uuids, xmin, snapshot_id):
        errors = []
        for i, uuid in enumerate(uuids):
            error = self.update_object(request, uuid, xmin)
            if error is not None:
                errors.append(error)
            if (i + 1) % 50 == 0:
                log.info('Indexing %d', i + 1)

        return errors

    def update_object(self, request, uuid, xmin):
        #self.state.start_uuid(uuid)

        last_exc = None
        try:
            result = request.embed('/%s/@@index-data' % uuid, as_user='INDEXER')
        except StatementError:
            self.state.failed_uuid(uuid)
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as e:
            log.error('Error rendering /%s/@@index-data', uuid, exc_info=True)
            last_exc = repr(e)

        if last_exc is None:
            for backoff in [0, 10, 20, 40, 80]:
                time.sleep(backoff)
                try:
                    self.es.index(
                        index=self.index, doc_type=result['item_type'], body=result,
                        id=str(uuid), version=xmin, version_type='external_gte',
                        request_timeout=30,
                    )
                except StatementError:
                    self.state.failed_uuid(uuid)
                    # Can't reconnect until invalid transaction is rolled back
                    raise
                except ConflictError:
                    log.warning('Conflict indexing %s at version %d', uuid, xmin)
                    self.state.failed_uuid(uuid)
                    return
                except (ConnectionError, ReadTimeoutError, TransportError) as e:
                    log.warning('Retryable error indexing %s: %r', uuid, e)
                    last_exc = repr(e)
                except Exception as e:
                    log.error('Error indexing %s', uuid, exc_info=True)
                    last_exc = repr(e)
                    break
                else:
                    # Get here on success and outside of try
                    #self.state.indexed_uuid(uuid)
                    return

        self.state.failed_uuid(uuid)
        timestamp = datetime.datetime.now().isoformat()
        return {'error_message': last_exc, 'timestamp': timestamp, 'uuid': str(uuid)}

    def shutdown(self):
        pass
