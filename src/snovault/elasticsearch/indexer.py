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

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

SEARCH_MAX = 99999  # OutOfMemoryError if too high

def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    registry[INDEXER] = Indexer(registry)


class IndexerState(object):
    def __init__(self, es):
        self.es = es
        self.key = "indexerstate"
        if not self.es.indices.exists(self.key):
            self.es.indices.create(index=self.key, body={'index': {'number_of_shards': 1}})
            mapping = {'default': {"_all":    {"enabled": False},
                                "_source": {"enabled": True},
                                # "_id":     {"index": "not_analyzed", "store": True},
                                # "_ttl":    {"enabled": True, "default": "1d"},
                                }}
            self.es.indices.put_mapping(index=self.key, doc_type='default', body=mapping)
        self.state_key       = 'primary_indexer'       # State of the current or last cycle
        self.todo_set        = 'primary_invalidated'   # one cycle of uuids, sent to the Secondary Indexer
        #self.in_progress_set = 'primary_in_progress'
        self.failed_set      = 'primary_failed'
        self.done_set        = 'primary_done'          # Trying to get all uuids from 'todo' to this set
        self.troubled_set    = 'primary_troubled'      # uuids that failed to index in any cycle
        self.last_set        = 'primary_last_cycle'    # uuids in the most recent finished cycle
        self.followup_prep_list = 'primary_followup_prep_list' # Setting up the uuids to be handled by a followup process
        self.followup_ready_list = 'staged_for_secondary_list'  # Followup list is added to here to pass baton
        self.repeated_set = 'primary_repeats'
        # DO NOT INHERIT! All keys that are cleaned up at the start and fully finished end of indexing
        self.success_set       = self.done_set
        self.cleanup_keys      = [self.todo_set,self.failed_set,self.done_set],  # ,self.in_progress_set
        self.cleanup_last_keys = [self.last_set]  # ,self.audited_set] cleaned up only when new indexing occurs
        self.cache = {}
        # desired:
        # 1) Hand off to secondary.  Will work fine
        # 2) Record failures and consider blacklisting them
        # 3) Detect and recover from abreviated cycle - probably have to settle for batches
        # 4) Identify what is being currently worked on.  Probably have to accept batches

    def get(self):
        try:
            state = self.es.get(index=self.key, doc_type='default', id=self.state_key)['_source']
        except:
            state = {}
        return state

    def put(self, state):
        # Don't save errors in es
        errors = state.pop('errors',None)
        state['indexer'] = self.state_key
        try:
            self.es.index(index=self.key, doc_type='default', body=state, id=self.state_key)
        except:
            log.warn("Failed to save to es: " +str(state))

        if errors is not None:
            state['errors'] = errors

    def get_obj(self, id):
        try:
            return self.es.get(index=self.key, doc_type='default', id=id).get('_source',{})
        except:
            return {}

    def put_obj(self, id, obj):
        try:
            self.es.index(index=self.key, doc_type='default', id=id, body=obj)
        except:
            log.warn("Failed to save to es: " + id)

    def get_list(self, id):
        return self.get_obj(id).get('list',[])

    def get_count(self, id):
        return self.get_obj(id).get('count',[])

    def put_list(self, id, a_list):
        return self.put_obj(id, { 'list': a_list, 'count': len(a_list) })

    def get_diff(self,orig_id, subtract_ids):
        result_set = set(self.get_list(orig_id))

        if len(result_set) > 0:
            for id in subtract_ids:
                subtract_list = self.get_list(id)
                if len(subtract_list):
                    result_set = orig_set.difference_update(set(subtract_list))
        return result_set

    def set_add(self, id, vals):
        set_to_update = set(self.get_list(id))
        if len(set_to_update) > 0:
            set_to_update.update(vals)
        else:
            set_to_update = set(vals)
        self.put_list(id, set_to_update)

    def list_extend(self, id, vals):
        list_to_extend = self.get_list(id)
        if len(list_to_extend) > 0:
            list_to_extend.extend(vals)
        else:
            list_to_extend = vals

        self.put_list(id, list_to_extend)

    def delete_objs(self, ids):
        for id in ids:
            try:
                self.es.delete(index=self.key, doc_type='default', id=id)
            except:
                pass

    def rename_objs(self, from_id, to_id):
        val = self.get_list(from_id)
        if val:
            self.put_list(to_id, val)
            self.delete_objs([from_id])

    def prep_for_followup(self, xmin, uuids):
        # set up new _cycle
        prep_list = [ "xmin:%s" % xmin ]
        prep_list.extend(uuids)
        self.put_list(self.followup_prep_list, prep_list)
        # No need to preserve anything on the prep_list as it passes to the staged list in one cycle.

    def abandon_prior_prep(self):
        self.delete_objs([self.followup_prep_list])

    def start_cycle(self, uuids, state=None):
        if state is None:
            state = self.get()
        state['status'] = 'indexing'
        state['cycle_count'] = len(uuids)
        self.put(state)
        self.delete_objs(self.cleanup_last_keys)
        self.delete_objs(self.cleanup_keys)
        self.put_list(self.todo_set, set(uuids))
        return state

    def successes_this_cycle(self):
        # Could be overwritten to change definition of success
        return self.get_count(self.success_set)

    def finish_cycle(self, state=None):
        if state is None:
            state = self.get()

        # handle troubled uuids:
        troubled_uuids = set(self.get_list(self.failed_set))
        if len(troubled_uuids):
            self.set_add(self.troubled_set, troubled_uuids)
        # TODO: could make twice_troubled set and use it to blacklist uuids

        # pass any staged items to followup
        if self.followup_prep_list is not None:
            hand_off_list = self.get_list(self.followup_prep_list)
            if len(hand_off_list) > 0:  # Have to push because ready_list may still have previous cycles in it
                self.list_extend(self.followup_ready_list, hand_off_list)
                self.delete_objs([self.followup_prep_list])

        # no accounting so todo => done => last in this function
        #self.rename_objs(self.todo_set, self.done_set)
        done_count = self.get_count(self.todo_set)
        self.rename_objs(self.todo_set, self.last_set)

        #done_count = self.get_count(self.done_set)  # no accounting so todo => done => last in this function
        if self.success_set != self.done_set:
            state['acted_on'] = self.successes_this_cycle()
        cycle_count = state.pop('cycle_count',None)
        #assert(cycle_count == done_count)
        state['indexed'] = done_count

        #self.rename_objs(self.done_set, self.last_set)   # no accounting so todo => done => last in this function
        self.delete_objs(self.cleanup_keys)
        state['status'] = 'done'
        state['cycles'] = state.get('cycles',0) + 1
        self.put(state)

        # returns successful count
        return state

    def unfinished_cycle(self):
        # if there is an unfinished cycle, then xmin and uuid's are returned
        state = self.get()
        if state.get('status','') != 'indexing':
            return (-1, None, [])

        xmin = state.get('xmin', -1)
        #snapshot = state.get('snapshot', None)
        if xmin == -1:  # or snapshot is None:
            return (-1, None, [])

        #assert(self.get_count(self.done_set) == 0)
        undone_uuids = self.get_diff(self.todo_set, [self.done_set])
        if len(undone_uuids) <= 0:  # TODO SEARCH_MAX?  SEARCH_MAX/10
            return (-1, None, [])

        # Note: do not clean up last cycle yet because we could be restarted multiple times.

        return (xmin, None, undone_uuids)

    def failed_uuid(self, uuid):
        self.set_add(self.failed_set, [uuid])  # Hopefully these are rare

    def repeated_uuid(self, uuid):
        self.set_add(self.repeated_set, [uuid])  # Hopefully these are rare


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
    stage_for_followup = request.registry["stage_for_followup"]
    session = request.registry[DBSESSION]()
    connection = session.connection()
    first_txn = None
    snapshot_id = None
    restart=False

    # May have undone uuids from prior cycle
    state = IndexerState(es)

    (xmin, snapshot_id, invalidated) = state.unfinished_cycle()

    if xmin > -1 and len(invalidated) > 0:
        restart = True
        result = state.get()
        flush = True
    else:

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

            if invalidated and not dry_run:
                # Exporting a snapshot mints a new xid, so only do so when required.
                # Not yet possible to export a snapshot on a standby server:
                # http://www.postgresql.org/message-id/CAHGQGwEtJCeHUB6KzaiJ6ndvx6EFsidTGnuLwJ1itwVH0EJTOA@mail.gmail.com
                if snapshot_id is None and not recovery:
                    snapshot_id = connection.execute('SELECT pg_export_snapshot();').scalar()

    if invalidated and not dry_run:
        if first_txn is None:
            first_txn = datetime.datetime.now(pytz.utc)

        result["snapshot"] = snapshot_id

        if stage_for_followup:
            # Note: undones should be added before, because those uuids will (hopefully) be indexed in this cycle
            state.prep_for_followup(xmin, invalidated)

        result = state.start_cycle(invalidated, result)

        # Do the work...
        #if len(invalidated) > 10:           # DEBUG: special to test fake interrupted cycle on local machine.
        #    invalidated = invalidated[10:]  #        If using, make sure to comment out state.finish_cycle() below
        errors = indexer.update_objects(request, invalidated, xmin, snapshot_id, restart)

        result = state.finish_cycle(result)

        if errors:
            result['errors'] = errors

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
        state.put(result)

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
        self.state = IndexerState(self.es)
        try:
            self.batch_size = int(registry.settings["indexer.batch_size"])  # found in buildout.cfg for production
        except:
            self.batch_size = 100

    def update_objects(self, request, uuids, xmin, snapshot_id=None, restart=False):
        errors = []
        for i, uuid in enumerate(uuids):
            error = self.update_object(request, uuid, xmin)
            if error is not None:
                errors.append(error)
            if (i + 1) % 50 == 0:
                log.info('Indexing %d', i + 1)

        return errors

    def update_object(self, request, uuid, xmin, restart=False):

        # If a restart occurred in the middle of indexing, this uuid might have already been indexd, so skip redoing it.
        if restart:
            try:
                #if self.es.exists(index=self.index, id=str(uuid), version=xmin, version_type='external_gte'):
                result = self.es.get(index=self.index, id=str(uuid), _source_include='uuid', version=xmin, version_type='external_gte')
                if result.get('_source') is not None:
                    self.state.repeated_uuid(uuid)
                    return
            except:
                pass

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
                    return

        self.state.failed_uuid(uuid)
        timestamp = datetime.datetime.now().isoformat()
        return {'error_message': last_exc, 'timestamp': timestamp, 'uuid': str(uuid)}

    def shutdown(self):
        pass
