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
from redis import Redis

log = logging.getLogger(__name__)
SEARCH_MAX = 99999  # OutOfMemoryError if too high


def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    registry[INDEXER] = Indexer(registry)


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

    if invalidated and not dry_run:
        # Exporting a snapshot mints a new xid, so only do so when required.
        # Not yet possible to export a snapshot on a standby server:
        # http://www.postgresql.org/message-id/CAHGQGwEtJCeHUB6KzaiJ6ndvx6EFsidTGnuLwJ1itwVH0EJTOA@mail.gmail.com
        snapshot_id = None
        if not recovery:
            snapshot_id = connection.execute('SELECT pg_export_snapshot();').scalar()
        # Store unfinished set after updating it
        redis_client = Redis(charset="utf-8", decode_responses=True)
        redis_pipe = redis_client.pipeline()
        if len(redis_client.sdiff("invalidated", "indexed", "failed")) > 1:  # Bek says this should be 1 and not 0
            redis_client.sadd('invalidated', *set(invalidated))
        else:
            # Set up previously finished cycle for secondary_indexer
            done_with = redis_client.lrange('wait-on-primary',0,-1)
            if len(done_with) > 0:
                redis_pipe.rpush('done-with-primary', *done_with).delete('wait-on-primary').execute()
            #for val in redis_client.lpop('wait-on-primary'):
            #    redis_pipe.rpush('done-with-primary', val)

            troubled_uuids = redis_client.smembers('failed')
            if len(troubled_uuids) > 0:
                redis_pipe.rpush('troubled uuids', *list(troubled_uuids))
            redis_pipe.delete('indexed', 'failed', 'invalidated', 'in progress')
            redis_pipe.sadd('invalidated', *set(invalidated))
            redis_pipe.execute()

        # Set up future cycle for secondary indexer rpush keeps them in order
        redis_pipe.rpush('wait-on-primary', "xmin:%s" % xmin).rpush('wait-on-primary', *list(invalidated)).execute()

        # Re-initiate invalidated set
        remaining_invalidated = list(redis_client.sdiff('invalidated', 'indexed', "failed"))
        result['errors'] = indexer.update_objects(request, remaining_invalidated, xmin, snapshot_id)
        result['indexed'] = len(invalidated)

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

        # This is the end of the cycle.  If all were indexed then we can kick them to the secondary indexer
        if len(redis_client.sdiff("invalidated", "indexed", "failed")) <= 1:
            # Set up just finished cycle for secondary_indexer
            done_with = redis_client.lrange('wait-on-primary',0,-1)
            if len(done_with) > 0:
                redis_pipe.rpush('done-with-primary', *done_with).delete('wait-on-primary').execute()
            #for val in redis_client.lpop('wait-on-primary'):
            #    redis_pipe.rpush('done-with-primary', val)
            redis_pipe.execute()

        if flush:
            try:
                es.indices.flush_synced(index=INDEX)  # Faster recovery on ES restart
            except ConflictError:
                pass

    if first_txn is not None:
        result['lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)

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
        self.redis_pipe = Redis(connection_pool=registry[CONNECTION].redis_pool, charset="utf-8", decode_responses=True).pipeline()

    def update_objects(self, request, uuids, xmin, snapshot_id):
        errors = []
        for i, uuid in enumerate(uuids):
            error = self.update_object(request, uuid, xmin)
            if error is not None:
                errors.append(error)
            if (i + 1) % 50 == 0:
                log.info('Indexing %d', i + 1)

        return errors

    def failed_uuid(self, uuid):
        self.redis_pipe.sadd("failed", uuid).srem("in progress", uuid).execute()

    def indexed_uuid(self, uuid):
        self.redis_pipe.sadd("indexed", uuid).srem("in progress", uuid).execute()

    def update_object(self, request, uuid, xmin):
        self.redis_pipe.sadd("in-progress", uuid).execute()

        last_exc = None
        try:
            result = request.embed('/%s/@@index-data' % uuid, as_user='INDEXER')
        except StatementError:
            self.failed_uuid(uuid)
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
                    self.failed_uuid(uuid)
                    # Can't reconnect until invalid transaction is rolled back
                    raise
                except ConflictError:
                    log.warning('Conflict indexing %s at version %d', uuid, xmin)
                    self.failed_uuid(uuid)
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
                    self.indexed_uuid(uuid)
                    return

        self.failed_uuid(uuid)
        timestamp = datetime.datetime.now().isoformat()
        return {'error_message': last_exc, 'timestamp': timestamp, 'uuid': str(uuid)}

    def shutdown(self):
        pass
