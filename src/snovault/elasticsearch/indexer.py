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
    INDEXER,
)
import datetime
import logging
import pytz
import time
import copy

from .indexer_utils import find_uuids_for_indexing, get_uuids_for_types


log = logging.getLogger(__name__)


def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    registry[INDEXER] = Indexer(registry)


@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    # Setting request.datastore here only works because routed views are not traversed.
    request.datastore = 'database'
    record = request.json.get('record', False)
    dry_run = request.json.get('dry_run', False)
    recovery = request.json.get('recovery', False)
    req_uuids = request.json.get('uuids', None)
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
            status = es.get(index='meta', doc_type='meta', id='indexing')
        except NotFoundError:
            interval_settings = {"index": {"refresh_interval": "30s"}}
            es.indices.put_settings(index='meta', body=interval_settings)
            pass
        else:
            last_xmin = status['_source']['xmin']

    result = {
        'xmin': xmin,
        'last_xmin': last_xmin,
    }

    flush = False
    if req_uuids is not None:
        invalidated = updated = set(req_uuids)
    elif last_xmin is None:
        # this will invalidate only the uuids of given types and does not
        # consider embedded/linked uuids.
        # if types is not provided, all types will be used
        result['types'] = types = request.json.get('types', None)
        invalidated = list(get_uuids_for_types(request.registry, types))
        updated = invalidated
        flush = True
    else:
        txns = session.query(TransactionRecord).filter(
            TransactionRecord.xid >= last_xmin,
        )

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

        # iterate through all indices and find items with matching embedded uuids
        indices = list(request.registry[COLLECTIONS].by_item_type.keys())
        invalidated, referencing, flush = find_uuids_for_indexing(indices, es, updated, renamed, log)
        result.update(
            max_xid=max_xid,
            renamed=renamed,
            updated=updated,
            referencing=len(referencing),
            invalidated=len(invalidated),
            txn_count=txn_count,
            first_txn_timestamp=first_txn.isoformat(),
        )
    log.debug("Indexing %s total items; %s primary items" %
             (str(len(invalidated)), str(len(updated))))
    if invalidated and not dry_run:
        # Exporting a snapshot mints a new xid, so only do so when required.
        # Not yet possible to export a snapshot on a standby server:
        # http://www.postgresql.org/message-id/CAHGQGwEtJCeHUB6KzaiJ6ndvx6EFsidTGnuLwJ1itwVH0EJTOA@mail.gmail.com
        snapshot_id = None
        if not recovery:
            snapshot_id = connection.execute('SELECT pg_export_snapshot();').scalar()

        result['errors'] = indexer.update_objects(request, invalidated, xmin, snapshot_id)
        result['indexed'] = len(invalidated)

        if record:
            try:
                es.index(index='meta', doc_type='meta', body=result, id='indexing')
            except:
                error_messages = copy.deepcopy(result['errors'])
                del result['errors']
                es.index(index='meta', doc_type='meta', body=result, id='indexing')
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error for {}, error message: {}'.format(item['uuid'], item['error_message']))
                        item['error_message'] = "Error occured during indexing, check the logs"
                result['errors'] = error_messages


            if es.indices.get_settings(index='meta')['meta']['settings']['index'].get('refresh_interval', '') != '1s':
                interval_settings = {"index": {"refresh_interval": "1s"}}
                es.indices.put_settings(index='meta', body=interval_settings)

        es.indices.refresh(index='meta')

        if flush:
            try:
                es.indices.flush_synced(index='meta')  # Faster recovery on ES restart
            except ConflictError:
                pass

    if first_txn is not None:
        result['lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)

    return result


class Indexer(object):
    def __init__(self, registry):
        self.es = registry[ELASTIC_SEARCH]

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
        try:
            result = request.embed('/%s/@@index-data' % uuid, as_user='INDEXER')
        except StatementError:
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as e:
            log.error('Error rendering /%s/@@index-data', uuid, exc_info=True)
            timestamp = datetime.datetime.now().isoformat()
            return {'error_message': repr(e), 'timestamp': timestamp, 'uuid': str(uuid)}
        last_exc = None
        for backoff in [0, 10, 20, 40, 80]:
            time.sleep(backoff)
            try:
                self.es.index(
                    index=result['item_type'], doc_type=result['item_type'], body=result,
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
                return

        timestamp = datetime.datetime.now().isoformat()
        return {'error_message': last_exc, 'timestamp': timestamp, 'uuid': str(uuid)}


    def shutdown(self):
        pass
