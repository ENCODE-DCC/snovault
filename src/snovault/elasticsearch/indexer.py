from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    TransportError,
)
from pyramid.view import view_config
from sqlalchemy.exc import StatementError
from urllib3.exceptions import ReadTimeoutError
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER,
    INDEXER_QUEUE
)
import datetime
import logging
import time
import copy
import json

log = logging.getLogger(__name__)


def includeme(config):
    config.add_route('index', '/index')

    config.scan(__name__)
    registry = config.registry
    if registry.settings.get('indexer'):
        registry[INDEXER] = Indexer(registry)
    else:
        registry[INDEXER] = None


@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    # Setting request.datastore here only works because routed views are not traversed.
    request.datastore = 'database'
    record = request.json.get('record', False)  # if True, make a record in meta
    dry_run = request.json.get('dry_run', False)  # if True, do not actually index
    es = request.registry[ELASTIC_SEARCH]
    indexer = request.registry[INDEXER]
    if not indexer:
        log.debug("Skipping indexing since this machine is not set to index.")
        return {}

    # indexing is either run with forced uuids passed through the request
    # (which is synchronous) OR uuids from the queue (see Indexer class)
    force_uuids = request.json.get('uuids', None)
    if not force_uuids:


    log.debug("Indexing %s total items; %s primary items" %
             (str(len(invalidated)), str(len(updated))))
    if not dry_run:
        index_start_time = datetime.datetime.now()
        index_start_str = index_start_time.isoformat()

        # create indexing record, with _id = indexing_start_time timestamp
        indexing_record = {
            'uuid': index_start_str,
            'indexing_status': 'started',
            'to_index': len(invalidated)
        }
        # initially index the indexing record
        if record:
            try:
                es.index(index='meta', doc_type='meta', body=indexing_record, id=index_start_str)
            except:
                log.error('Could not initialize indexing record for %s.', index_start_str)

        indexing_record['indexing_started'] = index_start_str
        # actually index
        if forced_uuids:
            errors = indexer.update_objects_forced(request, forced_uuids, index_start_str)
        else:
            errors = indexer.update_object_queued(request)
        indexing_record['errors'] = errors)
        index_finish_time = datetime.datetime.now()
        indexing_record['indexing_finished'] = index_finish_time.isoformat()
        indexing_record['indexing_elapsed'] = str(index_finish_time - index_start_time)
        indexing_record['indexed'] = len(invalidated)
        indexing_record['indexing_status'] = 'finished'

        if record:
            try:
                es.index(index='meta', doc_type='meta', body=indexing_record, id=index_start_str)
            except:
                indexing_record['indexing_status'] = 'errored'
                error_messages = copy.deepcopy(indexing_record['errors'])
                del indexing_record['errors']
                es.index(index='meta', doc_type='meta', body=indexing_record, id=index_start_str)
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error for {}, error message: {}'.format(item['uuid'], item['error_message']))
                        item['error_message'] = "Error occured during indexing, check the logs"
            es.indices.refresh(index='meta')
            # use this opportunity to sync flush the index (no harm if it fails)
            try:
                es.indices.flush_synced(index='meta')
            except ConflictError:
                pass

    return indexing_record


class Indexer(object):
    def __init__(self, registry):
        self.es = registry[ELASTIC_SEARCH]
        self.queue = registry[INDEXER_QUEUE]

    def update_objects_queued(self, request):
        """
        Used with the queue
        """
        errors = []
        count = 0
        messages = self.queue.recieve_messages()  # long polling used in SQS
        while len(messages) > 0:
            for msg in messages:
                msg = json.loads(msg)
                error = self.update_object(request, msg['uuid'], msg['timestamp'])
                if error is not None:
                    errors.append(error)
                count += 1
                if (count) % 50 == 0:
                    log.info('Indexing %d (queued)', count)
            self.queue.delete_messages(messages)
            messages = self.queue.recieve_messages()
        return errors


    def update_objects_forced(self, request, forced_uuids, timestamp):
        """
        Used with forced uuids (simply loop through)
        forced_uuids is a list of string uuids. Use timestamp of index run
        """
        errors = []
        for i, uuid in enumerate(forced_uuids):
            error = self.update_object(request, uuid, timestamp)
            if error is not None:
                errors.append(error)
            if (i + 1) % 50 == 0:
                log.info('Indexing %d (forced)', i + 1)
        return errors


    def update_object(self, request, uuid, timestamp):
        curr_time = datetime.datetime.now().isoformat()
        try:
            result = request.embed('/%s/@@index-data' % uuid, as_user='INDEXER')
        except StatementError:
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as e:
            log.error('Error rendering /%s/@@index-data', uuid, exc_info=True)
            return {'error_message': repr(e), 'time': curr_time, 'uuid': str(uuid)}
        last_exc = None
        for backoff in [0, 10, 20, 40, 80]:
            time.sleep(backoff)
            # timestamp from the queue or /index call (for forced uuids)
            # is used as the version number
            try:
                self.es.index(
                    index=result['item_type'], doc_type=result['item_type'], body=result,
                    id=str(uuid), version=timestamp, version_type='external_gt',
                    request_timeout=30
                )
            except StatementError:
                # Can't reconnect until invalid transaction is rolled back
                raise
            except ConflictError:
                log.warning('Conflict indexing %s at version %d', uuid, timestamp)
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
        return {'error_message': last_exc, 'time': curr_time, 'uuid': str(uuid)}


    def shutdown(self):
        pass
