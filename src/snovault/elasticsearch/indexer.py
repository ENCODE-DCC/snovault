from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    TransportError,
)
from pyramid.view import view_config
from urllib3.exceptions import ReadTimeoutError
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER,
    INDEXER_QUEUE
)
from .indexer_utils import index_timestamp
import datetime
import logging
import time
import copy

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
    record = request.json.get('record', False)  # if True, make a record in meta
    dry_run = request.json.get('dry_run', False)  # if True, do not actually index
    es = request.registry[ELASTIC_SEARCH]
    indexer = request.registry[INDEXER]
    if not dry_run:
        index_start_time = datetime.datetime.now()
        index_start_str = index_start_time.isoformat()

        # create indexing record, with _id = indexing_start_time timestamp
        indexing_record = {
            'uuid': index_start_str,
            'indexing_status': 'started',
        }

        # get info on what actually is being indexed
        indexing_content = {
            'type': 'sync' if request.json.get('uuids') else 'queue',
        }
        if indexing_content['type'] == 'sync':
            indexing_content['sync_uuids'] = len(request.json.get('uuids'))
        else:
            indexing_content['initial_queue_status'] = indexer.queue.number_of_messages()
        indexing_record['indexing_content'] = indexing_content
        indexing_record['indexing_started'] = index_start_str
        indexing_counter = [0]  # do this so I can pass it as a reference
        # actually index
        indexing_record['errors'] = indexer.update_objects(request, indexing_counter)
        index_finish_time = datetime.datetime.now()
        indexing_record['indexing_finished'] = index_finish_time.isoformat()
        indexing_record['indexing_elapsed'] = str(index_finish_time - index_start_time)
        # update record with final queue snapshot
        if indexing_content['type'] == 'queue':
            indexing_content['finished_queue_status'] = indexer.queue.number_of_messages()
        indexing_record['indexing_count'] = indexing_counter[0]
        indexing_record['indexing_status'] = 'finished'

        # with the index listener running more frequently, we don't want to
        # store a ton of useless records. Only store queue records that have
        # errors or have non-zero indexing count
        if record and indexing_content['type'] == 'queue' and not indexing_record['errors']:
            record = indexing_record['indexing_count'] > 0

        if record:
            try:
                es.index(index='meta', doc_type='meta', body=indexing_record, id=index_start_str)
                es.index(index='meta', doc_type='meta', body=indexing_record, id='latest_indexing')
            except:
                indexing_record['indexing_status'] = 'errored'
                error_messages = copy.deepcopy(indexing_record['errors'])
                del indexing_record['errors']
                es.index(index='meta', doc_type='meta', body=indexing_record, id=index_start_str)
                es.index(index='meta', doc_type='meta', body=indexing_record, id='latest_indexing')
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

    def update_objects(self, request, counter=None):
        """
        Top level update routing
        """
        # indexing is either run with sync uuids passed through the request
        # (which is synchronous) OR uuids from the queue
        sync_uuids = request.json.get('uuids', None)
        # actually index
        if sync_uuids:
            errors = self.update_objects_sync(request, sync_uuids, counter)
        else:
            errors = self.update_objects_queue(request, counter)


    def update_objects_queue(self, request, counter):
        """
        Used with the queue
        """
        errors = []
        to_delete = []  # hold messages that will be deleted
        messages = self.queue.receive_messages()  # long polling used in SQS
        while len(messages) > 0:
            for msg in messages:
                # msg['Body'] is just a string of uuids joined by commas
                errored = False
                for msg_uuid in msg['Body'].split(','):
                    error = self.update_object(request, msg_uuid)
                    if error is not None:
                        # on an error, replace the message back in the queue
                        errors.append(error)
                        errored = True
                        break
                    elif counter:  # don't increment counter on an error
                        counter[0] += 1
                # put the message back in the queue if we hit an error
                if errored:
                    self.queue.replace_messages([msg])
                else:
                    to_delete.append(msg)
                # delete messages when we have the right number
                if len(to_delete) == self.queue.delete_batch_size:
                    self.queue.delete_messages(to_delete)
                    to_delete = []
            messages = self.queue.receive_messages()
        # delete any outstanding messages
        if to_delete:
            self.queue.delete_messages(to_delete)
        return errors


    def update_objects_sync(self, request, sync_uuids, counter):
        """
        Used with sync uuids (simply loop through)
        sync_uuids is a list of string uuids. Use timestamp of index run
        """
        errors = []
        for i, uuid in enumerate(sync_uuids):
            error = self.update_object(request, uuid)
            if error is not None:  # don't increment counter on an error
                errors.append(error)
            elif counter:
                counter[0] += 1
        return errors


    def update_object(self, request, uuid):
        curr_time = datetime.datetime.now().isoformat()
        timestamp = index_timestamp()
        try:
            result = request.embed('/%s/@@index-data' % uuid, as_user='INDEXER')
        except Exception as e:
            log.error('Error rendering /%s/@@index-data', uuid, exc_info=True)
            return {'error_message': repr(e), 'time': curr_time, 'uuid': str(uuid)}
        last_exc = None
        for backoff in [0, 1, 2, 3]:
            time.sleep(backoff)
            # timestamp from the queue or /index call (for sync uuids)
            # is used as the version number
            try:
                self.es.index(
                    index=result['item_type'], doc_type=result['item_type'], body=result,
                    id=str(uuid), version=timestamp, version_type='external_gt',
                    request_timeout=30
                )
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
