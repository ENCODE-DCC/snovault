from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    TransportError,
)
from ..indexing_views import SidException
from pyramid.view import view_config
from urllib3.exceptions import ReadTimeoutError
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER,
    INDEXER_QUEUE
)
from snovault import (
    DBSESSION,
)
from .indexer_utils import find_uuids_for_indexing
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
    registry[INDEXER] = Indexer(registry)


@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    # Setting request.datastore here only works because routed views are not traversed.
    request.datastore = 'database'
    record = request.json.get('record', False)  # if True, make a record in es
    dry_run = request.json.get('dry_run', False)  # if True, do not actually index
    es = request.registry[ELASTIC_SEARCH]
    indexer = request.registry[INDEXER]

    # ensure we get the latest version of what is in the db as much as possible
    session = request.registry[DBSESSION]()
    connection = session.connection()
    connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')


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
        # try to ensure ES is reasonably up to date
        es.indices.refresh(index='_all')
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
                es.index(index='indexing', doc_type='indexing', body=indexing_record, id=index_start_str)
                es.index(index='indexing', doc_type='indexing', body=indexing_record, id='latest_indexing')
            except:
                indexing_record['indexing_status'] = 'errored'
                error_messages = copy.deepcopy(indexing_record['errors'])
                del indexing_record['errors']
                es.index(index='indexing', doc_type='indexing', body=indexing_record, id=index_start_str)
                es.index(index='indexing', doc_type='indexing', body=indexing_record, id='latest_indexing')
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error for {}, error message: {}'.format(item['uuid'], item['error_message']))
                        item['error_message'] = "Error occured during indexing, check the logs"
    es.indices.refresh(index='_all')
    return indexing_record


class Indexer(object):
    def __init__(self, registry):
        self.registry = registry
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


    def get_messages_from_queue(self, skip_deferred=False):
        """
        Simple helper method. Attempt to get items from deferred queue first,
        and if none are found, check primary and then secondary queues. Both use
        long polling. Returns list of messages received and the string target
        the the queue came from.
        If skip_deferred, don't check that queue.
        """
        try_order = ['primary', 'secondary'] if skip_deferred else ['deferred', 'primary', 'secondary']
        messages = None
        target_queue = None
        for try_queue in try_order:
            # SPECIAL CASE: if we are looking at secondary but have items in
            # deferred, exit so that we can get a new transaction
            if skip_deferred and try_queue == 'secondary':
                deferred_waiting = self.queue.number_of_messages().get('deferred_waiting')
                if deferred_waiting and deferred_waiting > 0:
                    break
            messages = self.queue.receive_messages(target_queue=try_queue)
            if messages:
                target_queue = try_queue
                break
        return messages, target_queue


    def find_and_queue_secondary_items(self, source_uuids, embedded_uuids):
        """
        Should be used when strict is False
        Find all associated uuids of the given set of uuid using ES and queue
        them in the secondary queue.
        Add embedded_uuids after find secondary uuids (they are "strict")
        """
        if source_uuids:
            associated_uuids = find_uuids_for_indexing(self.registry, source_uuids, log)
            associated_uuids |= embedded_uuids
            # remove already indexed primary uuids used to find them
            secondary_uuids = list(associated_uuids - source_uuids)
        else:
            secondary_uuids = embedded_uuids

        return self.queue.add_uuids(self.registry, secondary_uuids, strict=True, target_queue='secondary')


    def update_objects_queue(self, request, counter):
        """
        Used with the queue
        """
        errors = []
        # hold uuids that will be used to find secondary uuids
        non_strict_uuids = set()
        embedded_uuids = set()
        to_delete = []  # hold messages that will be deleted
        # only check deferred queue on the first run, since there shouldn't
        # be much in there at any given point
        messages, target_queue = self.get_messages_from_queue(skip_deferred=False)
        while len(messages) > 0:
            for idx, msg in enumerate(messages):
                # this code is needed for integration with old style messages
                try:
                    msg_body = json.loads(msg['Body'])
                except ValueError:
                    msg_body = msg['Body']
                if isinstance(msg_body, dict):
                    msg_uuid= msg_body['uuid']
                    msg_sid = msg_body['sid']
                    msg_curr_time = msg_body['timestamp']
                    msg_detail = msg_body.get('detail')
                    # check to see if we are using the same txn that caused a deferral
                    if target_queue == 'deferred' and msg_detail == str(request.tm.get()):
                        self.queue.replace_messages([msg], target_queue=target_queue)
                        continue
                    if msg_body['strict'] is False:
                        non_strict_uuids.add(msg_uuid)
                else:  # old uuid message format
                    msg_uuid = str(msg_body)
                    msg_sid = None
                    msg_curr_time = None
                if target_queue != 'secondary':  # add embedded uuids to secondary
                    error = self.update_object(request, msg_uuid, sid=msg_sid,
                        curr_time=msg_curr_time, add_to_secondary=embedded_uuids)
                else:
                    error = self.update_object(request, msg_uuid, sid=msg_sid,
                        curr_time=msg_curr_time, add_to_secondary=None)
                if error:
                    if error.get('error_message') == 'deferred_retry':
                        # send this to the deferred queue
                        # should be set to strict, since associated uuids will
                        # already have been found if it was intitially not
                        msg_body['strict'] = False
                        msg_body['detail'] = error['txn_str']
                        self.queue.send_messages([msg_body], target_queue='deferred')
                        # delete the old message
                        to_delete.append(msg)
                    else:
                        # on a regular error, replace the message back in the queue
                        # could do something with error, like putting on elasticache
                        self.queue.replace_messages([msg], target_queue=target_queue)
                        errors.append(error)
                else:
                    if counter: counter[0] += 1  # do not increment on error
                    to_delete.append(msg)
                # delete messages when we have the right number
                if len(to_delete) == self.queue.delete_batch_size:
                    self.queue.delete_messages(to_delete, target_queue=target_queue)
                    to_delete = []
            # add to secondary queue, if applicable
            # reset embedded/non-strict uuids after adding
            if non_strict_uuids or embedded_uuids:
                queued, failed = self.find_and_queue_secondary_items(non_strict_uuids, embedded_uuids)
                if failed:
                    error_msg = 'Failure(s) queueing secondary uuids: %s' % str(failed)
                    log.error('INDEXER: ' + error_msg)
                    errors.append({'error_message': error_msg})
                non_strict_uuids = set()
                embedded_uuids = set()
            prev_target_queue = target_queue
            messages, target_queue = self.get_messages_from_queue(skip_deferred=True)
            # if we have switched between primary and secondary queues, delete
            # outstanding messages using previous queue
            if prev_target_queue != target_queue and to_delete:
                self.queue.delete_messages(to_delete, target_queue=prev_target_queue)
                to_delete = []
        # we're done. delete any outstanding messages
        if to_delete:
            self.queue.delete_messages(to_delete, target_queue=target_queue)
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


    def update_object(self, request, uuid, sid=None, curr_time=None, add_to_secondary=None):
        """
        Actually index the uuid using the index-data view.
        add_to_secondary is an optional set. If provided, the embedded uuids
        from the request.embed(/<uuid>/@@index-data) will be added to the set.
        """
        if not curr_time:
            curr_time = datetime.datetime.utcnow().isoformat()  # utc

        # check the sid with a less intensive view than @@index-data
        if sid:
            index_data_query = '/%s/@@index-data?sid=%s' % (uuid, sid)
        else:
            index_data_query = '/%s/@@index-data' % uuid

        try:
            result = request.embed(index_data_query, as_user='INDEXER')
        except SidException as e:
            log.warning('Invalid sid found for %s with value %s. time: %s' % (uuid, sid, curr_time))
            # this will cause the item to be sent to the deferred queue
            return {'error_message': 'deferred_retry', 'txn_str': str(request.tm.get())}
        except KeyError as e:
            log.warning('KeyError for %s with sid %s. time: %s' % (uuid, sid, curr_time))
            # this will cause the item to be sent to the deferred queue
            return {'error_message': 'deferred_retry', 'txn_str': str(request.tm.get())}
        except Exception as e:
            log.error('Error rendering /%s/@@index-data', uuid, exc_info=True)
            return {'error_message': repr(e), 'time': curr_time, 'uuid': str(uuid)}

        last_exc = None
        for backoff in [0, 1, 2]:
            time.sleep(backoff)
            try:
                self.es.index(
                    index=result['item_type'], doc_type=result['item_type'], body=result,
                    id=str(uuid), version=result['sid'], version_type='external_gte',
                    request_timeout=30
                )
            except ConflictError:
                log.warning('Conflict indexing %s at version %s. time: %s' % (uuid, result['sid'], curr_time))
                # this may be somewhat common and is not harmful
                # do not return an error so the item is removed from the queue
                return
            except (ConnectionError, ReadTimeoutError, TransportError) as e:
                log.warning('Retryable error indexing %s: %r', uuid, e)
                last_exc = repr(e)
            except Exception as e:
                log.error('Error indexing %s', uuid, exc_info=True)
                last_exc = repr(e)
                break
            else:
                # add embedded_uuids to secondary queue if no errors
                # this makes it so all items embedded in this will get indexed
                # (on the secondary queue with strict=True)
                if isinstance(add_to_secondary, set):
                    add_to_secondary |= set(result.get('embedded_uuids', []))
                    # remove the uuid we are indexing (included in result['embedded_uuids'])
                    try:
                        add_to_secondary.remove(uuid)
                    except KeyError:  # catch a possible edge case?
                        pass
                return
        return {'error_message': last_exc, 'time': curr_time, 'uuid': str(uuid)}


    def shutdown(self):
        pass
