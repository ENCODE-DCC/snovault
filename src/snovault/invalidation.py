from collections import defaultdict
from pyramid.events import (
    BeforeRender,
    subscriber,
)
from pyramid.traversal import resource_path
from .interfaces import (
    AfterModified,
    BeforeModified,
    Created,
)
from .elasticsearch.interfaces import INDEXER_QUEUE, INDEXER_QUEUE_MIRROR
from .util import simple_path_ids
import logging
import datetime

log = logging.getLogger(__name__)


def includeme(config):
    config.scan(__name__)
    config.add_request_method(lambda request: defaultdict(set), '_updated_uuid_paths', reify=True)
    config.add_request_method(lambda request: {}, '_initial_back_rev_links', reify=True)


@subscriber(Created)
@subscriber(BeforeModified)
@subscriber(AfterModified)
def record_updated_uuid_paths(event):
    context = event.object
    updated = event.request._updated_uuid_paths
    uuid = str(context.uuid)
    name = resource_path(context)
    updated[uuid].add(name)


@subscriber(BeforeModified)
def record_initial_back_revs(event):
    context = event.object
    initial = event.request._initial_back_rev_links
    properties = context.upgrade_properties()
    initial[context.uuid] = {
        path: set(simple_path_ids(properties, path))
        for path in context.type_info.merged_back_rev
    }


@subscriber(Created)
@subscriber(AfterModified)
def invalidate_new_back_revs(event):
    '''
    Invalidate objects that rev_link to us
    Catch those objects which newly rev_link us
    '''
    context = event.object
    updated = event.request._updated_uuid_paths
    initial = event.request._initial_back_rev_links.get(context.uuid, {})
    properties = context.upgrade_properties()
    current = {
        path: set(simple_path_ids(properties, path))
        for path in context.type_info.merged_back_rev
    }
    for rel, uuids in current.items():
        for uuid in uuids.difference(initial.get(rel, ())):
            updated[uuid]


# Can we get rid of this?
@subscriber(BeforeRender)
def es_update_data(event):
    request = event['request']
    updated_uuid_paths = request._updated_uuid_paths

    if not updated_uuid_paths:
        return

    txn = transaction.get()
    data = txn._extension
    renamed = data['renamed'] = [
        uuid for uuid, names in updated_uuid_paths.items()
        if len(names) > 1
    ]
    updated = data['updated'] = list(updated_uuid_paths.keys())

    response = request.response
    response.headers['X-Updated'] = ','.join(updated)
    if renamed:
        response.headers['X-Renamed'] = ','.join(renamed)

    record = data.get('_snovault_transaction_record')
    if record is None:
        return

    xid = record.xid
    if xid is None:
        return

    response.headers['X-Transaction'] = str(xid)

    # Only set session cookie for web users
    namespace = None
    login = request.authenticated_userid
    if login is not None:
        namespace, userid = login.split('.', 1)

    if namespace == 'mailto':
        edits = request.session.setdefault('edits', [])
        edits.append([xid, list(updated), list(renamed)])
        edits[:] = edits[-10:]

    # XXX How can we ensure consistency here but update written records
    # immediately? The listener might already be indexing on another
    # connection. SERIALIZABLE isolation insufficient because ES writes not
    # serialized. Could either:
    # - Queue up another reindex on the listener
    # - Use conditional puts to ES based on serial before commit.
    # txn = transaction.get()
    # txn.addAfterCommitHook(es_update_object_in_txn, (request, updated))


def add_to_indexing_queue(success, request, item, edit_or_add):
    """
    Add item to queue for indexing. This function should be called from
    addAfterCommitHook.
    item arg is a dict: {'uuid': <item uuid>, 'sid': <item sid>}
    See item_edit and collection_add in .crud_view.py.
    edit_or_add is a string with value 'edit' or 'add'. If 'add', the item
    will be queued with strict indexing (no secondary items indexed).
    Otherwise, secondary items will also be queued.
    """
    error_msg = None
    if success:  # only queue if the transaction is successful
        try:
            # use strict mode if the item was added
            item['strict'] = edit_or_add == 'add'
            item['timestamp'] = datetime.datetime.utcnow().isoformat()
            indexer_queue = request.registry.get(INDEXER_QUEUE)
            indexer_queue_mirror = request.registry.get(INDEXER_QUEUE_MIRROR)
            if indexer_queue:
                # send to primary queue
                indexer_queue.send_messages([item], target_queue='primary')
                if indexer_queue_mirror:
                    # need to handle change of queued message form in FF-1075
                    if indexer_queue_mirror.second_queue_name:
                        # sends to primary queue for the mirror
                        indexer_queue_mirror.send_messages([item], target_queue='primary')
                    else:
                        # old form of sending uuids (when they were just strings)
                        indexer_queue_mirror.add_uuids(request.registry, [item['uuid']], strict=(edit_or_add == 'add'))
            else:
                # if the indexer queue is not configured but ES is, log an error
                from .elasticsearch.interfaces import ELASTIC_SEARCH
                es = request.registry.get(ELASTIC_SEARCH)
                if es:
                    raise Exception("Indexer queue not configured! Attempted to queue %s for method %s." % (str(item), edit_or_add))
        except Exception as e:
            error_msg = repr(e)
    else:
        error_msg = 'Transaction not successful! %s not queued for method %s.' % (str(item), edit_or_add)
    if error_msg:
        log.error('___Error queueing %s for indexing. Error: %s' % (str(item), error_msg))
