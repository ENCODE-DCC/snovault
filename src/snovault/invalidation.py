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
from .elasticsearch.interfaces import INDEXER_QUEUE
from .util import simple_path_ids
import transaction


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
def queue_item_and_invalidate_new_back_revs(event):
    '''
    Add item(s) to the INDEXER_QUEUE

    Invalidate objects that rev_link to us

    Catch those objects which newly rev_link us
    '''
    context = event.object
    updated = event.request._updated_uuid_paths
    initial = event.request._initial_back_rev_links.get(context.uuid, {})
    properties = context.upgrade_properties()

    # add item to queue
    # use strict mode if creating, otherwise should queue associated uuids
    # POSSIBLE ISSUES:
    # - on bin/load data, things get queued twice, once as created on once as modified
    indexer_queue = context.registry.get(INDEXER_QUEUE)
    if indexer_queue:  # Should only happen during testing... should we check something in registry?
        if event.__class__.__name__ == 'Created':
            indexer_queue.add_uuids(context.registry, [str(context.uuid)], strict=True)
        else:  # otherwise, event is AfterModified and need non-strict queueing
            indexer_queue.add_uuids(context.registry, [str(context.uuid)], strict=False)
    else:
        # if the indexer queue is not configured but ES is, raise an exception
        from .elasticsearch.interfaces import ELASTIC_SEARCH
        es = context.registry.get(ELASTIC_SEARCH)
        if es:
            raise Exception("Indexer queue not configured!")

    current = {
        path: set(simple_path_ids(properties, path))
        for path in context.type_info.merged_back_rev
    }
    for rel, uuids in current.items():
        for uuid in uuids.difference(initial.get(rel, ())):
            updated[uuid]


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
