'''
Simple uuid queue for indexing process
- Can be overridden by another script, class, module as long as it exposes
the proper functions used in the indexer classes.
'''
import time
from os import getpid


class SimpleUuidServer(object):
    '''Simple uuid queue as a list'''
    def __init__(self):
        self._uuids = []
        self._client_conns = {}
        self.errors = []

    def _get_uuid(self):
        if self._uuids:
            return self._uuids.pop()
        return None

    def _load_uuid(self, uuid):
        '''
        Load uuid into queue
        - Uuid ordering impacts indexing time, list is reversed due to
        pop being used in get uuid
        '''
        self._uuids.insert(0, uuid)

    def get_uuids(self, client_id, cnt):
        '''The only way to get uuids from queue'''
        uuids = []
        if client_id not in self._client_conns:
            self._client_conns[client_id] = {
                'uuid_cnt': 0,
                'get_cnt': 0,
                'results': None,
            }
        client_conn = self._client_conns[client_id]
        client_conn['get_cnt'] += 1
        if cnt == -1:
            cnt = len(self._uuids)
        if cnt and not client_conn['uuid_cnt']:
            # There are uuids on the server and the worker is not working
            while cnt > 0:
                uuid = self._get_uuid()
                if not uuid:
                    break
                uuids.append(uuid)
                cnt -= 1
            client_conn['uuid_cnt'] = len(uuids)
        return uuids

    def has_uuids(self):
        '''Are there uuids in the queue'''
        if self._uuids:
            return True
        return False

    def is_indexing(self):
        '''Is an indexing process currently running'''
        if self.has_uuids() or self.errors:
            return True
        for client_info in self._client_conns.values():
            if client_info['uuid_cnt']:
                return True
            elif client_info['results'] is None:
                return True
        return False

    def load_uuids(self, uuids):
        '''
        The only way to load uuids in queue
        - Uuids are to be loaded once per indexing session
        '''
        if self.has_uuids():
            return None
        for uuid in uuids:
            self._load_uuid(uuid)
        return len(self._uuids)

    def reset(self, force=False):
        '''
        Reset the queue so a new indexing process can begin
        - There cannot be any uuids remaining
        - The errors from the last indexing session must be removed
        - There cannot be any pending client connections
        '''
        msg = 'Okay'
        can_reset = True
        if self.has_uuids():
            can_reset = False
            msg = 'Queue Server cannot reset. Uuids=%d' % (
                len(self._uuids),
            )
        if self.errors:
            can_reset = False
            msg = 'Queue Server cannot reset. Errors=%d' % (
                len(self.errors),
            )
        for client_id, client_info in self._client_conns.items():
            if client_info['uuid_cnt']:
                can_reset = False
                msg = 'Queue Server cannot reset. Client Pending(%s)' % (
                    client_id,
                )
        if can_reset or force:
            self._uuids = []
            self._client_conns = {}
            self.errors = []
        return msg

    def update_finished(self, client_id, results):
        '''The way a client finishes a batch of uuids'''
        msg = 'Client id(%s) DNE.' % client_id
        if client_id in self._client_conns:
            client_conn = self._client_conns[client_id]
            client_conn['results'] = results
            batch_errors = []
            for error in results['errors']:
                error['client_id'] = client_id
                batch_errors.append(error)
            self.errors.extend(batch_errors)
            client_conn['uuid_cnt'] -= results['successes']
            client_conn['uuid_cnt'] -= len(batch_errors)
            if results['successes'] < 0 or client_conn['uuid_cnt']:
                msg = 'Queue Server cannot close client(%s)' % client_id
            else:
                msg = 'Okay'
        return msg


class SimpleUuidClient(object):
    '''Basic uuid client to get uuids for indexing'''

    def __init__(self, processes, chunk_size, batch_size, server_conn):
        self.server_conn = server_conn
        self.processes = processes
        self.chunk_size = chunk_size
        self.batch_size = batch_size
        self.client_id = self._get_client_id()
        self.is_running = False

    @staticmethod
    def _get_client_id():
        return '{}-{}'.format(
            getpid(),
            int(time.time() * 1000000),
        )

    def get_uuids(self):
        '''Get all or some of uuids'''
        return self.server_conn.get_uuids(self.client_id, cnt=self.batch_size)

    def update_finished(self, results):
        '''Update server with batch results'''
        return self.server_conn.update_finished(self.client_id, results)
