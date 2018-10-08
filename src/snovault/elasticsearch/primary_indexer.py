'''Primary Indexer, Base Indexer for all indexer'''
import datetime
import logging
import time

from elasticsearch.exceptions import (
    ConflictError as ESConflictError,
    ConnectionError as ESConnectionError,
    TransportError as ESTransportError,
)
from sqlalchemy.exc import StatementError as SQLStatementError
from urllib3.exceptions import ReadTimeoutError

from snovault import STORAGE

from .interfaces import ELASTIC_SEARCH


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class IndexItem(object):
    '''Container for item to index'''
    # pylint: disable=too-many-instance-attributes
    def __init__(self, request, uuid, xmin):
        self._backoff_errors = {}
        self._embed_doc = {}
        self._embed_url = '/{uuid}/@@index-data/'.format(uuid=uuid)
        self.error = None
        self.request = request
        self._uuid = str(uuid)
        self._xmin = xmin

    def _add_error(self, ecp, backoff=0):
        timestamp = datetime.datetime.now().isoformat()
        error = {
            'error_message': repr(ecp),
            'uuid': self._uuid,
            'timestamp': timestamp,
        }
        if backoff:
            self._backoff_errors[backoff] = error
        else:
            self.error = error

    def index_item(self, registry_es):
        '''Attempt to index uuid with embed doc as body'''
        if not self._embed_doc or self.error:
            return
        for backoff in [0, 10, 20, 40, 80]:
            time.sleep(backoff)
            try:
                registry_es.index(
                    index=self._embed_doc['item_type'],
                    doc_type=self._embed_doc['item_type'],
                    body=self._embed_doc,
                    id=self._uuid,
                    version=self._xmin,
                    version_type='external_gte',
                    request_timeout=30,
                )
            except SQLStatementError as ecp:
                log.error('SQL Error rendering %s', self._embed_url, exc_info=True)
                self._add_error(ecp, backoff=backoff)
                # Can't reconnect until invalid transaction is rolled back
                raise ecp
            except ESConflictError as ecp:
                log.warning('Conflict indexing %s at version %d', self._uuid, self._xmin)
                self._add_error(ecp, backoff=backoff)
                break
            except (ESConnectionError, ReadTimeoutError, ESTransportError) as ecp:
                log.warning('Retryable error indexing %s: %r', self._uuid, ecp)
                self._add_error(ecp, backoff=backoff)
            except Exception as ecp:  # pylint: disable=broad-except
                log.error('Error indexing %s', self._uuid, exc_info=True)
                self._add_error(ecp, backoff=backoff)
                break
            break

    def request_embed(self):
        '''Get embedded for uuid'''
        try:
            self._embed_doc = self.request.embed(self._embed_url, as_user='INDEXER')
        except SQLStatementError as ecp:
            log.error('SQL Error rendering %s', self._embed_url, exc_info=True)
            self._add_error(ecp)
            # Can't reconnect until invalid transaction is rolled back
            raise ecp
        except Exception as ecp:  # pylint: disable=broad-except
            log.error('Error rendering %s', self._embed_url, exc_info=True)
            self._add_error(ecp)


class PrimaryIndexer(object):
    '''Primary Indexer'''

    def __init__(self, registry):
        self.registry_es = registry[ELASTIC_SEARCH]
        self.esstorage = registry[STORAGE]
        self.index = registry.settings['snovault.elasticsearch.index']

    def update_objects(self, request, uuids, xmin, snapshot_id=None, restart=False):
        '''Wapper to iterate over update_object'''
        # pylint: disable=too-many-arguments, unused-argument
        errors = []
        for cnt, uuid in enumerate(uuids):
            index_item = IndexItem(request, uuid, xmin)
            self.update_object(index_item, restart=restart)
            if index_item.error is not None:
                errors.append(index_item.error)
            if (cnt + 1) % 50 == 0:
                log.info('Indexing %d', cnt + 1)
        return errors

    def update_object(self, index_item, restart=False):
        '''Gets embedded doc and indexes uuid'''
        # pylint: disable=unused-argument,
        index_item.request.datastore = 'database'
        index_item.request_embed()
        index_item.index_item(self.registry_es)

    def shutdown(self):
        '''Shut down - Not used in Indexer'''
        pass
