'''Primary Indexer, Base Indexer for all indexer'''
import datetime
import logging
import time

from elasticsearch.exceptions import (
    ConflictError as ESConflictError,
    ConnectionError as ESConnectionError,
    TransportError as ESTransportError,
)
from pyramid.settings import asbool
from sqlalchemy.exc import StatementError as SQLStatementError
from urllib3.exceptions import ReadTimeoutError

from snovault import STORAGE

from .interfaces import ELASTIC_SEARCH


log = logging.getLogger('snovault.elasticsearch.es_index_listener')  # pylint: disable=invalid-name


class IndexItem(object):
    '''Container for item to index'''
    # pylint: disable=too-many-instance-attributes
    def __init__(self, index_item_dict, uuid=None, xmin=None):
        self._backoff_errors = {}
        self._embed_doc = {}
        self._embed_url = '/{uuid}/@@index-data/'.format(uuid=uuid)
        self.error = None
        self._es_time = None
        self._es_total_time = None
        self.total_time = None
        self._uuid = uuid
        self._xmin = xmin
        self.update_from_dict(index_item_dict)
        if not uuid and not self._uuid:
            raise ValueError('IndexItem Uuid not present')

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

    def as_log_msg(self):
        '''Returns index info for uuid as a string message'''
        msg = (
            '{uuid} {url} {total_time}'
            '{doc_path} {doc_type} {embedded} {linked} {embed_time}'
            '{es_time} {es_backoffs} {es_total_time}'
        ).format(
            url=self._embed_url,
            uuid=self._uuid,
            total_time='%0.6f' % self.total_time,
            # Doc
            doc_path=self._embed_doc.get('paths', ''),
            doc_type=self._embed_doc.get('item_type', ''),
            embedded=len(self._embed_doc.get('embedded_uuids', [])),
            linked=len(self._embed_doc.get('linked_uuids', [])),
            embed_time='%0.6f' % self._embed_time,
            # ES
            es_time='%0.6f' % self._es_time,
            es_backoffs=len(self._backoff_errors),
            es_total_time='%0.6f' % self._es_total_time,
        )
        if self.error:
            msg = 'Error: {err} ({msg})'.format(
                err=self.error['error_message'],
                msg=msg,
            )
        return msg

    def get_pickable(self):
        '''Return a pickable dict to recreate an instance on index_item'''
        index_item_dict = {
            '_backoff_errors': self._backoff_errors,
            '_embed_doc': self._embed_doc,
            '_embed_time': self._embed_time,
            '_embed_url': self._embed_url,
            'error': self.error,
            '_es_time': self._es_time,
            '_es_total_time': self._es_total_time,
            'total_time': self.total_time,
            '_uuid': self._uuid,
            '_xmin': self._xmin,
        }
        return index_item_dict

    def run_index(self, registry_es):
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

    def request_embed(self, request):
        '''Get embedded for uuid'''
        try:
            self._embed_doc = request.embed(self._embed_url, as_user='INDEXER')
        except SQLStatementError as ecp:
            log.error('SQL Error rendering %s', self._embed_url, exc_info=True)
            self._add_error(ecp)
            # Can't reconnect until invalid transaction is rolled back
            raise ecp
        except Exception as ecp:  # pylint: disable=broad-except
            log.error('Error rendering %s', self._embed_url, exc_info=True)
            self._add_error(ecp)

    def update_from_dict(self, index_item_dict):
        '''Update index_item instance with new index_item_dict'''
        for key, value in index_item_dict.items():
            if hasattr(self, key):
                setattr(self, key, value)


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
        for uuid in uuids:
            index_item = IndexItem({}, uuid=uuid, xmin=xmin)
            index_item_dict = index_item.get_pickable()
            index_item_dict = self.update_object(
                request,
                index_item_dict,
                restart=restart
            )
            index_item.update_from_dict(index_item_dict)
            if index_item.error is not None:
                errors.append(index_item.error)
            if (cnt + 1) % 50 == 0:
                log.info('Indexing %d', cnt + 1)
        return errors

    def update_object(self, request, index_item_dict, restart=False):
        '''Gets embedded doc and indexes uuid'''
        # pylint: disable=unused-argument
        start_time = time.time()
        index_item = IndexItem(index_item_dict)
        request.datastore = 'database'
        index_item.request_embed(request)
        index_item.run_index(self.registry_es)
        index_item.total_time = time.time() - start_time
        return index_item.get_pickable()

    def shutdown(self):
        '''Shut down - Not used in Indexer'''
        pass
