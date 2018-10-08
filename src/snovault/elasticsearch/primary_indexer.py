'''Primary Indexer, Base Indexer for all indexer'''
import datetime
import logging
import time
import json

from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError as ESConnectionError,
    TransportError,
)
from sqlalchemy.exc import StatementError
from urllib3.exceptions import ReadTimeoutError

from snovault import STORAGE

from .interfaces import ELASTIC_SEARCH


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


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
        for i, uuid in enumerate(uuids):
            error = self.update_object(request, uuid, xmin)
            if error is not None:
                errors.append(error)
            if (i + 1) % 50 == 0:
                log.info('Indexing %d', i + 1)
        return errors

    def update_object(self, request, uuid, xmin, restart=False):
        '''Gets embedded doc and indexes uuid'''
        request.datastore = 'database'  # required by 2-step indexer
        last_exc = None
        try:
            doc = request.embed('/%s/@@index-data/' % uuid, as_user='INDEXER')
        except StatementError:
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as ecp:  # pylint: disable=broad-except
            log.error('Error rendering /%s/@@index-data', uuid, exc_info=True)
            last_exc = repr(ecp)

        if last_exc is None:
            for backoff in [0, 10, 20, 40, 80]:
                time.sleep(backoff)
                try:
                    self.registry_es.index(
                        index=doc['item_type'], doc_type=doc['item_type'], body=doc,
                        id=str(uuid), version=xmin, version_type='external_gte',
                        request_timeout=30,
                    )
                except StatementError:
                    # Can't reconnect until invalid transaction is rolled back
                    raise
                except ConflictError:
                    log.warning('Conflict indexing %s at version %d', uuid, xmin)
                    return
                except (ESConnectionError, ReadTimeoutError, TransportError) as ecp:
                    log.warning('Retryable error indexing %s: %r', uuid, ecp)
                    last_exc = repr(ecp)
                except Exception as ecp:  # pylint: disable=broad-except
                    log.error('Error indexing %s', uuid, exc_info=True)
                    last_exc = repr(ecp)
                    break
                else:
                    # Get here on success and outside of try
                    return
        timestamp = datetime.datetime.now().isoformat()
        return {'error_message': last_exc, 'timestamp': timestamp, 'uuid': str(uuid)}

    def shutdown(self):
        '''Shut down - Not used in Indexer'''
        pass
