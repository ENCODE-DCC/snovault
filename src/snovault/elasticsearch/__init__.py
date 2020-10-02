import binascii, logging

from os import urandom

from snovault.json_renderer import json_renderer
from snovault.util import get_root_request
from elasticsearch import Elasticsearch
from elasticsearch.connection import Urllib3HttpConnection
from elasticsearch.serializer import SerializationError
from pyramid.settings import (
    asbool,
    aslist,
)
from .interfaces import (
    APP_FACTORY,
    ELASTIC_SEARCH,
    INDEXER,
)
import json
import sys

from redis import StrictRedis


PY2 = sys.version_info.major == 2


ROOT_LOG = logging.getLogger('root')


class LocalStoreClient():
    client = None
    
    def __init__(self, redis_db=0, local_store=None):
        if local_store:
            self.client = StrictRedis(
                charset="utf-8",
                decode_responses=True,
                db=redis_db,
                host='localhost',
                port=6379,
                socket_timeout=5,
            )
        else:
            # ephemeral storage objects.
            self.state = {}
            self.events = []
            self.items = {}

    def get_tag(self, num_bytes=8):
        return binascii.b2a_hex(urandom(num_bytes)).decode('utf-8')
   
    def hash_get(self, key):
        # Get the dict
        if self.client:
            return self.client.hgetall(key)
        return self.state

    def hash_set(self, key, hash_dict):
        # Stores a dict.  Allowed values for keys are limited
        if self.client:
            return self.client.hmset(key, hash_dict)
        self.state.update(hash_dict)

    def item_get(self, key):
        if self.client:
            return self.client.get(key)
        return self.items.get(key)
    
    def item_set(self, key, item):
        # Add item with key
        if self.client:
            return self.client.set(key, item)
        self.items[key] = item

    def list_add(self, key, item):
        # List is for storing item tags
        if self.client:
            return self.client.lpush(key, item)
        self.events.insert(0, item)
    
    def list_get(self, key, start, stop):
        # list get must have range, 0 to -1 min/max
        if self.client:
            return self.client.lrange(key, start, stop)
        if stop == 0:
            stop += 1
        if stop == -1:
            return self.events[start:]
        else:
            return self.events[start:stop]


def includeme(config):
    settings = config.registry.settings
    settings.setdefault('snovault.elasticsearch.index', 'snovault')

    config.add_request_method(datastore, 'datastore', reify=True)

    addresses = aslist(settings['elasticsearch.server'])
    config.registry[ELASTIC_SEARCH] = Elasticsearch(
        addresses,
        serializer=PyramidJSONSerializer(json_renderer),
        connection_class=TimedUrllib3HttpConnection,
        retry_on_timeout=True,
        # maxsize=50
    )

    config.include('.cached_views')
    config.include('.esstorage')

    config.include('.indexer')
    config.include('.indexer_state')
    if asbool(settings.get('indexer')) and not PY2:
        config.include('.mpindexer')


def datastore(request):
    if request.__parent__ is not None:
        return request.__parent__.datastore
    datastore = 'database'
    if request.params.get('frame') == 'edit':
        return datastore
    if request.method in ('HEAD', 'GET'):
        datastore = request.params.get('datastore') or \
            request.headers.get('X-Datastore') or \
            request.registry.settings.get('collection_datastore', 'elasticsearch')
    return datastore


class PyramidJSONSerializer(object):
    mimetype = 'application/json'

    def __init__(self, renderer):
        self.renderer = renderer

    def loads(self, s):
        try:
            return json.loads(s)
        except (ValueError, TypeError) as e:
            raise SerializationError(s, e)

    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, (type(''), type(u''))):
            return data

        try:
            return self.renderer.dumps(data)
        except (ValueError, TypeError) as e:
            raise SerializationError(data, e)


class TimedUrllib3HttpConnection(Urllib3HttpConnection):
    stats_count_key = 'es_count'
    stats_time_key = 'es_time'

    def stats_record(self, duration):
        request = get_root_request()
        if request is None:
            return
        if not hasattr(request, '_stats'):
            # For an unknown reason, requests may reach this location without stats
            # Logging the issues and adding the stats late should be temporary
            ROOT_LOG.error('_stats object missing from request: %s', request.url)
            ROOT_LOG.error(request)
            request._stats = {
                'late_stats_added': True,
                'stats_count_key': 0,
                'stats_time_key': 0
            }

        duration = int(duration * 1e6)
        stats = request._stats
        stats[self.stats_count_key] = stats.get(self.stats_count_key, 0) + 1
        stats[self.stats_time_key] = stats.get(self.stats_time_key, 0) + duration

    def log_request_success(self, method, full_url, path, body, status_code, response, duration):
        self.stats_record(duration)
        return super(TimedUrllib3HttpConnection, self).log_request_success(
            method, full_url, path, body, status_code, response, duration)

    def log_request_fail(self, method, full_url, path, body, duration, status_code=None, response=None, exception=None):
        self.stats_record(duration)
        return super(TimedUrllib3HttpConnection, self).log_request_fail(
            method, full_url, path, body, duration, status_code, response, exception)
