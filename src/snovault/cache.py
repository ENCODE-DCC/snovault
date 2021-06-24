import json
from functools import cached_property
from typing import Any, Optional

from pyramid.threadlocal import manager
from sqlalchemy.util import LRUCache
import transaction.interfaces
from zope.interface import implementer

from snovault.local_storage import LocalStoreClient


@implementer(transaction.interfaces.ISynchronizer)
class ManagerLRUCache(object):
    """ Override capacity in settings.
    """
    def __init__(self, name, default_capacity=100, threshold=.5):
        self.name = name
        self.default_capacity = default_capacity
        self.threshold = threshold
        transaction.manager.registerSynch(self)

    @property
    def cache(self):
        if not manager.stack:
            return None
        threadlocals = manager.stack[0]
        if self.name not in threadlocals:
            registry = threadlocals['registry']
            capacity = int(registry.settings.get(self.name + '.capacity', self.default_capacity))
            threadlocals[self.name] = LRUCache(capacity, self.threshold)
        return threadlocals[self.name]

    def get(self, key, default=None):
        cache = self.cache
        if cache is None:
            return default
        try:
            return cache[key]
        except KeyError:
            return default

    def __contains__(self, key):
        cache = self.cache
        if cache is None:
            return False
        return key in cache

    def __setitem__(self, key, value):
        cache = self.cache
        if cache is None:
            return
        self.cache[key] = value

    # ISynchronizer

    def beforeCompletion(self, transaction):
        pass

    def afterCompletion(self, transaction):
        # Ensure cache is cleared for retried transactions
        if manager.stack:
            threadlocals = manager.stack[0]
            threadlocals.pop(self.name, None)

    def newTransaction(self, transaction):
        pass


class RedisLRUCache:
    def __init__(
        self, host, port, database_index=0, socket_timeout=None, local_timezone="GMT"
    ) -> None:
        self.host = host
        self.port = port
        self.database_index = database_index
        self.socket_timeout = socket_timeout
        self.local_timezone = local_timezone

    @cached_property
    def client(self) -> LocalStoreClient:
        return LocalStoreClient(
            host=self.host,
            port=self.port,
            db_index=self.database_index,
            socket_timeout=self.socket_timeout,
            local_tz=self.local_timezone
        )

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        cached = self.client.item_get(key)
        if cached is None:
            return default
        return json.loads(cached)

    def __contains__(self, key: str) -> bool:
        return self.client.client.exists(key) > 0

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Can store any json serializable type
        """
        self.client.item_set(key, json.dumps(value))
