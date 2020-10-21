import binascii

from os import urandom
from datetime import datetime
from pytz import timezone

from redis import StrictRedis


def base_result(local_store):
    local_dt = datetime.now(timezone(local_store.local_tz))
    return {
        '@type': ['result'],
        'utc_now': str(datetime.utcnow()),
        'lcl_now': f"{local_store.local_tz}: {local_dt}",
    }


class LocalStoreClient():
    '''
    Light redis wrapper and redis examples
    - get_tag function was added to return hex str
    - Can access client directly for full functionality
    '''
    def __init__(self, **kwargs):
        self.local_tz = kwargs.get('local_tz', 'GMT')
        self.client = StrictRedis(
            charset='utf-8',
            decode_responses=True,
            db=kwargs['db_index'],
            host=kwargs['host'],
            port=kwargs['port'],
            socket_timeout=kwargs['socket_timeout'],
        )

    @staticmethod
    def get_tag(tag, num_bytes=2):
        '''
        Tags are the tag plus a random hex bytes string
        - Bytes string length is 2 * num bytes
        '''
        rand_hex_str = binascii.b2a_hex(urandom(num_bytes)).decode('utf-8')
        return f"{tag}:{rand_hex_str}"

    def ping(self):
        return self.client.ping()

    def dict_get(self, key):
        return self.client.hgetall(key)

    def dict_set(self, key, hash_dict):
        return self.client.hmset(key, hash_dict)
    
    def get_tag_keys(self, tag):
        return self.client.keys(f"{tag}:*")

    def item_get(self, key):
        return self.client.get(key)
    
    def item_set(self, key, item):
        return self.client.set(key, item)

    def list_add(self, key, item):
        return self.client.lpush(key, item)
    
    def list_get(self, key, start=0, stop=-1):
        return self.client.lrange(key, start, stop)
