import binascii

from os import urandom
from datetime import datetime
from pytz import timezone

from redis import StrictRedis


def base_result(local_store):
    local_dt = datetime.now(timezone(local_store.local_tz))
    return {
        'utc_now': str(datetime.utcnow()),
        'lcl_now': f"{local_store.local_tz}: {local_dt}",
    }


class LocalStoreClient():
    '''
    Light redis wrapper and redis examples
    - get_tag function was added to return hex str
    - Can access client directly for full functionality
    - Server connection issues do not occur until accessing redis post __init__
    '''
    def __init__(
            self,
            db_index=0,
            host='localhost',
            local_tz='GMT',
            port=6379,
            timeout=5
        ):
        self.local_tz = local_tz
        redis_info = {
            'db': db_index,
            'host': host,
            'port': port,
            'socket_timeout': timeout,
        }
        self.client = StrictRedis(
            charset='utf-8',
            decode_responses=True,
            **redis_info
        )

    @staticmethod
    def get_tag(tag, num_bytes=8):
        '''
        Tags are the tag and a bytes string
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
