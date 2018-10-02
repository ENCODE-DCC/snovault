"""
Test the uuid_queue module redis_queues file
classes
    - RedisQueue
    - RedisPipeQueue
    - RedisListQueue
    - RedisListPipeQueue
    - RedisSetQueue
    - RedisSetPipeQueue
    - RedisSetPipeExecQueue
"""
import unittest

from ddt import ddt, data, unpack

from snovault.elasticsearch.uuid_queue.uuid_queue_adapter import UuidQueueTypes
from snovault.elasticsearch.uuid_queue.redis_queues import (
    RedisClient,
    RedisQueueMeta,
    RedisQueue,
    RedisPipeQueue,
    RedisListQueue,
    RedisListPipeQueue,
    RedisSetQueue,
    RedisSetPipeQueue,
    RedisSetPipeExecQueue,
)

from test_redis_qmeta import _check_redis_conn


REDIS_TEST_DB = 3


class TestRedisQueue(unittest.TestCase):
    '''
    RedisQueue base class
    - Cannot be used as a queue.  Only a base class.
    '''
    queue_name = 'testredisQ'
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'host': 'localhost',
            'port': 6379,
            'db': REDIS_TEST_DB,
        }
        cls.client = RedisClient(cls.args)
        cls.queue = RedisQueue(cls.queue_name, cls.client)

    def setUp(self):
        if not _check_redis_conn(self.client):
            self.fail('Redis Connection Failed')
        self.queue.qmeta.set_args()

    def tearDown(self):
        self.queue.qmeta.purge_meta()

    def test_init(self):
        '''Test RedisQueue initialization'''
        self.assertIsNone(self.queue.add_str)
        self.assertIsNone(self.queue.get_str)
        self.assertIsNone(self.queue.len_str)
        self.assertIsNone(self.queue.queue_type)
        self.assertEqual(self.queue.max_value_size, 262144)
        self.assertIsInstance(self.queue.qmeta, RedisQueueMeta)
        # pylint: disable=protected-access
        self.assertIsNone(self.queue._values)
        self.assertIsInstance(self.queue._client, RedisClient)

    def test_call_func_bad(self):
        '''
        Test RedisQueue _call_func bad func_str
        - Good function strings are tested in children
        '''
        bad_func_str = 'not-a-pyredis-function'
        try:
            # pylint: disable=protected-access
            self.queue._call_func(bad_func_str)
        except ValueError:
            pass
        else:
            self.fail('Should fail with ValueError')

    def test_add_value(self):
        '''Test RedisQueue _add_value'''
        some_value = 'some-value'
        try:
            # pylint: disable=protected-access
            self.queue._add_value(some_value)
        except TypeError:
            pass
        else:
            self.fail('Should fail with TypeError')

    def test_get_value(self):
        '''Test RedisQueue _get_value'''
        try:
            # pylint: disable=protected-access
            self.queue._get_value()
        except TypeError:
            pass
        else:
            self.fail('Should fail with TypeError')

    def test_does_exist(self):
        '''Test RedisQueue does_exist'''
        self.assertFalse(self.queue.does_exist())

    def test_has_values(self):
        '''Test RedisQueue has_values'''
        self.assertFalse(self.queue.has_values())

    def test_purge(self):
        '''
        Test RedisQueue purge
        - Cannot be tested on base class
        '''
        self.queue.purge()
        self.assertTrue(True)  # pylint: disable=redundant-unittest-assert

    def test_queue_length(self):
        '''Test RedisQueue queue_length'''
        self.assertIsNone(self.queue.queue_length())

    def test_is_queue_empty(self):
        '''Test RedisQueue is_queue_empty'''
        self.assertTrue(self.queue.is_queue_empty())


class TestRedisPipeQueue(unittest.TestCase):
    '''
    RedisPipeQueue base class
    - Cannot be used as a queue.  Only a base class.
    '''
    queue_name = 'testredispipeQ'
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'host': 'localhost',
            'port': 6379,
            'db': REDIS_TEST_DB,
        }
        cls.client = RedisClient(cls.args)
        cls.queue = RedisPipeQueue(cls.queue_name, cls.client)

    def setUp(self):
        if not _check_redis_conn(self.client):
            self.fail('Redis Connection Failed')
        self.queue.qmeta.set_args()

    def tearDown(self):
        self.queue.qmeta.purge_meta()

    def test_call_pipe(self):
        '''
        Test RedisPipeQueue _call_pipe'''
        try:
            pipe = None
            # pylint: disable=protected-access
            self.queue._call_pipe(pipe)
        except AttributeError:
            pass
        else:
            self.fail('Should fail with AttributeError')

    def test_get_pipe(self):
        '''Test RedisPipeQueue _get_pipe'''
        try:
            func_str = None
            # pylint: disable=protected-access
            self.queue._get_pipe(func_str)
        except TypeError:
            pass
        else:
            self.fail('Should fail with TypeError')

    def test_add_values(self):
        '''
        Test RedisPipeQueue add_values
        - Fails like _get_pipe
        '''
        values = [1, 2, 3]
        try:
            self.queue.add_values(values)
        except TypeError:
            pass
        else:
            self.fail('Should fail with TypeError')

    def test_get_values(self):
        '''
        Test RedisPipeQueue get_values
        - Fails like _get_pipe
        '''
        get_count = 15
        try:
            self.queue.get_values(get_count)
        except TypeError:
            pass
        else:
            self.fail('Should fail with TypeError')


@ddt
class TestRedisListQueue(unittest.TestCase):
    '''Test RedisListQueue, child of RedisQueue'''
    queue_name = 'testredislistQ'
    queue_type = UuidQueueTypes.REDIS_LIST
    @classmethod
    def setUpClass(cls):
        cls.args = {
            'host': 'localhost',
            'port': 6379,
            'db': REDIS_TEST_DB,
        }
        cls.client = RedisClient(cls.args)
        cls.queue = cls.client.get_queue(cls.queue_name, cls.queue_type)

    def setUp(self):
        if not _check_redis_conn(self.client):
            self.fail('Redis Connection Failed')
        self.queue.qmeta.set_args()
        self.queue.purge()

    def tearDown(self):
        self.queue.qmeta.purge_meta()
        self.queue.purge()

    def _get_all(self):
        '''Helper function to get all items in queue'''
        return self.client.lrange(self.queue_name, 0, -1)

    def test_add_values(self):
        '''Test RedisListQueue add_values through UuidBaseQueue'''
        values = [
            'id_' + str(val)
            for val in range(1, 4)
        ]
        values_bytes = 0
        for value in values:
            values_bytes += len(value)
        failed, bytes_added, call_cnt = self.queue.add_values(values)
        self.assertListEqual(failed, [])
        self.assertEqual(bytes_added, values_bytes)
        self.assertEqual(call_cnt, len(values))
        self.assertEqual(self.queue.queue_length(), len(values))
        all_values = self._get_all()
        all_values.sort()
        values.sort()
        self.assertListEqual(all_values, values)

    @data(
        # get_count
        (4), (40)
    )
    def test_get_values(self, get_count):
        '''
        Test RedisListQueue get_values through UuidBaseQueue
        - Greater than or equal added values
        '''
        values = [
            'id_' + str(val)
            for val in range(1, 4)
        ]
        self.queue.add_values(values)
        got_values, call_cnt = self.queue.get_values(get_count)
        self.assertEqual(call_cnt, len(values))
        self.assertEqual(self.queue.queue_length(), 0)
        all_values = self._get_all()
        self.assertListEqual(all_values, [])
        got_values.sort()
        values.sort()
        self.assertListEqual(got_values, values)

    def test_get_values_less(self):
        '''
        Test RedisListQueue get_values through UuidBaseQueue
        - Ask for less than added values
        '''
        values = [
            'id_' + str(val)
            for val in range(1, 4)
        ]
        get_count = len(values) - 1
        self.queue.add_values(values)
        got_values, call_cnt = self.queue.get_values(get_count)
        self.assertEqual(call_cnt, get_count)
        self.assertEqual(
            self.queue.queue_length(),
            len(values) - get_count
        )
        all_values = self._get_all()
        self.assertEqual(len(all_values), len(values) - get_count)
        self.assertEqual(len(got_values), get_count)

    def test_does_exist(self):
        '''Test RedisListQueue does_exist'''
        values = [
            'id_' + str(val)
            for val in range(1, 4)
        ]
        self.queue.add_values(values)
        self.assertTrue(self.queue.does_exist())

    def test_does_exist_empty(self):
        '''Test RedisListQueue does_exist when no values'''
        self.assertFalse(self.queue.does_exist())

    def test_has_values(self):
        '''Test RedisListQueue has_values'''
        values = [
            'id_' + str(val)
            for val in range(1, 4)
        ]
        self.queue.add_values(values)
        self.assertTrue(self.queue.has_values())

    def test_has_values_empty(self):
        '''Test RedisListQueue has_values when no values'''
        self.assertFalse(self.queue.has_values())

    def test_purge(self):
        '''Test RedisListQueue purge'''
        values = [
            'id_' + str(val)
            for val in range(1, 4)
        ]
        self.queue.add_values(values)
        self.queue.purge()
        self.assertFalse(self.queue.does_exist())

    def test_queue_length(self):
        '''Test RedisListQueue queue_length'''
        values = [
            'id_' + str(val)
            for val in range(1, 4)
        ]
        self.queue.add_values(values)
        self.assertEqual(self.queue.queue_length(), len(values))

    def test_is_queue_empty(self):
        '''Test RedisListQueue is_queue_empty'''
        values = [
            'id_' + str(val)
            for val in range(1, 4)
        ]
        get_count = len(values)
        self.queue.add_values(values)
        self.queue.get_values(get_count)
        self.assertTrue(self.queue.is_queue_empty())


class TestRedisSetQueue(TestRedisListQueue):
    '''
    Test RedisSetQueue, child of RedisQueue
    - All tests in TestRedisListQueue are run using set function strings
    '''
    queue_name = 'testredissetQ'
    queue_type = UuidQueueTypes.REDIS_SET
    def _get_all(self):
        '''Helper function to get all items in queue'''
        return list(self.client.smembers(self.queue_name))


class TestRedisListPipeQueue(TestRedisListQueue):
    '''
    Test RedisListPipeQueue, child of RedisPipeQueue<-RedisQueue
    - All tests in TestRedisListQueue are run using set function strings
    '''
    queue_name = 'testredissetQ'
    queue_type = UuidQueueTypes.REDIS_LIST_PIPE

    @data(
        # get_count
        (4) #, (40)
    )
    def test_get_values(self, get_count):
        '''
        Test RedisListQueue get_values through UuidBaseQueue
        - Greater than or equal added values
        '''
        values = [
            'id_' + str(val)
            for val in range(1, 4)
        ]
        assert False
        self.queue.add_values(values)
        got_values, call_cnt = self.queue.get_values(get_count)
        self.assertEqual(call_cnt, len(values))
        self.assertEqual(self.queue.queue_length(), 0)
        all_values = self._get_all()
        self.assertListEqual(all_values, [])
        got_values.sort()
        values.sort()
        self.assertListEqual(got_values, values)
