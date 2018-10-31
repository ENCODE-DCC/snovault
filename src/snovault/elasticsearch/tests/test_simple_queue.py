''''
Test the functionality of SimpleUuidServer and SimpleUuidClient
in the context of Indexer and MPIndexer
'''
from os import getpid
from unittest import (
    TestCase,
    mock,
)

from snovault.elasticsearch.simple_queue import (
    SimpleUuidClient,
    SimpleUuidServer,
)


MOCK_TIME = 123456.654321
QUEUE_RESET_MSG_BASE = 'Queue Server cannot reset. '


class TestSimpleServer(TestCase):
    '''Test Simple Uuid Server'''
    # pylint: disable=too-many-public-methods

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        '''Create a simple client to be used most tests'''
        cls.load_uuids = list(range(10, 99))
        cls.queue_server = SimpleUuidServer()

    def setUp(self):
        self.queue_server.reset(force=True)

    # private/protected
    def test_init_basic(self):
        '''Test a basic initialization'''
        self.assertListEqual([], self.queue_server.errors)
        # pylint: disable=protected-access
        self.assertListEqual([], self.queue_server._uuids)
        self.assertDictEqual({}, self.queue_server._client_conns)

    def test_get_uuid(self):
        '''Test _get_uuid'''
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        uuid = self.queue_server._get_uuid()
        self.assertEqual(uuid, self.load_uuids[0])

    def test_get_uuid_empty(self):
        '''Test _get_uuid with empty'''
        # pylint: disable=protected-access
        self.assertIsNone(self.queue_server._get_uuid())

    def test_load_uuid(self):
        '''Test _load_uuid'''
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        new_uuid = 'a-uuid'
        self.queue_server._load_uuid(new_uuid)
        self.assertEqual(new_uuid, self.queue_server._uuids[0])
        self.assertEqual(len(self.load_uuids) + 1, len(self.queue_server._uuids))

    # get_uuids
    def test_get_uuids(self):
        '''Test get_uuids'''
        client_id = '123-456'
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        expected_uuids = self.load_uuids[0:uuid_cnt]
        uuids = self.queue_server.get_uuids(client_id, uuid_cnt)
        self.assertListEqual(expected_uuids, uuids)

    def test_get_uuids_all(self):
        '''Test get_uuids all with new worker connection'''
        client_id = '123-456'
        uuid_cnt = -1
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        self.queue_server.get_uuids(client_id, uuid_cnt)
        self.assertListEqual(
            [client_id],
            list(self.queue_server._client_conns.keys())
        )
        only_conn = self.queue_server._client_conns[client_id]
        self.assertDictEqual(
            {'get_cnt': 1, 'uuid_cnt': len(self.load_uuids), 'results': None},
            only_conn
        )

    def test_get_uuids_conn_many(self):
        '''Test get_uuids with new worker connection'''
        client_ids = ['123-456', '654-321']
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        for client_id in client_ids:
            self.queue_server.get_uuids(client_id, uuid_cnt)
        conn_keys = list(self.queue_server._client_conns.keys())
        conn_keys.sort()
        self.assertListEqual(client_ids, conn_keys)

    def test_get_uuids_conn_one(self):
        '''Test get_uuids with new worker connection'''
        client_id = '123-456'
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        self.queue_server.get_uuids(client_id, uuid_cnt)
        self.assertListEqual(
            [client_id],
            list(self.queue_server._client_conns.keys())
        )
        only_conn = self.queue_server._client_conns[client_id]
        self.assertDictEqual(
            {'get_cnt': 1, 'uuid_cnt': uuid_cnt, 'results': None},
            only_conn
        )

    def test_get_uuids_conn_empty(self):
        '''Test get_uuids when empty'''
        client_id = '123-456'
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server.get_uuids(client_id, uuid_cnt)
        self.assertListEqual(
            [client_id],
            list(self.queue_server._client_conns.keys())
        )
        only_conn = self.queue_server._client_conns[client_id]
        self.assertDictEqual(
            {'get_cnt': 1, 'uuid_cnt': 0, 'results': None},
            only_conn
        )

    def test_get_uuids_conn_exist(self):
        '''Test get_uuids existing worker connection'''
        client_id = '123-456'
        uuid_cnt = 11
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        self.queue_server.get_uuids(client_id, uuid_cnt)
        self.queue_server.get_uuids(client_id, uuid_cnt)
        self.assertListEqual(
            [client_id],
            list(self.queue_server._client_conns.keys())
        )
        only_conn = self.queue_server._client_conns[client_id]
        self.assertDictEqual(
            {'get_cnt': 2, 'uuid_cnt': uuid_cnt, 'results': None},
            only_conn
        )

    def test_get_uuids_conn_too_many(self):
        '''Test get_uuids asking for too many'''
        client_id = '123-456'
        uuid_cnt = 110
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        self.queue_server.get_uuids(client_id, uuid_cnt)
        self.assertListEqual(
            [client_id],
            list(self.queue_server._client_conns.keys())
        )
        only_conn = self.queue_server._client_conns[client_id]
        self.assertDictEqual(
            {'get_cnt': 1, 'uuid_cnt': len(self.load_uuids), 'results': None},
            only_conn
        )

    # has_uuids
    def test_has_uuids(self):
        '''Test has_uuids'''
        # pylint: disable=protected-access
        self.queue_server._uuids = [1, 2, 3]
        self.assertTrue(self.queue_server.has_uuids())

    def test_has_uuids_empty(self):
        '''Test has_uuids when empty'''
        self.assertFalse(self.queue_server.has_uuids())

    # is_indexing
    def test_is_indexing_conn(self):
        '''Test is_indexing by connection without uuids'''
        client_id = '123-456'
        uuid_cnt = 110
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        self.queue_server.get_uuids(client_id, uuid_cnt)
        self.queue_server._client_conns[client_id]['uuid_cnt'] = 0
        self.queue_server._client_conns[client_id]['results'] = 'Some results'
        self.assertFalse(self.queue_server.is_indexing())

    def test_is_indexing_conn_full(self):
        '''Test is_indexing by connection with uuids'''
        client_id = '123-456'
        uuid_cnt = -1
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        self.queue_server.get_uuids(client_id, uuid_cnt)
        self.assertTrue(self.queue_server.is_indexing())

    def test_is_indexing_conn_no_result(self):
        '''Test is_indexing by connection without result'''
        client_id = '123-456'
        uuid_cnt = 110
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        self.queue_server.get_uuids(client_id, uuid_cnt)
        self.queue_server._client_conns[client_id]['uuid_cnt'] = 0
        self.assertTrue(self.queue_server.is_indexing())

    def test_is_indexing_errors(self):
        '''Test is_indexing by errors'''
        self.queue_server.errors = [1, 2, 3]
        self.assertTrue(self.queue_server.is_indexing())

    def test_is_indexing_uuids(self):
        '''Test is_indexing by uuids'''
        # pylint: disable=protected-access
        self.queue_server._uuids = [1, 2, 3]
        self.assertTrue(self.queue_server.is_indexing())

    # load_uuids
    def test_load_uuids(self):
        '''Test load_uuids'''
        len_load_uuids = self.queue_server.load_uuids(self.load_uuids.copy())
        # pylint: disable=protected-access
        self.assertListEqual(
            self.queue_server._uuids,
            list(reversed(self.load_uuids))
        )
        self.assertEqual(len(self.load_uuids), len_load_uuids)

    def test_load_uuids_full(self):
        '''Test load_uuids when has uuids already'''
        # pylint: disable=protected-access
        previous_uuids = ['1']
        self.queue_server._uuids = previous_uuids
        len_load_uuids = self.queue_server.load_uuids(self.load_uuids.copy())
        self.assertListEqual(
            previous_uuids,
            self.queue_server._uuids,
        )
        self.assertIsNone(len_load_uuids)

    # reset
    def test_reset(self):
        '''Test reset'''
        self.assertEqual('Okay', self.queue_server.reset())

    def test_reset_uuids(self):
        '''Test reset when uuids'''
        expected_msg = '%sUuids=%d' % (
            QUEUE_RESET_MSG_BASE,
            len(self.load_uuids),
        )
        self.queue_server.load_uuids(self.load_uuids.copy())
        msg = self.queue_server.reset()
        self.assertEqual(expected_msg, msg)

    def test_reset_errors(self):
        '''Test reset when errors'''
        errors = ['e1', 'e2']
        expected_msg = '%sErrors=%d' % (
            QUEUE_RESET_MSG_BASE,
            len(errors),
        )
        self.queue_server.errors = errors.copy()
        msg = self.queue_server.reset()
        self.assertEqual(expected_msg, msg)

    def test_reset_conn(self):
        '''Test reset when connection pending'''
        client_id = '123-456'
        uuid_cnt = 110
        expected_msg = '%sClient Pending(%s)' % (
            QUEUE_RESET_MSG_BASE,
            client_id,
        )
        # pylint: disable=protected-access
        self.queue_server._uuids = list(reversed(self.load_uuids))
        self.queue_server.get_uuids(client_id, uuid_cnt)
        msg = self.queue_server.reset()
        self.assertEqual(expected_msg, msg)

    # update_finished
    def test_update_finished(self):
        '''Test update_finished all success'''
        client_id = '123-456'
        uuid_cnt = 11
        results = {'errors': [], 'successes': uuid_cnt}
        self.queue_server.load_uuids(self.load_uuids.copy())
        self.queue_server.get_uuids(client_id, uuid_cnt)
        msg = self.queue_server.update_finished(client_id, results)
        self.assertEqual('Okay', msg)

    def test_update_finished_errors(self):
        '''Test update_finished with some errors'''
        client_id = '123-456'
        uuid_cnt = 11
        errors = [{'uuid': 1, 'error_message': 'some problem!'}]
        results = {'errors': errors, 'successes': uuid_cnt - len(errors)}
        self.queue_server.load_uuids(self.load_uuids.copy())
        self.queue_server.get_uuids(client_id, uuid_cnt)
        msg = self.queue_server.update_finished(client_id, results)
        self.assertEqual('Okay', msg)

    def test_update_finished_errors_all(self):
        '''Test update_finished with all errors'''
        client_id = '123-456'
        uuid_cnt = 2
        errors = [
            {'uuid': 1, 'error_message': 'some problem!'},
            {'uuid': 2, 'error_message': 'some problem!'},
        ]
        results = {'errors': errors, 'successes': uuid_cnt - len(errors)}
        self.queue_server.load_uuids(self.load_uuids.copy())
        self.queue_server.get_uuids(client_id, uuid_cnt)
        msg = self.queue_server.update_finished(client_id, results)
        self.assertEqual('Okay', msg)

    def test_update_finished_errors_bad(self):
        '''Test update_finished too many errors'''
        client_id = '123-456'
        uuid_cnt = 2
        expected_msg = 'Queue Server cannot close client(%s)' % client_id
        errors = [
            {'uuid': 1, 'error_message': 'some problem!'},
            {'uuid': 2, 'error_message': 'some problem!'},
            {'uuid': '?', 'error_message': 'some problem!'},
        ]
        results = {'errors': errors, 'successes': uuid_cnt - len(errors)}
        self.queue_server.load_uuids(self.load_uuids.copy())
        self.queue_server.get_uuids(client_id, uuid_cnt)
        msg = self.queue_server.update_finished(client_id, results)
        self.assertEqual(expected_msg, msg)

    def test_update_finished_no_client(self):
        '''Test update_finished when client does not exist'''
        client_id = '123-456'
        uuid_cnt = 11
        expected_msg = 'Client id(%s) DNE.' % client_id
        results = {'errors': [], 'successes': uuid_cnt}
        msg = self.queue_server.update_finished(client_id, results)
        self.assertEqual(expected_msg, msg)

    def test_update_finished_over(self):
        '''Test update_finished with too many successes'''
        client_id = '123-456'
        uuid_cnt = 2
        expected_msg = 'Queue Server cannot close client(%s)' % client_id
        errors = []
        results = {'errors': errors, 'successes': 1 + uuid_cnt - len(errors)}
        self.queue_server.load_uuids(self.load_uuids.copy())
        self.queue_server.get_uuids(client_id, uuid_cnt)
        msg = self.queue_server.update_finished(client_id, results)
        self.assertEqual(expected_msg, msg)


class TestSimpleUuidClient(TestCase):
    '''Test Simple Uuid Client'''

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        '''Create a simple client to be used most tests'''
        cls.processes = 1
        cls.chunk_size = 1
        cls.batch_size = 10
        cls.mock_server = SimpleUuidServer()
        uuids_loaded = [str(uuid) for uuid in range(100, 150)]
        cls.mock_return_uuids = uuids_loaded[0:cls.batch_size]
        cls.mock_server.get_uuids = mock.MagicMock(
            return_value=cls.mock_return_uuids
        )
        cls.mock_server.update_finished = mock.MagicMock(return_value=None)
        args = (cls.processes, cls.chunk_size, cls.batch_size, cls.mock_server)
        cls.simple_client = SimpleUuidClient(*args)

    def test_init_basic(self):
        '''Test a basic initialization and _get_client_id'''
        self.assertEqual(self.processes, self.simple_client.processes)
        self.assertEqual(self.chunk_size, self.simple_client.chunk_size)
        self.assertEqual(self.batch_size, self.simple_client.batch_size)
        self.assertTrue(self.mock_server is self.simple_client.server_conn)
        client_id = '{}-{}'.format(
            getpid(),
            int(MOCK_TIME * 1000000),
        )
        self.assertEqual(client_id, self.simple_client.client_id)

    def test_get_uuids_called(self):
        '''Test client get_uuids calls server get_uuids'''
        self.simple_client.get_uuids()
        self.assertEqual(1, self.mock_server.get_uuids.call_count)
        self.mock_server.get_uuids.assert_called_with(
            self.simple_client.client_id,
            cnt=self.batch_size,
        )
        self.assertListEqual(
            self.mock_return_uuids,
            self.mock_server.get_uuids.return_value
        )

    def test_update_finished_called(self):
        '''Test client update_finished calls server update_finished'''
        results = {}
        self.simple_client.update_finished(results)
        self.assertEqual(1, self.mock_server.update_finished.call_count)
        self.mock_server.update_finished.assert_called_with(
            self.simple_client.client_id,
            results,
        )
        self.assertIsNone(self.mock_server.update_finished.return_value)
