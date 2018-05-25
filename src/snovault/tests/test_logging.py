from snovault import stats
from snovault.crud_views import log as crud_view_log
from unittest import mock
import pytest
from snovault.tests.test_post_put_patch import COLLECTION_URL, item_with_uuid


@pytest.fixture
def mocked():
    return mock.patch.object(stats, 'log')


def test_stats_tween_logs_stats(testapp, mocked):
    ''' plus in this case we always log url '''
    with mocked as mocked_log :
        testapp.get("/")
        assert mocked_log.bind.call_count == 2
        assert mocked_log.bind.call_args_list[0] == mock.call(url_path='/',
                                                              url_qs='',
                                                              host='localhost')
        mocked_log.bind.assert_called_with(db_count=mock.ANY, db_time=mock.ANY,
                                                rss_begin=mock.ANY, rss_change=mock.ANY,
                                                rss_end=mock.ANY, wsgi_begin=mock.ANY,
                                                url_path='/',
                                                url_qs='',
                                                host='localhost',
                                                wsgi_end=mock.ANY, wsgi_time=mock.ANY)
    return


def test_stats_tween_logs_telemetry_id(testapp, mocked):
    with mocked as mocked_log:
        res = testapp.get("/?telemetry_id=test_telem")
        mocked_log.bind.assert_called()
        assert mocked_log.bind.call_count == 2
        assert mocked_log.bind.call_args_list[0] == mock.call(telemetry_id='test_telem',
                                                              url_path='/',
                                                              url_qs='telemetry_id=test_telem',
                                                              host='localhost')
        mocked_log.bind.assert_called_with(db_count=mock.ANY, db_time=mock.ANY,
                                           rss_begin=mock.ANY, rss_change=mock.ANY,
                                           rss_end=mock.ANY, wsgi_begin=mock.ANY,
                                           wsgi_end=mock.ANY, wsgi_time=mock.ANY,
                                           url_path='/',
                                           url_qs='telemetry_id=test_telem',
                                           host='localhost',
                                           telemetry_id='test_telem')

        # we should also return telem in the header
        assert 'telemetry_id=test_telem' in res.headers['X-Stats']
    return


def test_telemetry_id_carries_through_logging(testapp, external_tx):
    mocked = mock.patch.object(crud_view_log, 'info')
    with mocked as mock_log:
        res = testapp.post_json(COLLECTION_URL + "?telemetry_id=test&log_action=action_test", item_with_uuid[0], status=201)
        mock_log.assert_called_with(event="add_to_indexing_queue", uuid=mock.ANY, sid=mock.ANY)
        # also make sure we have a logger that has defaultsset from stats.py
        logger = crud_view_log.bind()
        assert logger._context.get('url_path') == COLLECTION_URL
        assert logger._context.get('url_qs') == "telemetry_id=test&log_action=action_test"
        assert logger._context.get('host') == 'localhost'
        assert logger._context.get('telemetry_id') == 'test'
        assert logger._context.get('log_action') == 'action_test'

