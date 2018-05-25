from snovault import stats
from unittest import mock
import pytest


@pytest.fixture
def mocked():
    return mock.patch.object(stats, 'log')


def test_stats_tween_logs_stats(testapp, mocked):
    ''' plus in this case we always log url '''
    with mocked as mocked_log :
        testapp.get("/")
        assert mocked_log.bind.call_count == 2
        assert mocked_log.bind.call_args_list[0] == mock.call(url='http://localhost/')
        mocked_log.bind.assert_called_with(db_count=mock.ANY, db_time=mock.ANY,
                                                rss_begin=mock.ANY, rss_change=mock.ANY,
                                                rss_end=mock.ANY, wsgi_begin=mock.ANY,
                                                url='http://localhost/',
                                                wsgi_end=mock.ANY, wsgi_time=mock.ANY)
    return


def test_stats_tween_logs_telemetry_id(testapp, mocked):
    with mocked as mocked_log :
        res = testapp.get("/?telemetry_id=test_telem")
        mocked_log.bind.assert_called()
        assert mocked_log.bind.call_count == 2
        assert mocked_log.bind.call_args_list[0] == mock.call(telemetry_id='test_telem',
                                                              url='http://localhost/?telemetry_id=test_telem')
        mocked_log.bind.assert_called_with(db_count=mock.ANY, db_time=mock.ANY,
                                           rss_begin=mock.ANY, rss_change=mock.ANY,
                                           rss_end=mock.ANY, wsgi_begin=mock.ANY,
                                           wsgi_end=mock.ANY, wsgi_time=mock.ANY,
                                           url='http://localhost/?telemetry_id=test_telem',
                                           telemetry_id='test_telem')

        # we should also return telem in the header
        assert 'telemetry_id=test_telem' in res.headers['X-Stats']
    return
