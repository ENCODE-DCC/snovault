"""Tests and Fixtures for testing elasticsearch.indexer_log"""
import logging
import pytest # pylint: disable=import-error


TMP_DIR = 'tmp-index-logger-dir'
TEST_FILENAME = 'test_indexing_time'


@pytest.fixture
def index_logger_off():
    '''Returns default IndexerLog with do_log=False'''
    from snovault.elasticsearch.index_logger import IndexLogger
    return IndexLogger()


@pytest.fixture
def index_logger_on(tmpdir):
    '''Returns default IndexerLog with do_log=False'''
    from snovault.elasticsearch.index_logger import IndexLogger
    index_logger = IndexLogger(
        do_log=True,
        log_name=TEST_FILENAME,
        log_path=str(tmpdir.mkdir(TMP_DIR)),
    )
    return index_logger


@pytest.fixture
def fake_index_uuid_output():
    '''Builds some fake output from the Indexer.update_object'''
    return {
        'error_message': 'fake-error_message',
        'timestamp': 'fake-timestamp',
        'uuid': 'fake-uuid',
        'es_time': 0.123,
        'es_ecp': 'fake-es_ecp',
        'embed_time': 0.456,
        'embed_ecp': 'fake-embed_ecp',
        'doc_paths_zero':'fake-doc_paths_zero',
        'doc_item_type': 'fake-doc_item_type',
        'doc_embedded_uuids': 'fake-doc_embedded_uuids',
        'doc_linked_uuids': 'fake-doc_linked_uuids',
    }


# pylint: disable=redefined-outer-name
def test_log_off_init(index_logger_off):
    '''Test the default OFF index log'''
    assert hasattr(index_logger_off, '_do_log')
    assert getattr(index_logger_off, '_do_log') is False
    assert hasattr(index_logger_off, '_the_log')
    assert getattr(index_logger_off, '_the_log') is None
    assert hasattr(index_logger_off, 'log_name')
    assert getattr(index_logger_off, 'log_name') == 'indexing_time'
    assert hasattr(index_logger_off, 'log_path')
    assert (
        getattr(index_logger_off, 'log_path') ==
        './'
    )
    # pylint: disable=protected-access
    assert index_logger_off._get_log() is None


# pylint: disable=redefined-outer-name
def test_log_off_bad_output(index_logger_off):
    '''
    Test the default OFF index log does break with bad output
    '''
    return_value = index_logger_off.append_output({})
    assert return_value is None


# pylint: disable=redefined-outer-name
def test_log_off_pass_public(index_logger_off):
    '''
    Test the default OFF index log does break on public functions

    * append_output function is tested in test_log_off_bad_output
    '''
    assert index_logger_off._the_log is None
    new_log_return = index_logger_off.new_log(
            'a uuid_len', 'a xmin', 'a snapshot_id')
    assert new_log_return is None
    write_log_return = index_logger_off.write_log('some msg')
    assert write_log_return is None


# pylint: disable=redefined-outer-name
def test_log_on(index_logger_on):
    '''Test the ON index log with edited init args'''
    assert hasattr(index_logger_on, '_do_log')
    assert getattr(index_logger_on, '_do_log') is True
    assert hasattr(index_logger_on, '_the_log')
    assert getattr(index_logger_on, '_the_log') is None
    assert hasattr(index_logger_on, 'log_name')
    assert getattr(index_logger_on, 'log_name') == TEST_FILENAME
    assert hasattr(index_logger_on, 'log_path')
    assert TMP_DIR in getattr(index_logger_on, 'log_path')


# pylint: disable=redefined-outer-name
def test_log_on_close_reset_get(index_logger_on):
    '''
    Test the ON index _close_handlers, _reset_log, and _get_log functions

    * The log name does not change so we need to reset
    '''
    # pylint: disable=protected-access
    index_logger_on._reset_log()
    assert isinstance(index_logger_on._the_log, logging.Logger)
    assert len(index_logger_on._the_log.handlers) == 1
    index_logger_on._close_handlers()
    assert not  index_logger_on._the_log.handlers


# pylint: disable=redefined-outer-name
def test_log_on_write_new_append(index_logger_on, fake_index_uuid_output):
    '''Test the ON index log new_log nd append output'''
    # pylint: disable=protected-access
    uuid_len = 5
    xmin = 'fake-xmin'
    snapshot_id = 'fake-snapshot_id'
    index_logger_on.new_log(uuid_len, xmin, snapshot_id)
    index_logger_on.append_output(fake_index_uuid_output)
    log_handler = index_logger_on._the_log.handlers[0]
    log_file_path = log_handler.baseFilename
    lines = []
    with open(log_file_path, 'r') as file_handler:
        for line in file_handler.readlines():
            lines.append(line.strip())
    assert len(lines) == 2
    expected_new_line = (
        'fake-timestamp fake-doc_paths_zero 0.456000 '
        'fake-embed_ecp 0.123000 fake-es_ecp '
        'fake-doc_embedded_uuids fake-doc_linked_uuids'
    )
    new_line = lines[0].split(' ', 2)[-1]
    expected_new_line = (
        'Starting Indexing 5 with xmin=fake-xmin and snapshot_id=fake-snapshot_id'
    )
    assert  new_line == expected_new_line
    expected_append_line = (
        'fake-timestamp fake-doc_paths_zero 0.456000 '
        'fake-embed_ecp 0.123000 fake-es_ecp '
        'fake-doc_embedded_uuids fake-doc_linked_uuids'
    )
    append_line = lines[1].split(' ', 2)[-1]
    assert  append_line == expected_append_line
