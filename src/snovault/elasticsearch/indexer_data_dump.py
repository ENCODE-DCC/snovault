"""
Optionally writes indexing data to file
* Collects data but only writes if do_log is set
* Writes the dicts sent in to timestamped files
"""
import time
import json
from os import makedirs as os_makedirs
from os.path import (
    isdir as os_isdir,
    expanduser as os_expanduser,
)


ENCODED_HOME = os_expanduser("~")
INDEXING_LOGS_DIR = ENCODED_HOME + '/.indexing-logs'
INITIAL_WRITE_DIR = INDEXING_LOGS_DIR + '/initial'
REINDEX_WRITE_DIR = INDEXING_LOGS_DIR + '/reindexes'


def _dump_output_to_file(
        base_file_path,
        outputs,
        out_size=100000,
        pretty=False
    ):
    '''Dump indexer outputs to json in batches'''
    path_index = 0
    while outputs:
        path_index += 1
        if len(outputs) >= out_size:
            out = outputs[:out_size]
            outputs = outputs[out_size:]
        else:
            out = outputs[:]
            outputs = []
        file_path = '%s_batch-%s.json' % (
            base_file_path,
            str(path_index),
        )
        with open(file_path, 'w') as file_handler:
            args = {}
            if pretty:
                args = {'indent': 4, 'separators': (',', ': ')}
            json.dump(out, file_handler, **args)


class IndexDataDump(object):
    '''Wraps the logging module for output indexing process'''

    def __init__(self, indexer_name, registry, do_log=False):
        self._do_log = do_log
        self._index_info = self._get_indexer_info(indexer_name, registry)

    def _dump_intial_index(self):
        '''
        Returns dump directory if we should dump
        the initial indexing data
        * To dump more indexing data use _dump_reindex if possible
        or move the INITIAL_WRITE_DIR data folder manually.
        '''
        if self._do_log and not os_isdir(INITIAL_WRITE_DIR):
            try:
                os_makedirs(INITIAL_WRITE_DIR)
            except Exception as ecp:  # pylint: disable=broad-except
                print(
                    'MAKE WARN:',
                    'Could not create initial dump dir',
                    repr(ecp)
                )
                return None
            return INITIAL_WRITE_DIR
        return None
    def _dump_reindex(self, is_reindex):
        '''
        Returns dump directory if we should dump
        the reindexing data
        '''
        if (self._do_log and is_reindex and
                os_isdir(REINDEX_WRITE_DIR)):
            return REINDEX_WRITE_DIR
        return None

    @staticmethod
    def _get_indexer_info(indexer_name, registry):
        '''
        Builds generic indexer info from registry config
        * Added to the output file as a dict with uuid='indexer_info'
        * Similar to run_info function below
        '''
        index_info = {
            'index_name': indexer_name
        }
        index_info_keys = [
            'elasticsearch.server',
            'embed_cache.capacity',
            'indexer.chunk_size',
            'indexer.processes',
            'snovault.app_version',
            'snovault.elasticsearch.index',
            'snp_search.server',
            'sqlalchemy.url',
        ]
        for key in index_info_keys:
            index_info[key] = registry.settings.get(key)
        return index_info

    @staticmethod
    def _get_time_str():
        return str(int(time.time() * 10000000))

    @staticmethod
    def debug_short_indexer(uuids, uuids_size, start=0, end=-1):
        '''
        Cuts the indexer uuids size for debugging
        * Typically called right before looping over the uuids
        '''
        return_uuids = []
        cnt = 0
        uuids = list(uuids)
        for uuid in uuids[start:end]:
            cnt += 1
            if cnt < uuids_size:
                return_uuids.append(uuid)
        return return_uuids

    @staticmethod
    def get_embed_dict(uuid):
        '''Sub output dict for embed request per uuid'''
        return {
            'doc_embedded': None,
            'doc_linked': None,
            'doc_path': None,
            'doc_type': None,
            'doc_size': None,
            'end_time': None,
            'exception': None,
            'exception_type': None,
            'failed': False,
            'start_time': time.time(),
            'url': "/%s/@@index-data/" % uuid,
        }

    @staticmethod
    def get_es_dict(backoff):
        '''
        Sub output dict for es indexing per uuid
        * es indexing per uuid can retry so es dicts will be a list
        ordered with the backoff term from update objects in indexer.
        '''
        return {
            'backoff': backoff,
            'exception': None,
            'exception_type': None,
            'failed': False,
            'start_time': time.time(),
            'end_time': None,
        }

    @staticmethod
    def get_output_dict(pid, uuid, xmin):
        '''Output dict per uuid, contains, es_dicts and embed_dict'''
        return {
            'end_time': None,
            'embed_dict': None,
            'es_dicts': [],
            'pid': pid,
            'start_time': time.time(),
            'uuid': uuid,
            'xmin': xmin,
        }

    @staticmethod
    def get_run_info(
            pid,
            uuid_count,
            xmin,
            snapshot_id,
            **updates
        ):
        '''Similar to indexer_info.  Added to output with uuid='run_info'''
        run_info = {
            '_dump_size': 100000,
            '_is_reindex': False,
            'chunksize': None,
            'chunkiness': None,
            'end_time': None,
            'pid': pid,
            'processes': None,
            'snapshot_id': snapshot_id,
            'start_time': time.time(),
            'uuid': 'run_info',
            'uuid_count': uuid_count,
            'workers': None,
            'xmin': xmin,
        }
        if updates:
            run_info.update(updates)
        return run_info

    def handle_outputs(self, outputs, run_info):
        '''Do what settings say to do with outputs'''
        dump_path = None
        if self._dump_intial_index():
            dump_path = '%s/%s_uuids-%d' % (
                INITIAL_WRITE_DIR,
                self._get_time_str(),
                run_info['uuid_count'],
            )
            print('_dump_intial_index', dump_path)
        elif self._dump_reindex(run_info['_is_reindex']):
            time_str = self._get_time_str()
            out_dir = '%s/%s' % (REINDEX_WRITE_DIR, time_str)
            try:
                os_makedirs(out_dir)
            except Exception as ecp:  # pylint: disable=broad-except
                print(
                    'MAKE WARN:',
                    'Could not create reindex dump dir',
                    repr(ecp)
                )
            else:
                dump_path = '%s/uuids-%d' % (
                    out_dir,
                    run_info['uuid_count'],
                )
            print('_dump_reindex', dump_path)
        if dump_path:
            outputs.append(run_info)
            _dump_output_to_file(
                dump_path,
                outputs,
                out_size=run_info['_dump_size'],
                pretty=True,
            )
        return dump_path
