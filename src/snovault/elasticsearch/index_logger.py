"""
Indexing Data Log

* Logs uuid progress, timing, and exceptions as they happen.
"""
import logging
import time


class IndexLogger(object):
    '''Basic Logger tailored to our indexing process'''
    log_name = 'indexing_time'
    log_path = './'

    def __init__(self, do_log=False, log_name=None, log_path=None):
        self._do_log = do_log
        if log_name:
            self.log_name = log_name
        if log_path:
            self.log_path = log_path
        self._the_log = None

    def _close_handlers(self):
        '''Close all log handlers'''
        for handler in self._the_log.handlers:
            handler.close()
            self._the_log.removeHandler(handler)

    def _get_log(self):
        if self._do_log:
            # Timestamp converted to micro seconds to separate index logs
            file_name = "{}-{}.log".format(
                self.log_name,
                str(int(time.time() * 10000000)),
            )
            file_path = "{}/{}".format(self.log_path, file_name)
            level = logging.INFO
            formatter_str = '%(asctime)s %(message)s'
            log = logging.getLogger(self.log_name)
            hanlder = logging.FileHandler(file_path)
            formatter = logging.Formatter(formatter_str)
            hanlder.setFormatter(formatter)
            log.addHandler(hanlder)
            log.setLevel(level)
            return log
        return None

    def _reset_log(self):
        '''
        Close handlers and Get new log

        * Logger gets logs name so we call twice to clear and get a new one
        '''
        if self._do_log:
            if self._the_log:
                self._close_handlers()
            self._the_log = self._get_log()

    def append_output(self, output):
        '''Log the output dict from the Indexer.update_object class'''
        if 'embed_time' in output and isinstance(output['embed_time'], float):
            output['embed_time'] = '%0.6f' % output['embed_time']
        if 'es_time' in output and isinstance(output['es_time'], float):
            output['es_time'] = '%0.6f' % output['es_time']
        self.write_log(
            '{timestamp} {doc_path} {doc_type} '
            '{embed_time} {embed_ecp} '
            '{es_time} {es_ecp} '
            '{embeds} {linked} '
            ''.format(
                embeds=output.get('doc_embedded'),
                embed_ecp=output.get('embed_ecp'),
                embed_time=output.get('embed_time'),
                es_time=output.get('es_time'),
                es_ecp=output.get('es_ecp'),
                doc_path=output.get('doc_path'),
                linked=output.get('doc_linked'),
                doc_type=output.get('doc_type'),
                timestamp=output.get('timestamp'),
            )
        )


    def new_log(self, len_uuids, xmin, snapshot_id):
        '''Reset log and add start message'''
        self._reset_log()
        self.write_log(
            'Starting Indexing {} with xmin={} and snapshot_id={}'.format(
                len_uuids, xmin, snapshot_id,
            )
        )
        self.write_log(
            'date time timestamp doc_path doc_type '
            'embed_time embed_ecp es_time es_ecp embeds linked'
        )

    def write_log(self, msg, uuid=None, start_time=None):
        '''Handles all logging message'''
        if self._the_log:
            uuid = str(uuid) + ' ' if uuid else ''
            diff = ''
            if start_time:
                diff = ' %0.6f' % (time.time() - start_time)
            self._the_log.info("%s%s%s", uuid, msg, diff)
