"""\
Example.

    %(prog)s production.ini

"""

from webtest import TestApp
from snovault.elasticsearch import ELASTIC_SEARCH
import atexit
import datetime
import elasticsearch.exceptions
import json
import logging
import os
import psycopg2
import signal
import sqlalchemy.exc
import sys
import threading
import time
from urllib.parse import parse_qsl

log = logging.getLogger(__name__)

EPILOG = __doc__
DEFAULT_INTERVAL = 3  # 3 second default
PY2 = sys.version_info[0] == 2

# We need this because of MVCC visibility.
# See slide 9 at http://momjian.us/main/writings/pgsql/mvcc.pdf
# https://devcenter.heroku.com/articles/postgresql-concurrency


def run(testapp, interval=DEFAULT_INTERVAL, dry_run=False, path='/index', update_status=None):
    log.info('___INDEXER LISTENER STARTING___')
    listening = False
    timestamp = datetime.datetime.now().isoformat()
    update_status(
        listening=listening,
        status='indexing',
        timestamp=timestamp
    )

    # Make sure elasticsearch is up before trying to index.
    if path == '/index_file':
        return
    else:
        es = testapp.app.registry[ELASTIC_SEARCH]
    es.info()

    # main listening loop
    while True:
        try:
            res = testapp.post_json(path, {
                'record': True,
                'dry_run': dry_run
            })
        except Exception as e:
            timestamp = datetime.datetime.now().isoformat()
            log.exception('index failed')
            update_status(error={
                'error': repr(e),
                'timestamp': timestamp,
            })
        else:
            timestamp = datetime.datetime.now().isoformat()
            result = res.json
            result['stats'] = {
                k: int(v) for k, v in parse_qsl(
                    res.headers.get('X-Stats', ''))
            }
            result['timestamp'] = timestamp
            update_status(last_result=result)
            if result.get('indexing_status') == 'finished':
                update_status(result=result)
                if result.get('errors'):
                    log.error('___INDEX LISTENER RESULT:___\n%s\n' % result)
                else:
                    log.debug('___INDEX LISTENER RESULT:___\n%s\n' % result)
        time.sleep(interval)


class ErrorHandlingThread(threading.Thread):
    if PY2:
        @property
        def _kwargs(self):
            return self._Thread__kwargs

        @property
        def _args(self):
            return self._Thread__args

        @property
        def _target(self):
            return self._Thread__target

    def run(self):
        # interval = self._kwargs.get('interval', DEFAULT_INTERVAL)
        interval = 60  # DB polling can and should be slower
        update_status = self._kwargs['update_status']
        while True:
            try:
                self._target(*self._args, **self._kwargs)
            except (psycopg2.OperationalError, sqlalchemy.exc.OperationalError, elasticsearch.exceptions.ConnectionError) as e:
                # Handle database restart
                log.warning('Database not there, maybe starting up: %r', e)
                timestamp = datetime.datetime.now().isoformat()
                update_status(
                    timestamp=timestamp,
                    status='sleeping',
                    error={'error': repr(e), 'timestamp': timestamp},
                )
                log.debug('sleeping')
                time.sleep(interval)
                continue
            except Exception:
                # Unfortunately mod_wsgi does not restart immediately
                log.exception('Exception in listener, restarting process at next request.')
                os.kill(os.getpid(), signal.SIGINT)
            break


def composite(loader, global_conf, **settings):
    listener = None

    # Register before testapp creation.
    @atexit.register
    def join_listener():
        if listener:
            log.debug('joining listening thread')
            listener.join()

    path = settings.get('path', '/index')

    # Composite app is used so we can load the main app
    app_name = settings.get('app', None)
    app = loader.get_app(app_name, global_conf=global_conf)
    username = settings.get('username', 'IMPORT')
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': username,
    }
    testapp = TestApp(app, environ)


    timestamp = datetime.datetime.now().isoformat()
    status_holder = {
        'status': {
            'status': 'starting listener',
            'started': timestamp,
            'errors': [],
            'results': [],
        },
    }

    def update_status(error=None, result=None, indexed=None, **kw):
        # Setting a value in a dictionary is atomic
        status = status_holder['status'].copy()
        status.update(**kw)
        if error is not None:
            status['errors'] = [error] + status['errors'][:2]
        if result is not None:
            status['results'] = [result] + status['results'][:9]
        status_holder['status'] = status

    kwargs = {
        'testapp': testapp,
        'update_status': update_status,
        'path': path,
    }
    if 'interval' in settings:
        kwargs['interval'] = float(settings['interval'])

    listener = ErrorHandlingThread(target=run, name='listener', kwargs=kwargs)
    listener.daemon = True
    log.debug('starting listener')
    listener.start()

    # Register before testapp creation.
    @atexit.register
    def shutdown_listener():
        log.debug('shutting down listening thread')

    def status_app(environ, start_response):
        status = '200 OK'
        response_headers = [('Content-type', 'application/json')]
        start_response(status, response_headers)
        return [json.dumps(status_holder['status'])]

    return status_app


def internal_app(configfile, app_name=None, username=None):
    from webtest import TestApp
    from pyramid import paster
    app = paster.get_app(configfile, app_name)
    if not username:
        username = 'IMPORT'
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': username,
    }
    return TestApp(app, environ)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Listen for changes from postgres and index in elasticsearch",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument(
        '--username', '-u', default='INDEXER', help="Import username")
    parser.add_argument(
        '--dry-run', action='store_true', help="Don't post to ES, just print")
    parser.add_argument(
        '-v', '--verbose', action='store_true', help="Print debug level logging")
    parser.add_argument(
        '--poll-interval', type=int, default=DEFAULT_INTERVAL,
        help="Poll interval between notifications")
    parser.add_argument(
        '--path', default='/index',
        help="Path of indexing view (/index or /index_file)")
    parser.add_argument('config_uri', help="path to configfile")
    args = parser.parse_args()

    logging.basicConfig()
    testapp = internal_app(args.config_uri, args.app_name, args.username)

    # Loading app will have configured from config file. Reconfigure here:
    if args.verbose or args.dry_run:
        logging.getLogger('snovault').setLevel(logging.DEBUG)

    return run(testapp, args.poll_interval, args.dry_run, args.path)


if __name__ == '__main__':
    main()
