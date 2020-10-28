"""\
Examples
For the development.ini you must supply the paster app name:

    %(prog)s development.ini --app-name app --init --clear

"""
from pkg_resources import resource_filename
from pyramid.paster import get_app, get_appsettings
from multiprocessing import Process

import atexit
import logging
import os.path
import select
import shutil
import sys
import pdb
try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess


EPILOG = __doc__

logger = logging.getLogger(__name__)


def nginx_server_process(prefix='', echo=False):
    args = [
        os.path.join(prefix, 'nginx'),
        '-c', resource_filename('snovault', 'nginx-dev.conf'),
        '-g', 'daemon off;'
    ]
    process = subprocess.Popen(
        args,
        close_fds=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    if not echo:
        process.stdout.close()

    if echo:
        print('Started: http://localhost:8000')

    return process

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Run development servers", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument('config_uri', help="path to configfile")
    parser.add_argument('--clear', action="store_true", help="Clear existing data")
    parser.add_argument('--init', action="store_true", help="Init database")
    parser.add_argument('--load', action="store_true", help="Load test set")
    parser.add_argument('--datadir', default='/tmp/snovault', help="path to datadir")
    args = parser.parse_args()

    appsettings = get_appsettings(args.config_uri, name='app')
    # Required settings in config
    local_storage_host = appsettings['local_storage_host']
    local_storage_port = appsettings['local_storage_port']
    local_storage_redis_index = appsettings['local_storage_redis_index']
    local_storage_timeout = appsettings['local_storage_timeout']

    logging.basicConfig()
    # Loading app will have configured from config file. Reconfigure here:
    logging.getLogger('snovault').setLevel(logging.INFO)

    from snovault.tests import elasticsearch_fixture, postgresql_fixture, redis_storage_fixture
    from snovault.elasticsearch import create_mapping
    datadir = os.path.abspath(args.datadir)
    pgdata = os.path.join(datadir, 'pgdata')
    esdata = os.path.join(datadir, 'esdata')
    redisdata = os.path.join(datadir, 'redisdata')
    if args.clear:
        for dirname in [pgdata, esdata, redisdata]:
            if os.path.exists(dirname):
                shutil.rmtree(dirname)
    if args.init:
        postgresql_fixture.initdb(pgdata, echo=True)

    postgres = postgresql_fixture.server_process(pgdata, echo=True)
    elasticsearch = elasticsearch_fixture.server_process(esdata, echo=True)
    nginx = nginx_server_process(echo=True)
    redis_config_path = redis_storage_fixture.initdb(redisdata, local_storage_port, echo=True)
    redis = redis_storage_fixture.server_process(redis_config_path, local_storage_port, local_storage_redis_index, echo=True)
    processes = [postgres, elasticsearch, nginx, redis]

    print_processes = []

    @atexit.register
    def cleanup_process():
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                for line in process.stdout:
                    sys.stdout.write(line.decode('utf-8'))
            except IOError:
                pass
            process.wait()
        for p in print_processes:
            p.terminate()

    if args.init:
        app = get_app(args.config_uri, args.app_name)
        create_mapping.run(app)

    if args.load:
        from pyramid.path import DottedNameResolver
        load_test_data = app.registry.settings.get('snovault.load_test_data')
        load_test_data = DottedNameResolver().resolve(load_test_data)
        load_test_data(app)

    print('Started. ^C to exit.')

    stdouts = [p.stdout for p in processes]

    def print_to_terminal(stdout):
        while True:
            for line in iter(stdout.readline, b''):
                sys.stdout.write(line.decode('utf-8'))


    readable, writable, err = select.select(stdouts, [], stdouts, 5)
    for stdout in readable:
        print_processes.append(Process(target=print_to_terminal, args=(stdout,)))
    for stdout in err:
        print_processes.append(Process(target=print_to_terminal, args=(stdout,)))
    for p in print_processes:
        p.start()

if __name__ == '__main__':
    main()
