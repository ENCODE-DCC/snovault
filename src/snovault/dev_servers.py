"""\
Examples
For the development.ini you must supply the paster app name:

    %(prog)s development.ini --app-name app --init --clear

"""
from pkg_resources import resource_filename
from pyramid.paster import get_app, get_appsettings

import atexit
import logging
import os.path
import select
import shutil
import sys
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
    parser.add_argument('--access-key',
                        help="store local or copy to s3, will generate and store access key for admin user", default=None)
    args = parser.parse_args()

    logging.basicConfig()
    # Loading app will have configured from config file. Reconfigure here:
    logging.getLogger('snowvault').setLevel(logging.DEBUG)

    # get the config and see if we want to connect to non-local servers
    config = get_appsettings(args.config_uri, args.app_name)

    from snovault.tests import elasticsearch_fixture, postgresql_fixture
    from snovault.elasticsearch import create_mapping
    datadir = os.path.abspath(args.datadir)
    pgdata = os.path.join(datadir, 'pgdata')
    esdata = os.path.join(datadir, 'esdata')
    ### comment out from HERE...
    if args.clear:
        for dirname in [pgdata, esdata]:
            if os.path.exists(dirname):
                shutil.rmtree(dirname)
    if args.init:
        postgresql_fixture.initdb(pgdata, echo=True)
    ### ... to HERE to disable recreation of test db
    ### may have to `rm /tmp/snovault/pgdata/postmaster.pid`

    postgres = postgresql_fixture.server_process(pgdata, echo=True)
    nginx = nginx_server_process(echo=True)
    processes = [postgres, nginx]
    if not config.get('elasticsearch.aws_auth'):
        elasticsearch = elasticsearch_fixture.server_process(esdata, echo=True)
        processes = [postgres, elasticsearch, nginx]

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


    app = get_app(args.config_uri, args.app_name)
    if args.init:
        create_mapping.run(app, check_first=False)

    if args.load:
        from pyramid.path import DottedNameResolver
        load_test_data = app.registry.settings.get('snovault.load_test_data')
        load_test_data = DottedNameResolver().resolve(load_test_data)
        load_test_data(app, args.access_key)

    print('Started. ^C to exit.')

    stdouts = [p.stdout for p in processes]

    # Ugly should probably use threads instead
    while True:
        readable, writable, err = select.select(stdouts, [], stdouts, 5)
        for stdout in readable:
            for line in iter(stdout.readline, b''):
                sys.stdout.write(line.decode('utf-8'))
        if err:
            for stdout in err:
                for line in iter(stdout.readline, b''):
                    sys.stdout.write(line.decode('utf-8'))
            break

if __name__ == '__main__':
    main()
