import os.path
import shutil
import sys
from time import sleep
try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess


# This is required for local osx testing, circle version is seperate
_OSX_ES_VERSION = 5
# These are set in .circleci/config.  es version may not be in circle config
_CIRCLE_BUILD = os.environ.get('BASH_ENV') == '/home/circleci/.bashrc'
_CIRCLE_ES_VERSION = os.environ.get('ES_MAJOR_VERSION', str(_OSX_ES_VERSION))
# Optional Encoded configuraion repo
_ENCD_CONFIG_DIR = os.environ.get('ENCD_CONFIG_DIR')


def _get_args_and_env(esdata, esdata_override, kwargs):
    env = os.environ.copy()
    args = [
        os.path.join(kwargs.get('prefix', ''), 'elasticsearch'),
    ]
    if esdata_override:
        # ES configuration dir is specified, not default location.
        # 'esdata' is ignored and kwargs values are overidden by elasticsearch.yml
        esconfig = f"{esdata_override}/config"
        if kwargs['version'] <= 5:
            # How elasticsearch 5 sets config path
            args.append(f"-Epath.conf={esconfig}")
        else:
            # How elasticsearch 6+ sets config path
            env["ES_PATH_CONF"] = esconfig
        # How to add jvm options.  Must remove/rename default jvm.options file
        jvm_options = _get_jvm_options(f"{esconfig}/jvm.options")
        if jvm_options:
            env['ES_JAVA_OPTS'] = jvm_options
        return args, env
    # Default 'esdata' location
    args.extend([
        '-Enetwork.host=%s' % kwargs.get('host', '127.0.0.1'),
        '-Ehttp.port=%d' % kwargs.get('port', 9201),
        '-Epath.data=%s' % os.path.join(esdata, 'data'),
        '-Epath.logs=%s' % os.path.join(esdata, 'logs'),
    ])
    if _CIRCLE_BUILD and int(_CIRCLE_ES_VERSION) == 5:
        args.append('-Epath.conf=./conf')
    return args, env


def _get_config_override(esdata, kwargs):
    if _ENCD_CONFIG_DIR:
        # If found, elasticsearch.yml config overrides vars like logs/data/host/port/etc...
        es_data_dir = f"{_ENCD_CONFIG_DIR}/elasticsearch/es{kwargs['version']}"
        if kwargs.get('local_test', False):
            es_data_dir += 'test'
        if os.path.exists(f"{es_data_dir}/config/elasticsearch.yml"):
            return es_data_dir
    return None


def _get_jvm_options(jvm_options_path):
    with open(jvm_options_path, 'r') as fileh:
        return ' '.join([
            line.strip()
            for line in fileh.readlines()
            if line[0] == '-'
        ])


def _clear(esdata, esdata_override):
    rm_dirs = [esdata]
    if esdata_override:
        esconfig = f"{esdata_override}/config"
        rm_dirs.extend([f"{esdata_override}/data", f"{esdata_override}/logs", f"{esconfig}/scripts"])
        es_keystore_path = f"{esconfig}/elasticsearch.keystore"
        if os.path.exists(es_keystore_path):
            os.remove(es_keystore_path)
    for dir_path in rm_dirs:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)


def server_process(datadir, **kwargs):
    kwargs['version'] = kwargs.get('version', _OSX_ES_VERSION)
    esdata = os.path.join(datadir, 'data')
    esdata_override = _get_config_override(esdata, kwargs)
    if kwargs.get('clear', False):
        _clear(esdata, esdata_override)
    args, env = _get_args_and_env(esdata, esdata_override, kwargs)
    process = subprocess.Popen(
        args,
        close_fds=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )

    SUCCESS_LINE = b'started\n'
    lines = []
    for line in iter(process.stdout.readline, b''):
        if kwargs.get('echo', False):
            sys.stdout.write(line.decode('utf-8'))
        lines.append(line)
        if line.endswith(SUCCESS_LINE):
            print('detected start, broke')
            break
    else:
        code = process.wait()
        msg = ('Process return code: %d\n' % code) + b''.join(lines).decode('utf-8')
        raise Exception(msg)

    if not kwargs.get('echo', False):
        process.stdout.close()
    print('returning process')
    return process


def main():
    import atexit
    import shutil
    import tempfile
    datadir = tempfile.mkdtemp()

    print('Starting in dir: %s' % datadir)
    try:
        process = server_process(datadir, echo=True)
    except:
        shutil.rmtree(datadir)
        raise

    @atexit.register
    def cleanup_process():
        try:
            if process.poll() is None:
                process.terminate()
                for line in process.stdout:
                    sys.stdout.write(line.decode('utf-8'))
                process.wait()
        finally:
            shutil.rmtree(datadir)

    print('Started. ^C to exit.')

    try:
        for line in iter(process.stdout.readline, b''):
            sys.stdout.write(line.decode('utf-8'))
    except KeyboardInterrupt:
        raise SystemExit(0)


if __name__ == '__main__':
    main()
