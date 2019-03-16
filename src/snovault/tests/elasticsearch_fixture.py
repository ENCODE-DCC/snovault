import os.path
import sys
from time import sleep
try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess


def server_process(datadir, host='127.0.0.1', port=9201, prefix='', echo=False):
    args = [
        os.path.join(prefix, 'elasticsearch'),
        '-Enetwork.host=%s' % host,
        '-Ehttp.port=%d' % port,
        '-Epath.data=%s' % os.path.join(datadir, 'data'),
        '-Epath.logs=%s' % os.path.join(datadir, 'logs'),
    ]
    if os.environ.get('TRAVIS'):
        print('IN TRAVIS')
        echo = True
        args.append('-Epath.conf=%s/conf' % os.environ['TRAVIS_BUILD_DIR'])
    elif os.path.exists('/etc/elasticsearch'):
        print('NOT IN TRAVIS')
        args.append('-Epath.conf=./conf')
    print(args)
    process = subprocess.Popen(
        args,
        close_fds=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    SUCCESS_LINE = b'started\n'

    lines = []
    for line in iter(process.stdout.readline, b''):
        if echo:
            sys.stdout.write(line.decode('utf-8'))
        lines.append(line)
        if line.endswith(SUCCESS_LINE):
            print('detected start, broke')
            break
    else:
        code = process.wait()
        msg = ('Process return code: %d\n' % code) + b''.join(lines).decode('utf-8')
        raise Exception(msg)

    if not echo:
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
