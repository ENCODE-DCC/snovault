import pytest
import os

from time import sleep
from subprocess import TimeoutExpired

from pyramid.paster import get_appsettings


def pytest_configure():
    import logging
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

    class Shorten(logging.Filter):
        max_len = 500

        def filter(self, record):
            if record.msg == '%r':
                record.msg = record.msg % record.args
                record.args = ()
            if len(record.msg) > self.max_len:
                record.msg = record.msg[:self.max_len] + '...'
            return True

    logging.getLogger('sqlalchemy.engine.base.Engine').addFilter(Shorten())


@pytest.mark.fixture_cost(10)
@pytest.yield_fixture(scope='session')
def engine_url(request):
    # Ideally this would use a different database on the same postgres server
    from urllib.parse import quote
    from .postgresql_fixture import initdb, server_process
    tmpdir = request.config._tmpdirhandler.mktemp('postgresql-engine', numbered=True)
    tmpdir = str(tmpdir)
    initdb(tmpdir)
    process = server_process(tmpdir)

    yield 'postgresql://postgres@:5432/postgres?host=%s' % quote(tmpdir)

    if process.poll() is None:
        process.terminate()
        process.wait()


@pytest.mark.fixture_cost(10)
@pytest.yield_fixture(scope='session')
def postgresql_server(request):
    from urllib.parse import quote
    from .postgresql_fixture import initdb, server_process
    tmpdir = request.config._tmpdirhandler.mktemp('postgresql', numbered=True)
    tmpdir = str(tmpdir)
    initdb(tmpdir)
    process = server_process(tmpdir)

    yield 'postgresql://postgres@:5432/postgres?host=%s' % quote(tmpdir)

    if process.poll() is None:
        process.terminate()
        process.wait()


@pytest.fixture(scope='session')
def elasticsearch_host_port():
    from webtest.http import get_free_port
    return get_free_port()


@pytest.mark.fixture_cost(10)
@pytest.yield_fixture(scope='session')
def elasticsearch_server(request, elasticsearch_host_port):
    from .elasticsearch_fixture import server_process
    host, port = elasticsearch_host_port
    tmpdir = request.config._tmpdirhandler.mktemp('elasticsearch', numbered=True)
    tmpdir = str(tmpdir)
    process = server_process(str(tmpdir), host=host, port=9201, echo=False)
    print('PORT CHANGED')
    yield 'http://%s:%d' % (host, 9201)

    if 'process' in locals() and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except TimeoutExpired:
            process.kill()


@pytest.mark.fixture_cost(10)
@pytest.yield_fixture(scope='session')
def redis_server(request):
    from .redis_storage_fixture import initdb, server_process
    datadir = str(request.config._tmpdirhandler.mktemp('redisdatatest', numbered=True))
    appsettings = get_appsettings('development.ini', name='app')
    # Required settings in config
    local_storage_host = appsettings['local_storage_host']
    local_storage_port = appsettings['local_storage_port']
    local_storage_redis_index = appsettings['local_storage_redis_index']
    local_storage_timeout = appsettings['local_storage_timeout']
    # Build fixture
    redis_config_path = initdb(datadir, local_storage_port, echo=True)
    process = server_process(redis_config_path, local_storage_port, local_storage_redis_index, echo=True)
    # Sleep for short time to allow redis db to initialize
    sleep(0.25)
    yield f"Redis testing: redis-cli -p {local_storage_port} -n {local_storage_redis_index})"
    if 'process' in locals() and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except TimeoutExpired:
            process.kill()


# http://docs.sqlalchemy.org/en/rel_0_8/orm/session.html#joining-a-session-into-an-external-transaction
# By binding the SQLAlchemy Session to an external transaction multiple testapp
# requests can be rolled back at the end of the test.

@pytest.yield_fixture(scope='session')
def conn(engine_url):
    from snovault.app import configure_engine
    from snovault.storage import Base

    engine_settings = {
        'sqlalchemy.url': engine_url,
    }

    engine = configure_engine(engine_settings)
    conn = engine.connect()
    tx = conn.begin()
    try:
        Base.metadata.create_all(bind=conn)
        yield conn
    finally:
        tx.rollback()
        conn.close()
        engine.dispose()


@pytest.fixture(scope='session')
def _DBSession(conn):
    import snovault.storage
    import zope.sqlalchemy
    from sqlalchemy import orm
    # ``server`` thread must be in same scope
    DBSession = orm.scoped_session(orm.sessionmaker(bind=conn), scopefunc=lambda: 0)
    zope.sqlalchemy.register(DBSession)
    snovault.storage.register(DBSession)
    return DBSession


@pytest.fixture(scope='session')
def DBSession(_DBSession, zsa_savepoints, check_constraints):
    return _DBSession


@pytest.yield_fixture
def external_tx(request, conn):
    # print('BEGIN external_tx')
    tx = conn.begin_nested()
    yield tx
    tx.rollback()
    # # The database should be empty unless a data fixture was loaded
    # from snovault.storage import Base
    # for table in Base.metadata.sorted_tables:
    #     assert conn.execute(table.count()).scalar() == 0


@pytest.fixture
def transaction(request, external_tx, zsa_savepoints, check_constraints):
    import transaction
    transaction.begin()
    request.addfinalizer(transaction.abort)
    return transaction


@pytest.yield_fixture(scope='session')
def zsa_savepoints(conn):
    """ Place a savepoint at the start of the zope transaction

    This means failed requests rollback to the db state when they began rather
    than that at the start of the test.
    """
    from transaction.interfaces import ISynchronizer
    from zope.interface import implementer

    @implementer(ISynchronizer)
    class Savepoints(object):
        def __init__(self, conn):
            self.conn = conn
            self.sp = None
            self.state = None

        def beforeCompletion(self, transaction):
            pass

        def afterCompletion(self, transaction):
            # txn be aborted a second time in manager.begin()
            if self.sp is None:
                return
            if self.state == 'commit':
                self.state = 'completion'
                self.sp.commit()
            else:
                self.state = 'abort'
                self.sp.rollback()
            self.sp = None
            self.state = 'done'

        def newTransaction(self, transaction):
            self.state = 'new'
            self.sp = self.conn.begin_nested()
            self.state = 'begun'
            transaction.addBeforeCommitHook(self._registerCommit)

        def _registerCommit(self):
            self.state = 'commit'

    zsa_savepoints = Savepoints(conn)

    import transaction
    transaction.manager.registerSynch(zsa_savepoints)

    yield zsa_savepoints
    transaction.manager.unregisterSynch(zsa_savepoints)


@pytest.fixture
def session(transaction, DBSession):
    """ Returns a setup session

    Depends on transaction as storage relies on some interaction there.
    """
    return DBSession()


@pytest.yield_fixture(scope='session')
def check_constraints(conn, _DBSession):
    '''Check deffered constraints on zope transaction commit.

    Deferred foreign key constraints are only checked at the outer transaction
    boundary, not at a savepoint. With the Pyramid transaction bound to a
    subtransaction check them manually.
    '''
    from transaction.interfaces import ISynchronizer
    from zope.interface import implementer

    @implementer(ISynchronizer)
    class CheckConstraints(object):
        def __init__(self, conn):
            self.conn = conn
            self.state = None

        def beforeCompletion(self, transaction):
            pass

        def afterCompletion(self, transaction):
            pass

        def newTransaction(self, transaction):

            @transaction.addBeforeCommitHook
            def set_constraints():
                self.state = 'checking'
                session = _DBSession()
                session.flush()
                sp = self.conn.begin_nested()
                try:
                    self.conn.execute('SET CONSTRAINTS ALL IMMEDIATE')
                except:
                    sp.rollback()
                    raise
                else:
                    self.conn.execute('SET CONSTRAINTS ALL DEFERRED')
                finally:
                    sp.commit()
                    self.state = None

    check_constraints = CheckConstraints(conn)

    import transaction
    transaction.manager.registerSynch(check_constraints)

    yield check_constraints

    transaction.manager.unregisterSynch(check_constraints)


@pytest.yield_fixture
def execute_counter(conn, zsa_savepoints, check_constraints):
    """ Count calls to execute
    """
    from contextlib import contextmanager
    from sqlalchemy import event

    class Counter(object):
        def __init__(self):
            self.reset()
            self.conn = conn

        def reset(self):
            self.count = 0

        @contextmanager
        def expect(self, count):
            start = self.count
            yield
            difference = self.count - start
            assert difference == count

    counter = Counter()

    @event.listens_for(conn, 'after_cursor_execute')
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        # Ignore the testing savepoints
        if zsa_savepoints.state != 'begun' or check_constraints.state == 'checking':
            return
        counter.count += 1

    yield counter

    event.remove(conn, 'after_cursor_execute', after_cursor_execute)


@pytest.yield_fixture
def no_deps(conn, DBSession):
    from sqlalchemy import event

    session = DBSession()

    @event.listens_for(session, 'after_flush')
    def check_dependencies(session, flush_context):
        assert not flush_context.cycles

    @event.listens_for(conn, "before_execute", retval=True)
    def before_execute(conn, clauseelement, multiparams, params):
        return clauseelement, multiparams, params

    yield

    event.remove(session, 'before_flush', check_dependencies)


@pytest.fixture(scope='session')
def wsgi_server_host_port(request):
    wsgi_args = dict(request.config.option.wsgi_args or ())
    if ('port_range.min' in wsgi_args and 'port_range.max' in wsgi_args):
        import socket
        import os
        # return available port in specified range if min and max are defined
        port_temp, port_max = int(wsgi_args['port_range.min']), int(wsgi_args['port_range.max'])
        port_assigned = False
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while not port_assigned and port_temp <= port_max:
            try:
                s.bind(('', port_temp))
                port_assigned = True
            except OSError:
                port_temp += 1
        if not port_assigned:
            # port failed to be assigned, so raise an error
            raise
        ip, port = s.getsockname()
        s.close()
        ip = os.environ.get('WEBTEST_SERVER_BIND', '127.0.0.1')
        return ip, port
    else:
        # otherwise get any free port
        from webtest.http import get_free_port
        return get_free_port()


@pytest.fixture(scope='session')
def wsgi_server_app(app):
    return app


@pytest.mark.fixture_cost(100)
@pytest.yield_fixture(scope='session')
def wsgi_server(request, wsgi_server_app, wsgi_server_host_port):
    from webtest.http import StopableWSGIServer
    host, port = wsgi_server_host_port

    server = StopableWSGIServer.create(
        wsgi_server_app,
        host=host,
        port=port,
        threads=1,
        channel_timeout=60,
        cleanup_interval=10,
        expose_tracebacks=True,
    )
    assert server.wait()
    print("wsgi server port {}".format(port))

    yield 'http://%s:%s' % wsgi_server_host_port

    server.shutdown()
