"""\
Migrate dataset type

"""
import logging
import transaction
from pyramid.paster import get_app
from pyramid.threadlocal import manager
from pyramid.testing import DummyRequest
from snovault.interfaces import (
    STORAGE,
    DBSESSION
)
from snovault.storage import (
    CurrentPropertySheet,
)

EPILOG = __doc__

logger = logging.getLogger(__name__)


test_uuids = ['1b18dab4-50be-4a1d-9b95-5e9fd840c8fb']


def run(app, dry_run=False):

    storage = app.registry[STORAGE].write
    session = app.registry[DBSESSION]
    to_delete = test_uuids
    import pdb;pdb.set_trace()
    # to_delete = storage.__iter__('talen')
    for rid in to_delete:
        model = storage.get_by_uuid(str(rid))
        sp = session.begin_nested()

        for cp in model.data.values():
            for p in cp.history:
                session.delete(p)
                logger.info("Queueing %s for deletion", p)
            session.delete(cp)
            logger.info("Queueng %s for deletion", cp)


        session.delete(model)
        try:
            sp.commit()
            logger.info('Deleted %s', model.rid)
        except:
            sp.rollback()
            logger.info('Rollback deletion of %s', model.rid)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Migrate dataset type", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--app-name', default="app", help="Pyramid app name in configfile")
    parser.add_argument('--abort', action='store_true', help="Rollback transaction")
    parser.add_argument('--dry-run', action='store_true', help="Don't actually do anything")
    parser.add_argument('config_uri', help="path to configfile")
    args = parser.parse_args()

    logging.basicConfig()
    app = get_app(args.config_uri, args.app_name)
    # Loading app will have configured from config file. Reconfigure here:
    logging.getLogger('snowflakes').setLevel(logging.DEBUG)

    raised = False
    try:
        run(app, args.dry_run)
    except:
        raised = True
        raise
    finally:
        if raised or args.abort:
            transaction.abort()
            logger.info('Rolled back.')
        else:
            transaction.commit()


if __name__ == '__main__':
    main()
