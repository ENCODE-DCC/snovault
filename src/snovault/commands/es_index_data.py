from pyramid.paster import get_app
import logging
from webtest import TestApp

EPILOG = __doc__


def run(app, collections=None, last_xmin=None, uuids=None):
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'INDEXER',
    }
    testapp = TestApp(app, environ)
    post_body = {
        'last_xmin': last_xmin,
        'types': collections,
        'recovery': True,
        'record': True
    }
    if uuids:
        post_body['uuids'] = list(uuids)
    testapp.post_json('/index', post_body)


def main():
    ''' Indexes app data loaded to elasticsearch '''

    import argparse
    parser = argparse.ArgumentParser(
        description="Index data in Elastic Search", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--item-type', action='append', help="Item type")
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument('config_uri', help="path to configfile")
    args = parser.parse_args()

    logging.basicConfig()
    options = {
        'embed_cache.capacity': '5000',
        'indexer': 'true',
    }
    app = get_app(args.config_uri, args.app_name, options)

    # Loading app will have configured from config file. Reconfigure here:
    logging.getLogger('snovault').setLevel(logging.DEBUG)
    return run(app, args.item_type)


if __name__ == '__main__':
    main()
