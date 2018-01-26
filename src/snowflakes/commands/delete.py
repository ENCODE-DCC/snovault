import argparse
import transaction
from pyramid.paster import get_app
from snovault.interfaces import STORAGE


def make_batch(iterable, batch_size):
    total = len(iterable)
    for i in range(0, total, batch_size):
        yield iterable[i:min(i + batch_size, total)]


def get_uuids_to_delete(infile):
    with open(infile, 'r') as f:
        uuids_to_delete = [
            line.strip()
            for line in f
            if line.strip() != ''
        ]
    print('{} uuids found.'.format(len(uuids_to_delete)))
    return uuids_to_delete


def delete_uuids(batch, storage, update):
    for rid in batch:
        try:
            print('Deleting {}'.format(rid))
            storage.delete_by_uuid(str(rid))
        except AttributeError:
            print('ERROR: UUID NOT FOUND. Skipping.')
    # Commit every batch.
    if update:
        print('Commiting')
        transaction.commit()


def run(app, infile, update, batch_size):
    if update:
        print('Transaction will commit.')
    else:
        print('Update not specified. Transaction will abort.')
    # Access delete_by_uuid function in storage.py.
    storage = app.registry[STORAGE].write
    # Get list of uuids.
    uuids_to_delete = get_uuids_to_delete(infile)
    # Chunk uuids into transaction groups.
    batch_uuids = make_batch(uuids_to_delete, batch_size)
    for i, batch in enumerate(batch_uuids):
        print('Batch {}:'.format(i))
        delete_uuids(batch, storage, update)
    if not update:
        print('Aborting')
        transaction.abort()


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--app-name', default='app', help='Pyramid app name in config.')
    parser.add_argument('--infile', required=True, help='Path to list of UUIDs to delete.')
    parser.add_argument('--update', action='store_true', help='Only commit transaction if specified.')
    parser.add_argument('--batch_size', default=1, type=int, help='Transaction commit batch_size.')
    parser.add_argument('config_uri', help='Path to config.')
    return parser.parse_args()


def main():
    args = parse_args()
    app = get_app(args.config_uri, args.app_name)
    with transaction.manager:
        run(app, args.infile, args.update, args.batch_size)


if __name__ == '__main__':
    main()
