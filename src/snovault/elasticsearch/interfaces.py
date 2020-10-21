from zope.interface import Interface

# Registry tool id
APP_FACTORY = 'app_factory'
ELASTIC_SEARCH = 'elasticsearch'
SNP_SEARCH_ES = 'snp_search'
INDEXER = 'indexer'
INDEXER_STORE = f"{INDEXER}_store"
INDEXER_STATE_TAG = f"{INDEXER}_state:hash"
INDEXER_EVENTS_TAG = f"{INDEXER}_event"
INDEXER_EVENTS_LIST = f"{INDEXER_EVENTS_TAG}:list"
RESOURCES_INDEX = 'snovault-resources'


class ICachedItem(Interface):
    """ Marker for cached Item
    """
