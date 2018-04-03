from zope.interface import Interface

# Registry tool id
APP_FACTORY = 'app_factory'
ELASTIC_SEARCH = 'elasticsearch'
SNP_SEARCH_ES = 'snp_search'
INDEXER = 'indexer'
INDEXER_QUEUE = 'indexer_queue'
INDEXER_QUEUE_MIRROR = 'indexer_queue_mirror'


class ICachedItem(Interface):
    """ Marker for cached Item
    """
