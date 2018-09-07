from zope.interface import Interface

# Registry tool id
APP_FACTORY = 'app_factory'
ELASTIC_SEARCH = 'elasticsearch'
SNP_SEARCH_ES = 'snp_search'
INDEXER = 'indexer'
RESOURCES_INDEX = 'snovault-resources'

REGION_INDEXER_NAME = 'region' + INDEXER
VIS_INDEXER_NAME = 'vis' + INDEXER

class ICachedItem(Interface):
    """ Marker for cached Item
    """
