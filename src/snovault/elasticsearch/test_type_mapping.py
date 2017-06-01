from elasticsearch import RequestError
from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text, Mapping, Index
from elasticsearch_dsl.connections import connections

def create_mapping_by_type(in_type="test_type"):
    connections.create_connection(hosts=['http://localhost:9200'])
    es = connections.get_connection()
    type_index = Index(in_type)
    @type_index.doc_type
    class TestType(DocType):
        title = Text()
    type_index.settings(number_of_shards=2)
    # add "index_analyzer":"snovault_index_analyzer" to settings
    # add "search_analyzer":"snovault_search_analyzer" to settings
    type_index.create()

if __name__ == '__main__':
    create_mapping_by_type("zzgy")
