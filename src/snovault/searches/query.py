from elasticsearch_dsl import Search as SearchDSL


class Query():
    '''
    Wrapper for elaticsearch-dsl query.
    '''

    def __init__(self, query=None):
        self.query = query or self.new_query()

    def new_query(self):
        return SearchDSL()

    def using(self, client):
        self.query = self.query.using(client)
        return self

    def get_query_string(self):
        pass

    def filter(self):
        pass

    def post_filter(self):
        pass

    def exclude(self):
        pass

    def sort(self):
        pass

    def indices(self):
        pass

    def agg(self):
        pass

    def to_dict(self):
        return self.query.to_dict()

    @classmethod
    def from_dict(cls, query_dict):
        return cls(
            query=cls().new_query().from_dict(
                query_dict
            )
        )
