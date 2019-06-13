from elasticsearch_dsl import Search


class ResponseBuilder():

    def __init__(self, response_fields=[]):
        self.response = {}
        self.response_fields = response_fields
        self.validate_response_fields()

    def render(self):
        '''
        Expects response_fields will return dictionaries with unique keys.
        '''
        for f in self.response_fields:
            self.response.update(f.render())
        return self.response


class QueryBuilder():
    '''
    Interface for building specific queries.
    '''

    def __init__(self, params_parser, *args, **kwargs):
        self.params_parser = params_parser
        self.args = args
        self.kwargs = kwargs

    def build_query(self):
        '''
        Public method to be implemented by children.
        '''
        raise NotImplementedError


class BasicSearchQuery(QueryBuilder):

    def __init__(self, *args, **kwargs):
        super.__init__(args, kwargs)


    def build_query(self):
        s = Search(using=es, index='snovault-resources')
        s = s.query('query_string', query='chip-seq rna', fields=['_all'], default_operator='AND')
        s.aggs.bucket('types', 'terms', field='embedded.@type')
        s = s.post_filter('terms', principals_allowed__view=['system.Everyone'])
        s = s.post_filter('terms', **{'embedded.@type': ['Experiment']})
