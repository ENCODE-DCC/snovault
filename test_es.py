from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth


if __name__ == '__main__':

    addresses = ['search-fourfront-builds-uhevxdzfcv7mkm5pj5svcri3aq.us-east-1.es.amazonaws.com:80']
    es_options = {'connection_class': RequestsHttpConnection,
                  'retry_on_timeout': True,
                  'maxsize': 50  # parallellism...
                 }
    # drop port if it's there
    host = addresses[0].split(':')
    auth = BotoAWSRequestsAuth(aws_host=host[0],
                               aws_region='us-east-1',
                               aws_service='es')
    es_options['connection_class'] = RequestsHttpConnection
    es_options['http_auth'] = auth

    es = Elasticsearch(addresses, **es_options)

    import pdb; pdb.set_trace()
    print(es)


