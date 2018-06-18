# import libraries
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# import UDF
import utils


def es_connector(host, **config):
    aws_access_key_id = config.get('aws_access_key_id')
    aws_secret_access_key = config.get('aws_secret_access_key')
    region_name = config.get('region_name')
    # Create AWS Auth
    awsauth = AWS4Auth(aws_access_key_id, aws_secret_access_key, region_name, 'es')
    # Connect to Elasticsearch
    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )
    return es


if __name__ == '__main__':
    # Read config file
    config = utils.read_config('./credentials.yml')

    # Connect to es
    # TODO: Change `es_host` to your elasticsearch domain Endpoint
    es_host = 'search-worldcup-tweets-pystwusocqyuaf6tlxuyobkpzu.us-east-2.es.amazonaws.com'
    es = es_connector(host=es_host, **config)

    # Get the basic info from the current cluster.
    print(es.info())

    es_index = 'test-index'
    doc_type = 'test-type'
    id = 1

    # Add a JSON document to Elasticsearch.
    # # TODO: Modify the doc / Add more attributes
    # doc = {
    #     'author': 'Shu Yan',
    #     'text': 'Testing AWS elasticsearch service - Kibana',
    #     'created_at': str(time.ctime()),
    # }
    # # Add `id` by 1 everytime you insert an new doc
    # res = es.index(index=index,
    #                doc_type=doc_type,
    #                id=id, body=doc)
    # print(res)

    # # Get a JSON document from the index based on its id.
    # res = es.get(index=index, doc_type=doc_type, id=id)
    # print(res['_source'])

    # # Delete all documents
    # res = es.delete_by_query(index=es_index, doc_type=doc_type, body={"query": {"match_all": {}}})
    # print(res)

    # # Delete index
    # res = es.indices.delete(index=es_index, ignore=[400, 404])
    # print(res)