import functools

# import UDF
import utils
from es_mapping import *


def lambda_handler(event, context):
    print('Lambda trigger begin...')

    # Read config file
    config = utils.read_config('./credentials.yml')

    # Connect to es
    # TODO: Change `es_host` to your elasticsearch domain Endpoint
    es_host = config['es_host']
    es = utils.es_connector(host=es_host, **config)
    print(es.info())

    # Check es index
    if not es.indices.exists(index=es_index):
        print('Creating mapping...')
        es.indices.create(index=es_index, body={'mappings': mapping})

    # Connect to S3 and comprehend
    session = utils.aws_authorizer(**config)
    s3 = session.client(service_name='s3')
    comprehend = session.client(service_name='comprehend')

    # Define extractor
    extractor = functools.partial(utils.tweet_extractor, sentiment=utils.get_sentiment, comprehend=comprehend)
    print('Importing tweets to ES...')
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print(bucket, key)
        total = utils.s3_to_es_uploader(s3=s3, es=es,
                                        bucket=bucket, key=key,
                                        extractor=extractor, upload=True)
        print('Total tweets uploaded: {}'.format(total))

    print('Lambda trigger end...')

    return event
