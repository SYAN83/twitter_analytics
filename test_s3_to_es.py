from elasticsearch.helpers import bulk
import functools

# import UDF and mapping
import utils
from es_mapping import es_index, doc_type, mapping

# import logging
import logging
from logging.config import fileConfig

fileConfig('./logging_config.ini')
logger = logging.getLogger()


def s3_to_es_uploader(s3, es, bucket, key, extractor, upload=False):
    bucket, key = bucket, key
    body = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
    tweets = []
    bulk_size, count = 25, 0
    for raw_tweet in utils.json_parser(raw_data=body):
        parsed_tweet = extractor(raw_tweet)
        if upload:
            bulk_doc = {
                "_index": es_index,
                "_type": doc_type,
                "_id": parsed_tweet['id_str'],
                "_source": parsed_tweet
            }
            tweets.append(bulk_doc)
            if len(tweets) == bulk_size:
                success, _ = bulk(es, tweets)
                count += success
                logger.info('ElasticSearch indexed {} documents'.format(count))
                tweets = []
        else:
            print(parsed_tweet)
    else:
        if upload:
            success, _ = bulk(es, tweets)
            count += success
            logger.info('ElasticSearch indexed {} documents'.format(count))
    return count


if __name__ == '__main__':
    # Read config file
    config = utils.read_config('./credentials.yml')

    # Connect to es
    # TODO: Change `es_host` in credentials.yml to your elasticsearch domain Endpoint
    es_host = config['es_host']
    es = utils.es_connector(host=es_host, **config)

    # Create es index mapping (schema)
    if not es.indices.exists(index=es_index):
        logger.info('Creating mapping...')
        es.indices.create(index=es_index, body={'mappings': mapping})

    # Connect to S3 and Comprehend
    session = utils.aws_authorizer(**config)
    s3 = session.client(service_name='s3')
    comprehend = session.client(service_name='comprehend')

    # TODO: Change `bucket`
    bucket = 'worldcup-tweets'
    # TODO: Change `key` to any key (file path) available in your S3 bucket
    key = '2018/06/18/05/worldcup-tweets-1-2018-06-18-05-26-30-e08944d1-2dfa-432d-9933-813cc9a7d198'
    # Setting `sentiment` to None will disable calling Comprehend API
    extractor = functools.partial(utils.tweet_extractor, sentiment=utils.get_sentiment, comprehend=comprehend)
    # TODO: First test parse tweets by setting `upload=False`,
    # TODO: Then upload tweets to Elasticsearch by setting `upload=True`
    total = s3_to_es_uploader(s3=s3, es=es,
                              bucket=bucket, key=key,
                              extractor=extractor,
                              upload=True)
    print('Total tweets uploaded: {}'.format(total))
