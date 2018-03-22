import tweepy
import yaml
import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from es_mapping import *
from elasticsearch.helpers import bulk

# import logging
import logging
from logging.config import fileConfig

fileConfig('./logging_config.ini')
logger = logging.getLogger()


def read_config(file_path):
    """Import configurations from the config yaml file.

    :param file_path: The path to the config yaml file.
    :return: credentials as a Python dict.
    """
    with open(file_path, 'r') as f:
        config = yaml.load(f)
    return config


def twitter_authorizer(**kwargs):
    """Twitter API authentication.

    :param kwargs: Twitter API credentials.
    :return: Twitter API wrapper.
    """
    auth = tweepy.OAuthHandler(kwargs['consumer_key'],
                               kwargs['consumer_secret'])
    auth.set_access_token(kwargs['access_token'],
                          kwargs['access_token_secret'])
    return tweepy.API(auth)


def aws_authorizer(**kwargs):
    """AWS Python SDK authentication.

    :param kwargs: AWS SDK credentials.
    :return: Boto3 session.
    """
    session = boto3.Session(aws_access_key_id=kwargs['aws_access_key_id'],
                            aws_secret_access_key=kwargs['aws_secret_access_key'],
                            region_name=kwargs['region_name'])
    return session


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


def json_parser(raw_data):
    data = raw_data.strip().split('\n')
    return [json.loads(x) for x in data]


def get_sentiment(text, **kwargs):
    if 'comprehend' not in kwargs:
        raise ValueError('comprehend not available')
    comprehend = kwargs.get('comprehend')
    sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    return {k: v for k, v in sentiment.items() if k in ('Sentiment', 'SentimentScore',)}


def tweet_extractor(tweet, sentiment=None, **kwargs):
    # extract hashtags and user_mentions
    hashtags = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
    mentions = [user_mention['screen_name'] for user_mention in tweet['entities']['user_mentions']]
    # extracted data
    data = {'id_str': tweet['id_str'],
            'created_at': tweet['created_at'],
            'timestamp_ms': tweet['timestamp_ms'],
            'text': tweet['text'],
            'hashtags': hashtags,
            'mentions': mentions,
            'user': tweet['user']['screen_name']}

    # get geo-coordinates if available
    if tweet.get('geo') is not None:
        data['coordinates'] = tweet['geo'].get('coordinates')[::-1]
    # get city if available
    if tweet.get('place') is not None and tweet['place'].get('place_type') == 'city':
        data['city'] = tweet['place'].get('full_name')
    # change text/hashtags/user_mentions if extend_tweet is available
    if tweet.get('extended_tweet'):
        extended_tweet = tweet['extended_tweet']
        if extended_tweet.get('full_text'):
            data['text'] = extended_tweet['full_text']
        hashtags = [hashtag['text'] for hashtag in extended_tweet['entities']['hashtags']]
        if hashtags:
            data['hashtags'] = hashtags
        mentions = [user_mention['screen_name'] for user_mention in extended_tweet['entities']['user_mentions']]
        if mentions:
            data['mentions'] = mentions

    # run sentiment function
    if sentiment is not None:
        data['sentiment'] = sentiment(text=data['text'], **kwargs)
    # output extracted data
    return data


def s3_to_es_uploader(s3, es, bucket, key, extractor, upload=False):
    bucket, key = bucket, key
    body = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
    tweets = []
    bulk_size, count = 25, 0
    for raw_tweet in json_parser(raw_data=body):
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
    pass