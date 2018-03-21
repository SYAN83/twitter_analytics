import tweepy
import yaml
import boto3
import json


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


def json_parser(raw_data):
    data = raw_data.strip().split('\n')
    return [json.loads(x) for x in data]


def get_sentiment(text, **kwargs):
    if 'comprehend' not in kwargs:
        raise ValueError('comprehend not available')
    comprehend = kwargs['comprehend']
    sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    return {k: v for k, v in sentiment.items() if k in ('Sentiment', 'SentimentScore',)}


def tweet_extractor(tweet, callback=get_sentiment, **kwargs):
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
    # get sentiment scores
    data['sentiment'] = callback(text=data['text'], **kwargs)
    # output extracted data
    return data


if __name__ == '__main__':
    config = read_config(file_path='./credentials.yml')
    session = aws_authorizer(**config)
    text = 'This is terr'
    print(get_sentiment(session=session, text=text))