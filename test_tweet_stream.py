import tweepy
import pprint
import json
import time

# import UDF
import utils

# import logging
import logging
from logging.config import fileConfig

fileConfig('./logging_config.ini')
logger = logging.getLogger()


class TweetStreamListener(tweepy.StreamListener):

    def __init__(self):
        logger.info('Initializing stream listener')

    def on_connect(self):
        logger.info('Stream Listener connected.')

    def on_data(self, raw_data):
        tweet = json.loads(raw_data, )
        # Display raw tweet
        pprint.pprint(tweet)
        # # Display created time and text only
        # print('Time: {}\tText: {}'.format(tweet['created_at'], tweet['text']))

    def on_error(self, status_code):
        logger.error(status_code)
        return True


if __name__ == '__main__':
    # Read config file
    config = utils.read_config(file_path='./credentials.yml')
    # Connect to Twitter API
    api = utils.twitter_authorizer(**config)
    listener = TweetStreamListener()
    streamer = tweepy.Stream(auth=api.auth,
                             listener=listener,
                             retry_count=3)
    # Start streaming
    # TODO: Change `track` to different topics
    track = ['#winterstorm','#snowstorm', 'snow', 'storm']
    logger.info('Start streaming tweets containing one or more words in `{}`'.format(track))
    streamer.filter(track=track,
                    languages=['en'],
                    async=True)
    # Stop streaming after 30 seconds
    time.sleep(30)
    logger.info('Disconnecting twitter API...')
    streamer.disconnect()
