# import libraries
import tweepy
import time

# import UDF
import utils

# logging
import logging
from logging.config import fileConfig

fileConfig('./logging_config.ini')
logger = logging.getLogger()


class TweetStreamListener(tweepy.StreamListener):
    """
    A simple Twitter Streaming API wrapper
    """
    def __init__(self):
        logger.info('Initializing stream listener')
        super().__init__()
        self.counter = 0

    def on_connect(self):
        logger.info('Stream Listener connected.')

    def on_status(self, status):
        # status is a instance of tweepy.Status class, status._json is the status data formatted as a python dict
        # TODO: (Optional) Explore other tweet object attributes
        # Ref: https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        print('\nCreated at: {}'.format(status.created_at))
        print(status.text)

        # tweet counter increased by 1
        self.counter += 1

    def on_error(self, status_code):
        logger.error('Connection error: {}'.format(status_code))
        # Return True to restart
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

    # TODO: (Optional) Add/change topics in `track`.
    track = ['#worldcup', ]

    logger.info('Start streaming tweets containing one or more words in `{}`'.format(track))
    streamer.filter(track=track,
                    languages=['en'],
                    async=True)
    # Stop streaming after t seconds
    t = 10
    time.sleep(t)
    logger.info('Disconnecting twitter API...')
    streamer.disconnect()
    logger.info('Total tweets received in {} seconds: {}'.format(t, listener.counter))
