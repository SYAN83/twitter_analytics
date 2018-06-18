# import libraries
import tweepy
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
    """
    A simple Twitter Streaming API wrapper
    """
    def __init__(self, session, DeliveryStreamName):
        logger.info('Initializing stream listener')
        super().__init__()
        self.counter = 0
        logger.info('Connecting to Kinesis firehose')
        self.session = session
        self.firehose = self.session.client('firehose')
        self.DeliveryStreamName = DeliveryStreamName

    def on_connect(self):
        if self.DeliveryStreamName not in self.firehose.list_delivery_streams()['DeliveryStreamNames']:
            raise ConnectionAbortedError('DeliveryStream not set up yet.')
        else:
            logger.info('Stream Listener connected.')

    def on_status(self, status):
        tweet = json.dumps(status._json) + '\n'
        response = self.firehose.put_record(
            DeliveryStreamName=self.DeliveryStreamName,
            Record={'Data': tweet},
        )
        logger.info('Status: {}\tText: {}'.format(response['ResponseMetadata']['HTTPStatusCode'],
                                                  status.text[:100]))
        # tweet counter increased by 1
        self.counter += 1

    def on_error(self, status_code):
        logger.error('Connection error: {}'.format(status_code))
        # Return True to restart
        return True


if __name__ == '__main__':
    # Read config file
    config = utils.read_config(file_path='./credentials.yml')
    # Connect to AWS
    session = utils.aws_authorizer(**config)
    # Connect to Twitter API
    api = utils.twitter_authorizer(**config)

    # TODO: Change `stream_name` to your firehose stream name.
    stream_name = 'worldcup-tweets'
    listener = TweetStreamListener(session=session,
                                   DeliveryStreamName=stream_name)
    # (Optional) Change `retry_count` to a bigger number if your stream stops too often
    streamer = tweepy.Stream(auth=api.auth,
                             listener=listener,
                             retry_count=8)
    # Start streaming

    # TODO: (Optional) Add/change topics in `track`.
    track = ['#worldcup', ]

    logger.info('Start streaming tweets containing one or more words in `{}`'.format(track))
    streamer.filter(track=track,
                    languages=['en'],
                    async=True)

    # TODO: (Optional) Change `300`(seconds) to your desired time
    time.sleep(3600)
    # Stop streaming
    logger.info('Disconnecting twitter API...')
    streamer.disconnect()
