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

    def __init__(self, session, DeliveryStreamName):
        logger.info('Initializing stream listener')
        logger.info('Connecting to Kinesis firehose')
        self.session = session
        self.firehose = self.session.client('firehose')
        self.DeliveryStreamName = DeliveryStreamName

    def on_connect(self):
        if self.DeliveryStreamName not in self.firehose.list_delivery_streams()['DeliveryStreamNames']:
            raise ConnectionAbortedError('DeliveryStream not set up yet.')
        else:
            logger.info('Stream Listener connected.')

    def on_data(self, raw_data):
        # Put Tweet data to Kinesis
        response = self.firehose.put_record(
            DeliveryStreamName=self.DeliveryStreamName,
            Record={'Data': raw_data},
        )
        logger.info('Status: {}\tText: {}'.format(response['ResponseMetadata']['HTTPStatusCode'],
                                                  json.loads(raw_data)['text']))

    def on_error(self, status_code):
        logger.error(status_code)
        return True


if __name__ == '__main__':
    # Read config file
    config = utils.read_config(file_path='./credentials.yml')
    # Connect to AWS
    session = utils.aws_authorizer(**config)
    # Connect to Twitter API
    api = utils.twitter_authorizer(**config)
    # TODO: Change `stream_name` to your firehose stream name.
    stream_name = 'demo-tweet-to-s3'
    listener = TweetStreamListener(session=session,
                                   DeliveryStreamName=stream_name)
    # TODO: (Optional) Change `retry_count` to a bigger number if your stream stops too often
    streamer = tweepy.Stream(auth=api.auth,
                             listener=listener,
                             retry_count=3)
    # Start streaming
    # TODO: Change `track` to different topics
    track = ['#deletefacebook']
    logger.info('Start streaming tweets containing one or more words in `{}`'.format(track))
    streamer.filter(track=track,
                    languages=['en'],
                    async=True)
    # TODO: (Optional) Change `300`(seconds) to your desired time
    time.sleep(600)
    # Stop streaming
    logger.info('Disconnecting twitter API...')
    streamer.disconnect()
