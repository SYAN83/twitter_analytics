import utils


if __name__ == '__main__':
    # Read config file
    config = utils.read_config(file_path='./credentials.yml')
    # Connect to AWS
    session = utils.aws_authorizer(**config)
    # Creat firehose client
    firehose = session.client('firehose')
    # List deliver streams
    print(firehose.list_delivery_streams())
    # TODO: Change `stream_name` below to your firehose stream name.
    stream_name = 'demo-tweet-to-s3'
    # TODO: Change `data` to any string or JSON object.
    data = 'testing_2'
    # Putting record to stream
    response = firehose.put_record(
        DeliveryStreamName=stream_name,
        Record={'Data': data}
    )
    print(response)



