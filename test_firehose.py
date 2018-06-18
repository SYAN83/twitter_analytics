import utils


def main():
    # Read config file
    config = utils.read_config(file_path='./credentials.yml')
    # Connect to AWS
    session = utils.aws_authorizer(**config)
    # Creat firehose client
    firehose = session.client('firehose')

    # List deliver streams
    print(firehose.list_delivery_streams())

    # Putting record to stream
    # TODO: Change `stream_name` below to your firehose stream name.
    stream_name = 'worldcup-tweets'
    # TODO: (Optional) Change `data` to any string.
    data = 'testing'
    response = firehose.put_record(
        DeliveryStreamName=stream_name,
        Record={'Data': data}
    )
    print(response)


if __name__ == '__main__':
    main()
