from utils import *

def get_sentiment(text, **kwargs):
    if 'comprehend' not in kwargs:
        raise ValueError('comprehend not available')
    comprehend = kwargs['comprehend']
    sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    return {k: v for k, v in sentiment.items() if k in ('Sentiment', 'SentimentScore',)}




if __name__ == '__main__':
    config = read_config(file_path='./credentials.yml')
    session = aws_authorizer(**config)
    comprehend = session.client(service_name='comprehend')

    # TODO: (Optional) Define your own `docs` and get their sentiment scores
    # Echo (2nd Generation) review headers (5 stars/3 stars/1 star) from Amazon
    docs = ['Sound quality has been improved again! 3rd time\'s the charm.',
            'Hoped to replace our small bluetooth speakers. Not so much',
            'Very disappointed in the sound that was sold as "improved"']

    # # Single document API
    # for doc in docs:
    #     print(doc)
    #     print(get_sentiment(text=doc, comprehend=comprehend))

    # Batch processing documents (maximum 25 docuemnts in each batch)
    response = comprehend.batch_detect_sentiment(
        TextList=docs,
        LanguageCode='en'
    )
    for result in response['ResultList']:
        result['text'] = docs[result['Index']]
        del result['Index']
        print(result)
