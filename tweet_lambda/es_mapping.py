es_index = 'tweet-index'
doc_type = 'tweet-type'
mapping = {
    doc_type: {
        'properties': {
            'id_str'
            'created_at': {'type': 'text'},
            'timestamp_ms': {'type': 'date'},
            'text': {'type': 'text'},
            'user': {'type': 'keyword'},
            'sentiment': {
                'properties': {
                    'Sentiment': {'type': 'keyword'},
                    'SentimentScore': {
                        'properties': {
                            'Positive': {'type': 'float'},
                            'Negative': {'type': 'float'},
                            'Neutral': {'type': 'float'},
                            'Mixed': {'type': 'float'}
                        }
                    }
                }
            },
            'hashtags': {'type':'keyword'},
            'mentions': {'type':'keyword'},
            'coordinates': {'type':'geo_point'},
            'city': {'type': 'keyword'},
        }
    }
}