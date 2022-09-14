import json

from kafka import KafkaConsumer, KafkaProducer
import requests
import jdatetime


month_mapping = {
    'فروردین': 1, 'اردیبهشت': 2, 'خرداد': 3, 'تیر': 4, 'مرداد': 5, 'شهریور': 6,
    'مهر': 7, 'آبان': 8, 'آذر': 9, 'دی': 10, 'بهمن': 11, 'اسفند': 12
}


def pre_process(data):

    data_res = requests.post(
        'http://es-container:9200/persian_analyzer/_analyze',
        json={
            "text": data,
            "analyzer": "rebuilt_persian"
        },
    )
    return ' '.join([record['token'] for record in data_res.json()['tokens']])


if __name__ == "__main__":
    res = requests.put(
        'http://es-container:9200/persian_analyzer',
        json={
            "settings": {
                "analysis": {
                    "char_filter": {
                        "zero_width_spaces": {
                            "type": "mapping",
                            "mappings": ["\\u200C=>\\u0020"]
                        }
                    },
                    "filter": {
                        "persian_stop": {
                            "type": "stop",
                            "stopwords": "_persian_"
                        }
                    },
                    "analyzer": {
                        "rebuilt_persian": {
                            "tokenizer": "standard",
                            "char_filter": ["zero_width_spaces"],
                            "filter": [
                                "lowercase",
                                "decimal_digit",
                                "arabic_normalization",
                                "persian_normalization",
                                "persian_stop"
                            ]
                        }
                    }
                }
            }
        }
    )

    consumer = KafkaConsumer('news', 'crypto',
                             group_id='pre-process',
                             bootstrap_servers=['broker:9092'])

    producer = KafkaProducer(bootstrap_servers=['broker:9092'])

    for i, msg in enumerate(consumer):

        try:
            if msg.topic == 'news':
                msg_splitted = msg.value.decode('UTF-8').split(',')
                title, text, summary = msg_splitted[3], msg_splitted[4], msg_splitted[6]

                title = pre_process(title)
                text = pre_process(text)
                summary = pre_process(summary)
                time = msg_splitted[8].split()
                time = time[1:] if len(time) == 7 else time
                year, month, day = int(time[3]), int(month_mapping[time[2]]), int(time[1])
                hour, minute = int(time[5].split(':')[0]), int(time[5].split(':')[1])

                message = {
                    'code': msg_splitted[2],
                    'title': title,
                    'text': text,
                    'category': msg_splitted[5],
                    'summary': summary,
                    'writer': msg_splitted[7],
                    '@timestamp': jdatetime.datetime(year, month, day, hour, minute).togregorian().isoformat(),
                    'tags': msg_splitted[9].split('-'),
                    'link': msg_splitted[10]
                }

                producer.send('news-persistence', bytes(json.dumps(message), 'UTF-8'))
                producer.send('news_statistics', bytes(json.dumps(message), 'UTF-8'))

            if msg.topic == 'crypto':
                msg_splitted = msg.value.decode('UTF-8').split('\t')

                date = msg_splitted[7].split()[0]
                time_ = msg_splitted[7].split()[1]
                year, month, day = int(date.split('/')[0]), int(date.split('/')[1]), int(date.split('/')[2])
                hour, minute = int(time_.split(':')[0]), int(time_.split(':')[1])

                message = {
                    'title_fa': msg_splitted[1],
                    'title_en': msg_splitted[2],
                    'symbol': msg_splitted[3],
                    'price_usd': float(msg_splitted[4]),
                    'price_irr': float(msg_splitted[5].replace(',', '')),
                    'delta_percentage': float(msg_splitted[6].replace('%', '')),
                    '@timestamp': jdatetime.datetime(year, month, day, hour, minute).togregorian().isoformat()
                }

                producer.send('crypto-persistence', bytes(json.dumps(message), 'UTF-8'))
                producer.send('crypto_statistics', bytes(json.dumps(message), 'UTF-8'))

            print(i)

        except Exception as e:
            print('one failed', e)






