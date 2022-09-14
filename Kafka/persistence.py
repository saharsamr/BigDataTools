from kafka import KafkaConsumer
import requests


if __name__ == "__main__":

    consumer = KafkaConsumer('news-persistence', 'crypto-persistence',
                             group_id='persistence',
                             bootstrap_servers=['broker:9092'])

    for i, msg in enumerate(consumer):

        print(i)

        msg_value = msg.value.decode('UTF-8')
        msg_topic = msg.topic.split('-')[0]

        headers = {'Content-type': 'application/json'}
        data_res = requests.post(
            f'http://es-container:9200/{msg_topic}/_doc',
            msg_value,
            headers=headers
        )



