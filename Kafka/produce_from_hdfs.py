from kafka import KafkaProducer
from sys import argv
import time


if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=['broker:9092'])

    with open(argv[2]) as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if i != 0:
                producer.send(argv[1], bytes(line, 'UTF-8'))
        time.sleep(60)
