from kafka import KafkaProducer
import csv
import json
from threading import Thread
import logging
import time


def kafka_python_producer_sync(producer, row, topic):
    producer.send(topic, value=row)
    print(row)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()

class KafkaProducer(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.producer = KafkaProducer(bootstrap_servers='34.123.212.95:9092',
                                                value_serializer=lambda v: json.dumps(v).encode('utf-8')) 
                                                
    def run(self):
        while True:
            try:
                with open('/Users/jonathan/Documents/JADS/year-1/data-engineering/DE-assignment-2/work_dir/data/F1_tweets.csv') as file:
                    reader = csv.DictReader(file, delimiter=",")
                    counter = 0
                    for row in reader:
                        if(counter <= 1000):
                            kafka_python_producer_sync(self.producer, row, 'tweet')
                            counter += 1
                file.close()
                time.sleep(30)
            except Exception as err:
                logging.info(f"Unexpected {err=}, {type(err)=}")
                time.sleep(30)
