from kafka import KafkaProducer
import csv
import json
from threading import Thread
import logging
import time


def kafka_python_producer_sync(producer, row, topic):
    producer.send(topic, value=row)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()

class F1Producer(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.producer = KafkaProducer(bootstrap_servers='35.225.244.213:9092',
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8')) 
                                                
    def run(self):
        
        while True:
            try:
                with open('/Users/jonathan/Documents/JADS/year-1/data-engineering/DE-assignment-2/work_dir/data/F1_tweets.csv') as file:
                    reader = csv.DictReader(file, delimiter=",")
                    lines = [row for row in reader]
                    # for row in reader:
                    kafka_python_producer_sync(self.producer, lines[:2], 'tweet')
                    
                    # counter = 0
                    # if(counter <= 1):
                    #         kafka_python_producer_sync(self.producer, reader[:,10], 'tweet')
                    #         counter += 1
                    # for row in reader:
                        
                file.close()
                time.sleep(30)
            except Exception as err:
                print(err)
                logging.info(f"Unexpected {err=}, {type(err)=}")
                time.sleep(30)

if __name__ == '__main__':
    producer = F1Producer()
    producer.run()