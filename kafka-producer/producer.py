from kafka import KafkaProducer
import csv
import json

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


if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers='34.123.212.95:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')) 

    with open('/Users/jonathan/Documents/JADS/year-1/data-engineering/DE-assignment-2/work_dir/data/F1_tweets.csv') as file:
        reader = csv.DictReader(file, delimiter=",")
        counter = 0
        for row in reader:
            if(counter <= 0):
                kafka_python_producer_sync(producer, row, 'tweet')
                counter += 1
    file.close()
