from kafka import KafkaProducer


class KafkaLog:
    def __init__(self, topic_name):
        try:
            self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
            self.topic_name = topic_name
        except Exception as e:
            # handle Kafka producer errors here
            print(f"Error creating Kafka producer: {e}")
            self.producer = None

    def ingest(self, data):
        try:
            # send the data to the Kafka topic
            self.producer.send(self.topic_name, data.encode('utf-8'))
        except Exception as e:
            # handle ingestion errors here
            print(f"Error ingesting data: {e}")


if __name__ == '__main__':
    # create an instance of the KafkaLog class
    log = KafkaLog('topic_name')

    # ingest some data
    data = '{"name": "John", "age": 30}'
    log.ingest(data)
