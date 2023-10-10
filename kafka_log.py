import asyncio

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

    async def ingest(self, data):
        try:
            # send the data to the Kafka topic
            await self.producer.send(self.topic_name, data.encode('utf-8'))
        except Exception as e:
            # handle ingestion errors here
            print(f"Error ingesting data: {e}")

    def send(self, data):
        asyncio.run(self.ingest(data))


if __name__ == '__main__':
    # create an instance of the KafkaLog class
    log = KafkaLog('my_topic')

    # send some data to the Kafka topic
    data = '{"name": "John", "age": 30}'
    log.send(data)

    # wait for the data to be ingested
    asyncio.run(asyncio.sleep(1))
