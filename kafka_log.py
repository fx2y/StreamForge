import asyncio
import logging

from kafka import KafkaProducer


class KafkaLog:
    """
    A class for ingesting data into a Kafka topic.
    """

    def __init__(self, topic_name):
        """
        Initializes a Kafka producer for the given topic.

        :param topic_name: The name of the Kafka topic.
        """
        self.producer = self._create_producer()
        self.topic_name = topic_name

    def _create_producer(self):
        """
        Creates a Kafka producer with the default configuration.

        :return: A Kafka producer instance.
        """
        try:
            return KafkaProducer(bootstrap_servers=['localhost:9092'])
        except Exception as e:
            logging.error(f"Error creating Kafka producer: {e}")
            return None

    async def ingest(self, data):
        """
        Ingests the given data into the Kafka topic.

        :param data: The data to be ingested.
        """
        try:
            future = self.producer.send(self.topic_name, data.encode('utf-8'))
            result = await future.get()
            logging.debug(f"Data ingested into Kafka topic {self.topic_name}: {result}")
        except Exception as e:
            logging.error(f"Error ingesting data: {e}")

    def send(self, data):
        """
        Sends the given data to the Kafka topic.

        :param data: The data to be sent.
        """
        asyncio.run(self.ingest(data))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    # create an instance of the KafkaLog class
    log = KafkaLog('my_topic')

    # send some data to the Kafka topic
    data = '{"name": "John", "age": 30}'
    log.send(data)

    # wait for the data to be ingested
    asyncio.run(asyncio.sleep(1))
