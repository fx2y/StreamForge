from kafka import KafkaConsumer


# Deserializer class for deserializing data
class Deserializer:
    def deserialize(self, data):
        # implement deserialization logic here
        return data


def receive_data_from_kafka(topic_name):
    """
    Receive data from Kafka using a Kafka consumer.

    Args:
        topic_name (str): The name of the Kafka topic to subscribe to.

    Returns:
        list: A list of messages received from the Kafka topic.
    """
    # create a Kafka consumer that subscribes to a topic
    consumer = KafkaConsumer(topic_name)

    # receive data from the Kafka consumer
    return [message.value for message in consumer]


def deserialize_data(data):
    """
    Deserialize data using a Deserializer class.

    Args:
        data (list): A list of serialized data.

    Returns:
        list: A list of deserialized data.
    """
    # Create a single instance of the Deserializer class
    deserializer = Deserializer()

    # Deserialize all the data items using the same deserializer instance
    return [deserializer.deserialize(item) for item in data]
