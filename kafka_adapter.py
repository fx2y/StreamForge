from kafka import KafkaConsumer


class Deserializer:
    def deserialize(self, data):
        # implement deserialization logic here
        return data


def receive_data(topic_name):
    # create a Kafka consumer that subscribes to a topic
    consumer = KafkaConsumer(topic_name)

    # receive data from the Kafka consumer
    data = []
    for message in consumer:
        data.append(message.value)

    return data


def deserialize_data(data):
    deserializer = Deserializer()
    deserialized_data = []
    for item in data:
        deserialized_data.append(deserializer.deserialize(item))

    return deserialized_data
