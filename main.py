import kafka_adapter

# receive data from Kafka
data = kafka_adapter.receive_data_from_kafka('topic_name')

# deserialize the received data
deserialized_data = kafka_adapter.deserialize_data(data)
