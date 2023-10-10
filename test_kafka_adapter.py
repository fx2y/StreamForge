import unittest
from unittest.mock import patch, Mock

from kafka_adapter import receive_data_from_kafka, deserialize_data


class TestKafkaAdapter(unittest.TestCase):

    @patch('kafka_adapter.KafkaConsumer')
    def test_receive_data_from_kafka(self, mock_consumer):
        # Arrange
        topic_name = 'test_topic'
        mock_message1 = Mock(value='test_message1')
        mock_message2 = Mock(value='test_message2')
        mock_consumer.return_value = [mock_message1, mock_message2]

        # Act
        result = receive_data_from_kafka(topic_name)

        # Assert
        mock_consumer.assert_called_once_with(topic_name)
        self.assertEqual(result, ['test_message1', 'test_message2'])

    @patch('kafka_adapter.Deserializer')
    def test_deserialize_data(self, mock_deserializer):
        # Arrange
        data = ['serialized_data1', 'serialized_data2']
        mock_deserialized_data1 = Mock()
        mock_deserialized_data2 = Mock()
        mock_deserializer.return_value.deserialize.side_effect = [mock_deserialized_data1, mock_deserialized_data2]
        expected_result = [mock_deserialized_data1, mock_deserialized_data2]

        # Act
        result = deserialize_data(data)

        # Assert
        mock_deserializer.assert_called_once_with()
        mock_deserializer.return_value.deserialize.assert_any_call('serialized_data1')
        mock_deserializer.return_value.deserialize.assert_any_call('serialized_data2')
        self.assertEqual(result, expected_result)


if __name__ == '__main__':
    unittest.main()
