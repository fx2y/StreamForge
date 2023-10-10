import json


class Deserializer:
    def deserialize(self, serialized_data: str) -> dict:
        # Deserialize the data using the json.loads() method
        deserialized_data = json.loads(serialized_data)

        # Check if the deserialized data is a dictionary
        if not isinstance(deserialized_data, dict):
            # Raise a ValueError if the deserialized data is not a dictionary
            raise ValueError("Deserialized data is not a dictionary")

        # Return the deserialized data
        return deserialized_data


if __name__ == '__main__':
    # create an instance of the Deserializer class
    deserializer = Deserializer()

    # deserialize some data
    data = '{"name": "John", "age": 30}'
    deserialized_data = deserializer.deserialize(data)
    print(deserialized_data)
