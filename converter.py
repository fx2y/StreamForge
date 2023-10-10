from dataclasses import dataclass
from typing import Dict

import ujson


@dataclass
class Target:
    name: str
    age: int


class Converter:
    def convert_to_target(self, data: Dict[str, str]) -> Target:
        if 'name' not in data or 'age' not in data:
            raise ValueError('Data is missing required fields')

        return Target(**data)

    def serialize_data(self, data: Target) -> str:
        return ujson.dumps({'name': data.name, 'age': data.age})

    def deserialize_data(self, data: str) -> Target:
        json_data = ujson.loads(data)
        return Target(name=json_data['name'], age=json_data['age'])


if __name__ == '__main__':
    # Create an instance of the Converter class
    converter = Converter()

    # Define some sample data to convert
    data = {'name': 'John Doe', 'age': '30'}

    # Convert the data to a Target object
    target = converter.convert_to_target(data)

    # Print the name and age of the Target object
    print(f"Name: {target.name}, Age: {target.age}")

    # Serialize the Target object to JSON format
    json_data = converter.serialize_data(target)

    # Print the serialized JSON data
    print(f"Serialized data: {json_data}")

    # Deserialize the JSON data back to a Target object
    deserialized_target = converter.deserialize_data(json_data)

    # Print the name and age of the deserialized Target object
    print(f"Deserialized name: {deserialized_target.name}, Deserialized age: {deserialized_target.age}")
