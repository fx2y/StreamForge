from flask import Flask, jsonify, request

from partitioning_service import PartitioningService

app = Flask(__name__)
partitioning_service = PartitioningService(nodes=["node1", "node2", "node3"], db_file="streamforge.db", replicas=3)


@app.route('/metadata/partitions')
def get_partitions_metadata():
    metadata = partitioning_service.partitioning_metadata.metadata
    return jsonify(metadata)


@app.route('/metadata/nodes/<node_id>')
def get_node_partitions(node_id):
    partitions = partitioning_service.get_node_partitions(node_id)
    return jsonify(partitions)


@app.route('/metadata/nodes/least-loaded')
def get_least_loaded_node():
    node = partitioning_service.get_least_loaded_node()
    return jsonify(node)


@app.route('/metadata/partitions/<partition_id>/node')
def get_partition_node(partition_id):
    node_id = partitioning_service.get_partition_node_balanced(partition_id)
    return jsonify(node_id)


@app.route('/api/query', methods=['POST'])
def query():
    data = request.get_json()
    query_type = data.get('type')
    if query_type == 'metadata':
        metadata = partitioning_service.partitioning_metadata.metadata
        return jsonify(metadata)
    elif query_type == 'node_partitions':
        node_id = data.get('node_id')
        partitions = partitioning_service.get_node_partitions(node_id)
        return jsonify(partitions)
    elif query_type == 'least_loaded_node':
        node = partitioning_service.get_least_loaded_node()
        return jsonify(node)
    elif query_type == 'partition_node':
        partition_id = data.get('partition_id')
        node_id = partitioning_service.get_partition_node_balanced(partition_id)
        return jsonify(node_id)
    else:
        return jsonify({'error': 'Invalid query type'})


if __name__ == '__main__':
    app.run()
