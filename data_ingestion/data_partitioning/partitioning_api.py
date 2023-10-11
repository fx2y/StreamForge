from flask import Flask, jsonify

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
    node_id = partitioning_service.get_partition_node(partition_id)
    return jsonify(node_id)


if __name__ == '__main__':
    app.run()
