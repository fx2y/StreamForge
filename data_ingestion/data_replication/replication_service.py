import requests


class ReplicationService:
    def __init__(self, partitioning_service):
        self.partitioning_service = partitioning_service

    def replicate_data(self, partition_id, data):
        nodes = self.partitioning_service.partitioning_metadata.get_partition_nodes(partition_id)
        primary_node_id = nodes[0]
        # Send data to primary node
        self.send_data_to_node(primary_node_id, data)
        # Replicate data to replicas
        self.replicate_data_to_secondary_nodes(partition_id, data)

    def replicate_data_to_secondary_nodes(self, partition_id, data):
        nodes = self.partitioning_service.partitioning_metadata.get_partition_nodes(partition_id)
        secondary_node_ids = nodes[1:]
        for secondary_node_id in secondary_node_ids:
            # Send data to secondary node
            self.send_data_to_node(secondary_node_id, data)

    def send_data_to_node(self, node_id, data):
        # Send data to node
        url = f"http://{node_id}/data"
        response = requests.post(url, data=data)
        if response.status_code != 200:
            raise Exception(f"Failed to send data to node {node_id}")
