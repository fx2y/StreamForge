from data_ingestion.data_partitioning.consistent_hashing import ConsistentHashing
from data_ingestion.data_partitioning.partitioning_metadata import PartitioningMetadata


class PartitioningService:
    def __init__(self, nodes, replicas=3):
        self.consistent_hashing = ConsistentHashing(nodes, replicas)
        self.partitioning_metadata = PartitioningMetadata()

    def add_partition(self, partition_id):
        node_id = self.consistent_hashing.get_node(partition_id)
        self.partitioning_metadata.add_partition(partition_id, node_id, self.consistent_hashing.replicas)

    def remove_partition(self, partition_id):
        node_id = self.get_partition_node(partition_id)
        self.partitioning_metadata.remove_partition(partition_id, node_id)

    def get_partition_node(self, partition_id):
        return self.consistent_hashing.get_node(partition_id)

    def get_node_partitions(self, node_id):
        return self.partitioning_metadata.get_node_partitions(node_id)

    def get_node_partitions_count(self, node_id):
        return len(self.partitioning_metadata.get_node_partitions(node_id))

    def get_least_loaded_node(self):
        nodes = list(self.consistent_hashing.ring.values())
        nodes.sort(key=self.get_node_partitions_count)
        return nodes[0] if nodes else None

    def get_partition_node_balanced(self, partition_id):
        node_id = self.consistent_hashing.get_node(partition_id)
        least_loaded_node = self.get_least_loaded_node()
        if least_loaded_node is not None and self.get_node_partitions_count(node_id) > self.get_node_partitions_count(
                least_loaded_node):
            node_id = least_loaded_node
        return node_id


if __name__ == '__main__':
    # Create an instance of the PartitioningService class
    nodes = ["node1", "node2", "node3"]
    replicas = 3
    partitioning_service = PartitioningService(nodes, replicas)

    # Add a partition to the system
    partition_id = "partition1"
    partitioning_service.add_partition(partition_id)

    # Get the node that stores a partition based on its partitioning key
    node_id = partitioning_service.get_partition_node(partition_id)

    # Get the partitions stored on a specific node
    node_partitions = partitioning_service.get_node_partitions(node_id)

    # Get the number of partitions stored on a specific node
    node_partitions_count = partitioning_service.get_node_partitions_count(node_id)

    # Get the node with the least number of partitions stored on it
    least_loaded_node = partitioning_service.get_least_loaded_node()

    # Get the node that should store a partition based on a load balancing strategy that distributes the partitions evenly across the nodes
    node_id_balanced = partitioning_service.get_partition_node_balanced(partition_id)
