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
