class NodeFailureDetector:
    def __init__(self, partitioning_service):
        self.partitioning_service = partitioning_service

    def detect_node_failure(self, node_id):
        partitions = self.partitioning_service.partitioning_metadata.get_node_partitions(node_id)
        for partition_id in partitions:
            replicas = self.partitioning_service.partitioning_metadata.get_partition_replicas(partition_id)
            if node_id in replicas:
                replicas.remove(node_id)
                if not replicas:
                    # All replicas failed, recover from another node
                    self.recover_partition(partition_id)

    def recover_partition(self, partition_id):
        nodes = self.partitioning_service.node_discovery.get_nodes()
        for node_id in nodes:
            if node_id not in self.partitioning_service.partitioning_metadata.get_partition_nodes(partition_id):
                # Recover partition on node
                break
