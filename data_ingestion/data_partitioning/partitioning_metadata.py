class PartitioningMetadata:
    def __init__(self):
        self.metadata = dict()

    def add_partition(self, partition_id, node_id, replicas):
        if node_id not in self.metadata:
            self.metadata[node_id] = dict()
        self.metadata[node_id][partition_id] = replicas

    def remove_partition(self, partition_id, node_id):
        if node_id in self.metadata and partition_id in self.metadata[node_id]:
            del self.metadata[node_id][partition_id]

    def get_node_partitions(self, node_id):
        if node_id in self.metadata:
            return self.metadata[node_id]
        return dict()
