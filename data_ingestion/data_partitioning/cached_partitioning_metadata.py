import time

from data_ingestion.data_partitioning.partitioning_metadata import PartitioningMetadata


class CachedPartitioningMetadata:
    def __init__(self, db_file, cache_size=1000, cache_ttl=60):
        self.metadata = PartitioningMetadata(db_file)
        self.cache_size = cache_size
        self.cache_ttl = cache_ttl
        self.cache = dict()

    def add_replica(self, node_id, partition_id, replica_id):
        self.metadata.add_replica(node_id, partition_id, replica_id)
        self.evict_cache()

    def remove_replica(self, node_id, partition_id, replica_id):
        self.metadata.remove_replica(node_id, partition_id, replica_id)
        self.evict_cache()

    def get_partition_nodes(self, partition_id):
        if partition_id in self.cache:
            timestamp, nodes = self.cache[partition_id]
            if time.time() - timestamp <= self.cache_ttl:
                return nodes
            else:
                del self.cache[partition_id]
        nodes = self.metadata.get_partition_nodes(partition_id)
        self.cache[partition_id] = (time.time(), nodes)
        self.evict_cache()
        return nodes

    def get_node_partitions(self, node_id):
        if node_id in self.cache:
            timestamp, partitions = self.cache[node_id]
            if time.time() - timestamp <= self.cache_ttl:
                return partitions
            else:
                del self.cache[node_id]
        partitions = self.metadata.get_node_partitions(node_id)
        self.cache[node_id] = (time.time(), partitions)
        self.evict_cache()
        return partitions

    def get_partition_replicas(self, partition_id):
        if partition_id in self.cache:
            timestamp, replicas = self.cache[partition_id]
            if time.time() - timestamp <= self.cache_ttl:
                return replicas
            else:
                del self.cache[partition_id]
        replicas = self.metadata.get_partition_replicas(partition_id)
        self.cache[partition_id] = (time.time(), replicas)
        self.evict_cache()
        return replicas

    def evict_cache(self):
        if len(self.cache) > self.cache_size:
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][0])
            del self.cache[oldest_key]

    def refresh_cache(self):
        self.cache.clear()
