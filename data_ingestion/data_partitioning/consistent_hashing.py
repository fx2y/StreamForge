import xxhash


class ConsistentHashing:
    def __init__(self, nodes, replicas=3):
        self.replicas = replicas
        self.ring = dict()
        for node in nodes:
            for i in range(replicas):
                key = self.hash(f"{node}:{i}")
                self.ring[key] = node

    def hash(self, key):
        return xxhash.xxh64(key).intdigest()

    def get_node(self, key):
        if not self.ring:
            return None
        hash_key = self.hash(key)
        for node in sorted(self.ring.keys()):
            if hash_key <= node:
                return self.ring[node]
        return self.ring[min(self.ring.keys())]


if __name__ == '__main__':
    nodes = ['node1', 'node2', 'node3']
    consistent_hashing = ConsistentHashing(nodes)

    partitioning_keys = ['key1', 'key2', 'key3', 'key4', 'key5']
    for key in partitioning_keys:
        node = consistent_hashing.get_node(key)
        print(f"Partitioning key '{key}' maps to node '{node}'")
