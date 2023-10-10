class NodeDiscovery:
    def __init__(self):
        self.nodes = set()

    def add_node(self, node_id):
        self.nodes.add(node_id)

    def remove_node(self, node_id):
        self.nodes.discard(node_id)
