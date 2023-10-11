import time


class ReplicationProtocol:
    def __init__(self, replicas):
        self.replicas = replicas
        self.leader = None
        self.batch_size = 100  # Set batch size to 100 for example purposes

    def elect_leader(self):
        self.leader = self.replicas[0]  # Select first replica as leader for simplicity
        return self.leader

    def send_heartbeats(self):
        while True:
            for replica in self.replicas:
                if replica != self.leader:
                    replica.receive_heartbeat()
            time.sleep(1)

    def send_data(self, data):
        batch = []
        for record in data:
            batch.append(record)
            if len(batch) == self.batch_size:
                self.leader.send_batch(batch)
                batch = []
        if batch:
            self.leader.send_batch(batch)

    def receive_batch(self, batch):
        for replica in self.replicas:
            if replica != self.leader:
                replica.receive_batch(batch)

    def receive_data(self, data):
        # Store data in local log
        pass

    def detect_failures(self):
        for replica in self.replicas:
            if replica != self.leader:
                if not replica.is_alive():
                    self.replicas.remove(replica)

    def handle_network_partitions(self):
        for replica in self.replicas:
            if replica != self.leader:
                if not self.leader.can_communicate_with(replica):
                    self.replicas.remove(replica)

    def recover_from_failures(self):
        if not self.leader.is_alive():
            self.elect_leader()
        for replica in self.replicas:
            if replica != self.leader:
                if not replica.is_alive():
                    self.replicas.remove(replica)
