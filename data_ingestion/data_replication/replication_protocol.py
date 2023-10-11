import time


class ReplicationProtocol:
    def __init__(self, replicas):
        self.replicas = replicas
        self.leader = None

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
        for replica in self.replicas:
            if replica != self.leader:
                replica.receive_data(data)

    def receive_data(self, data):
        # Store data in local log
        pass
