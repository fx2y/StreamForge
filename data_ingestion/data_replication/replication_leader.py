import zlib
from multiprocessing import Pool, Manager


class ReplicationLeader:
    def __init__(self, replicas):
        """
        Initialize the replication leader with a list of replicas.
        """
        self.replicas = replicas
        self.batch_size = 100  # Set batch size to 100 for example purposes
        self.sequence_number = 0
        self.data_buffer = Manager().list()
        self.acknowledgements = Manager().dict()

    def receive_data(self, data):
        """
        Receive data from the data source adapters and send it to the other replicas.
        """
        compressed_data = zlib.compress(data.encode())
        batch = []
        for record in compressed_data:
            batch.append((self.sequence_number, record))
            self.sequence_number += 1
            if len(batch) == self.batch_size:
                self.data_buffer.append(batch)
                batch = []
        if batch:
            self.data_buffer.append(batch)
        self._send_data()

    def _send_data(self):
        """
        Send data to the other replicas.
        """
        while self.data_buffer:
            batch = self.data_buffer.pop(0)
            with Manager() as manager:
                shared_batch = manager.list(batch)
                with Pool(len(self.replicas)) as p:
                    p.map(self._send_batch, [(replica, shared_batch) for replica in self.replicas if replica != self])
            self._wait_for_acknowledgements(batch)

    def _send_batch(self, replica, shared_batch):
        """
        Send a batch of data to a replica.
        """
        batch = list(shared_batch)
        replica.receive_batch(batch)

    def _wait_for_acknowledgements(self, batch):
        """
        Wait for acknowledgements from the followers for a batch of data.
        """
        for sequence_number, _ in batch:
            while True:
                if all(acknowledgements.get(sequence_number, False) for acknowledgements in
                       self.acknowledgements.values()):
                    break

    def receive_acknowledgement(self, replica_id, sequence_number):
        """
        Receive an acknowledgement from a follower for a sequence number.
        """
        self.acknowledgements[replica_id][sequence_number] = True

    def receive_batch(self, batch):
        """
        Receive a batch of data from a follower and store it in the local log.
        """
        for sequence_number, record in batch:
            decompressed_data = zlib.decompress(record)
            # Store decompressed data in local log
            pass
        self._send_acknowledgements(batch)

    def _send_acknowledgements(self, batch):
        """
        Send acknowledgements to the followers for a batch of data.
        """
        for replica in self.replicas:
            if replica != self:
                replica.receive_acknowledgement(self.replicas.index(self), batch[-1][0])
