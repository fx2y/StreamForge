import unittest
from multiprocessing import Manager
from unittest.mock import Mock, patch

from replication_leader import ReplicationLeader


class TestReplicationLeader(unittest.TestCase):
    def setUp(self):
        self.replicas = [Mock(), Mock(), Mock()]
        self.leader = ReplicationLeader(self.replicas)

    def test_receive_data(self):
        data = 'test data'
        self.leader.receive_data(data)
        self.assertEqual(len(self.leader.data_buffer), 1)

    def test_send_data(self):
        self.leader.data_buffer = Manager().list([[(0, b'test data')]])
        self.leader._send_data()
        for replica in self.replicas:
            if replica != self.leader:
                replica.receive_batch.assert_called_once()

    def test_receive_acknowledgement(self):
        replica_id = 1
        sequence_number = 0
        self.leader.receive_acknowledgement(replica_id, sequence_number)
        self.assertTrue(self.leader.acknowledgements[replica_id][sequence_number])

    def test_receive_batch(self):
        batch = [(0, b'test data')]
        self.leader.receive_batch(batch)
        for replica in self.replicas:
            if replica != self.leader:
                replica.receive_acknowledgement.assert_called_once()

    def test_handle_failures(self):
        self.replicas[1].is_alive.return_value = False
        self.leader.handle_failures()
        self.assertNotIn(self.replicas[1], self.leader.replicas)

    def test_handle_network_partitions(self):
        replica = Mock()
        self.leader.replicas.append(replica)
        self.leader._can_communicate = Mock(return_value=False)
        self.leader.handle_network_partitions()
        self.assertNotIn(replica, self.leader.replicas)

    def test_can_communicate(self):
        replica = Mock()
        with patch.object(replica, 'ping', return_value=True):
            self.assertTrue(self.leader._can_communicate(replica))

    def test_ping(self):
        sequence_number = 0
        self.assertTrue(self.leader.ping(sequence_number))

    def test_backpressure(self):
        self.leader.data_buffer = Manager().list([[(0, b'test data')]])
        self.assertTrue(self.leader.backpressure())

    def test_recover(self):
        self.leader._get_latest_sequence_number = Mock(return_value=0)
        self.leader._get_data_buffer = Mock(return_value=Manager().list())
        self.leader.recover()
        self.assertEqual(self.leader.sequence_number, 0)
        self.assertEqual(len(self.leader.data_buffer), 0)

    def test_set_batch_size(self):
        batch_size = 50
        self.leader.set_batch_size(batch_size)
        self.assertEqual(self.leader.batch_size, batch_size)

    def test_batch_data(self):
        data = 'test data'
        self.leader.batch_data(data)
        self.assertEqual(len(self.leader.data_buffer), 1)


if __name__ == '__main__':
    unittest.main()
