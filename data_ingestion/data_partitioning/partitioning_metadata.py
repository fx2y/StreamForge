import sqlite3


class PartitioningMetadata:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS partition_replicas (
                node_id TEXT,
                partition_id TEXT,
                replica_id TEXT,
                PRIMARY KEY (node_id, partition_id, replica_id)
            )
        ''')
        self.conn.commit()

    def add_replica(self, node_id, partition_id, replica_id):
        self.cursor.execute('''
            INSERT INTO partition_replicas (node_id, partition_id, replica_id)
            VALUES (?, ?, ?)
        ''', (node_id, partition_id, replica_id))
        self.conn.commit()

    def remove_replica(self, node_id, partition_id, replica_id):
        self.cursor.execute('''
            DELETE FROM partition_replicas
            WHERE node_id = ? AND partition_id = ? AND replica_id = ?
        ''', (node_id, partition_id, replica_id))
        self.conn.commit()

    def get_partition_nodes(self, partition_id):
        self.cursor.execute('''
            SELECT DISTINCT node_id
            FROM partition_replicas
            WHERE partition_id = ?
        ''', (partition_id,))
        return [row[0] for row in self.cursor.fetchall()]

    def get_node_partitions(self, node_id):
        self.cursor.execute('''
            SELECT DISTINCT partition_id
            FROM partition_replicas
            WHERE node_id = ?
        ''', (node_id,))
        return [row[0] for row in self.cursor.fetchall()]

    def get_partition_replicas(self, partition_id):
        self.cursor.execute('''
            SELECT replica_id
            FROM partition_replicas
            WHERE partition_id = ?
        ''', (partition_id,))
        return [row[0] for row in self.cursor.fetchall()]

    def add_partition(self, partition_id, node_id, replicas):
        for replica_id in replicas:
            self.add_replica(node_id, partition_id, replica_id)

    def remove_partition(self, partition_id, node_id):
        self.cursor.execute('''
            DELETE FROM partition_replicas
            WHERE node_id = ? AND partition_id = ?
        ''', (node_id, partition_id))
        self.conn.commit()
