import time
import os
import shutil
from raft_cluster import RaftNode, RaftClient, OP_SET, OP_GET

def cleanup():
    for f in os.listdir('.'):
        if f.endswith('.wal') or f.endswith('.meta') or f.endswith('.snap') or f.endswith('.tmp'):
            try:
                os.remove(f)
            except Exception:
                pass

def main():
    cleanup()

    peers_node1 = {2: ("127.0.0.1", 8002), 3: ("127.0.0.1", 8003)}
    peers_node2 = {1: ("127.0.0.1", 8001), 3: ("127.0.0.1", 8003)}
    peers_node3 = {1: ("127.0.0.1", 8001), 2: ("127.0.0.1", 8002)}

    node1 = RaftNode(node_id=1, peers=peers_node1, wal_path="node1.wal", snapshot_threshold=10)
    node2 = RaftNode(node_id=2, peers=peers_node2, wal_path="node2.wal", snapshot_threshold=10)
    node3 = RaftNode(node_id=3, peers=peers_node3, wal_path="node3.wal", snapshot_threshold=10)

    print("Starting 3 Raft cluster nodes...")
    node1.start("127.0.0.1", 8001)
    node2.start("127.0.0.1", 8002)
    node3.start("127.0.0.1", 8003)

    print("Waiting for cluster leader election...")
    time.sleep(3)

    client = RaftClient(cluster_nodes=[("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)])

    print("\nExecuting baseline write operations...")
    for i in range(15):
        client.execute(OP_SET, {"key": f"key_{i}", "value": f"value_{i}"})

    print("Executing baseline read operation...")
    res = client.execute(OP_GET, {"key": "key_5"})
    print("GET key_5 Result:", res)

    print("\nCluster initialized successfully.")

if __name__ == "__main__":
    main()
