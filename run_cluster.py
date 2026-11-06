import time
import threading
from raft_cluster import RaftNode, RaftClient, OP_SET, OP_GET, OP_APPEND

def main():

    peers_node1 = {2: ("127.0.0.1", 8002), 3: ("127.0.0.1", 8003)}
    peers_node2 = {1: ("127.0.0.1", 8001), 3: ("127.0.0.1", 8003)}
    peers_node3 = {1: ("127.0.0.1", 8001), 2: ("127.0.0.1", 8002)}

    node1 = RaftNode(node_id=1, peers=peers_node1, wal_path="node1.wal")
    node2 = RaftNode(node_id=2, peers=peers_node2, wal_path="node2.wal")
    node3 = RaftNode(node_id=3, peers=peers_node3, wal_path="node3.wal")

    print("Starting Raft nodes...")
    node1.start("127.0.0.1", 8001)
    node2.start("127.0.0.1", 8002)
    node3.start("127.0.0.1", 8003)

    print("Waiting for leader election (3 seconds)...")
    time.sleep(3)

    client = RaftClient(cluster_nodes=[("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)])

    print("\nExecuting SET test...")
    res_set = client.execute(OP_SET, {"key": "test_key", "value": "Hello Raft"})
    print("SET Result:", res_set)

    print("\nExecuting GET test...")
    res_get = client.execute(OP_GET, {"key": "test_key"})
    print("GET Result:", res_get)

    print("\nCluster is running. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down cluster...")
        node1.running = False
        node2.running = False
        node3.running = False

if __name__ == "__main__":
    main()