import time
import threading
from raft_cluster import RaftClient, OP_SET, OP_GET, OP_APPEND

CLUSTER_NODES = [("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)]

def run_benchmark(num_ops=100, num_threads=4):
    print(f"Running benchmark with {num_ops} ops across {num_threads} threads...")
    start_time = time.time()
    
    success_count = 0
    lock = threading.Lock()

    def worker(thread_id):
        nonlocal success_count
        client = RaftClient(CLUSTER_NODES, client_id=f"bench_client_{thread_id}")
        for i in range(num_ops // num_threads):
            key = f"key_{thread_id}_{i}"
            val = f"val_{i}"
            res = client.execute(OP_SET, {"key": key, "value": val})
            if res.get("success"):
                with lock:
                    success_count += 1

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
    for t in threads: t.start()
    for t in threads: t.join()

    total_time = time.time() - start_time
    ops_per_sec = success_count / total_time if total_time > 0 else 0
    print(f"Benchmark completed: {success_count}/{num_ops} ops succeeded in {total_time:.2f}s ({ops_per_sec:.2f} ops/sec)")

if __name__ == "__main__":
    time.sleep(3)
    run_benchmark(num_ops=60, num_threads=3)
