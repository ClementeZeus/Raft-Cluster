# Deep-Dive Technical Blueprint & Line-by-Line Guide: Distributed Consensus Key-Value Store (Raft Engine)

This document provides a line-by-line breakdown and architectural explanation of the custom Python Raft Distributed Key-Value Store implementation (`raft_cluster.py`).

---

## Architecture Overview

The system is split into four primary architectural tiers:
1. **Binary Framing & Wire Protocol**: Custom binary protocol using struct-packed headers to handle TCP streaming and sticky packet fragmentation.
2. **Write-Ahead Logging (WAL)**: Synchronous filesystem persistence (`fsync`) to survive unexpected crash loops.
3. **Raft State Machine**: Leader election, randomized timers, heartbeat routines, term tracking, and $N/2 + 1$ majority quorum replication.
4. **Client Routing Engine**: Dynamic redirection to the cluster leader with fallback address probing.

---

## Comprehensive Line-by-Line Breakdown

---

### TIER 1: Imports & Binary Header Setup

```python
import socket
import struct
import json
import os
import time
import random
import threading
```
* **`import socket`**: Used for raw TCP network communications between nodes and clients.
* **`import struct`**: Used for binary serialization and deserialization of network packet headers.
* **`import json`**: Serializes dynamic payload dictionaries into byte payloads for wire transmission.
* **`import os`**: Interacts with the underlying filesystem, specifically for forcing disk flushes (`os.fsync`).
* **`import time`**: Provides timestamping functionality for election timeouts and heartbeat tracking.
* **`import random`**: Generates randomized election timeout windows ($1.5s - 3.0s$) to eliminate split-vote scenarios.
* **`import threading`**: Runs asynchronous task loops (accepting sockets, election timers, and heartbeat workers).

```python
MAGIC_BYTES = b"RAFT"
HEADER_FORMAT = "!4sB I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
```
* **`MAGIC_BYTES = b"RAFT"`**: 4-byte byte string used at the start of every binary payload frame to validate packet integrity and prevent stream corruption.
* **`HEADER_FORMAT = "!4sB I"`**: Struct layout specification string:
  * `!`: Network byte order (Big-Endian).
  * `4s`: 4-byte string array (`MAGIC_BYTES`).
  * `B`: 1-byte unsigned char (Opcode number).
  * `I`: 4-byte unsigned integer (Length of the variable JSON payload).
* **`HEADER_SIZE = struct.calcsize(HEADER_FORMAT)`**: Pre-calculated byte size of the binary header layout (exactly 9 bytes).

```python
OP_SET = 1
OP_GET = 2
OP_DELETE = 3
OP_APPEND = 4
OP_RAFT_VOTE = 5
OP_RAFT_VOTE_RESP = 6
OP_RAFT_APPEND_ENTRIES = 7
OP_RAFT_APPEND_RESP = 8
OP_RESPONSE = 9
```
* **`OP_SET` through `OP_RESPONSE`**: Command opcodes mapping network requests to state operations or internal consensus RPCs.

---

### TIER 2: Network Serialization Functions

```python
def send_message(sock, opcode: int, payload: dict):
    data = json.dumps(payload).encode('utf-8')
    header = struct.pack(HEADER_FORMAT, MAGIC_BYTES, opcode, len(data))
    sock.sendall(header + data)
```
* **`def send_message(...)`**: Encapsulates data into a binary frame and transmits it down an open socket.
* **`data = json.dumps(payload).encode('utf-8')`**: Converts the dictionary object into a UTF-8 encoded byte array.
* **`header = struct.pack(...)`**: Packs `MAGIC_BYTES`, opcode, and payload byte length into the 9-byte header frame.
* **`sock.sendall(header + data)`**: Ensures the entire binary header and body are pushed to the network layer without partial buffer writes.

```python
def recv_exact(sock, n_bytes: int):
    buf = bytearray()
    while len(buf) < n_bytes:
        chunk = sock.recv(n_bytes - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)
```
* **`def recv_exact(...)`**: Custom stream parser built to resolve the **TCP sticky packet and packet fragmentation problem**.
* **`buf = bytearray()`**: Allocates an expandable memory buffer for accumulated stream fragments.
* **`while len(buf) < n_bytes:`**: Loops until precisely `n_bytes` have been read off the wire socket.
* **`chunk = sock.recv(...)`**: Reads available data chunks up to the remaining missing byte count.
* **`if not chunk: return None`**: Returns `None` if the remote connection breaks or disconnects.
* **`buf.extend(chunk)`**: Appends raw socket chunks into the stream buffer.
* **`return bytes(buf)`**: Returns the exact binary slice.

```python
def recv_message(sock):
    header_bytes = recv_exact(sock, HEADER_SIZE)
    if not header_bytes:
        return None, None
    magic, opcode, length = struct.unpack(HEADER_FORMAT, header_bytes)
    if magic != MAGIC_BYTES:
        raise ValueError(f"Invalid magic bytes: {magic}")
    payload_bytes = recv_exact(sock, length)
    if payload_bytes is None:
        raise ConnectionError("Truncated payload read")
    payload = json.loads(payload_bytes.decode('utf-8'))
    return opcode, payload
```
* **`def recv_message(...)`**: High-level binary packet reader.
* **`header_bytes = recv_exact(sock, HEADER_SIZE)`**: Reads exactly 9 header bytes from the stream buffer.
* **`magic, opcode, length = struct.unpack(...)`**: Decodes binary values according to the network endianness layout.
* **`if magic != MAGIC_BYTES:`**: Validates payload protocol compatibility; fails fast on invalid connections.
* **`payload_bytes = recv_exact(sock, length)`**: Uses the extracted `length` field from the header to pull the body payload off the wire.
* **`payload = json.loads(...)`**: Deserializes raw UTF-8 JSON back into Python native primitives.

---

### TIER 3: Durable Write-Ahead Log (WAL)

```python
class WAL:
    def __init__(self, filename: str):
        self.filename = filename
        self.file = open(filename, "a+b", buffering=0)
```
* **`class WAL`**: Manages durable append-only operations on disk.
* **`open(..., "a+b", buffering=0)`**: Opens file handles in unbuffered raw binary append mode for immediate filesystem access.

```python
    def append_log(self, term: int, index: int, command: dict):
        entry = {"term": term, "index": index, "command": command}
        line = json.dumps(entry).encode('utf-8') + b"\n"
        self.file.write(line)
        os.fsync(self.file.fileno())
```
* **`append_log(...)`**: Appends mutation entries directly to disk.
* **`line = json.dumps(...) + b"\n"`**: Serializes the log entry as a line-delimited string record.
* **`self.file.write(line)`**: Pushes encoded byte strings to OS buffer pages.
* **`os.fsync(self.file.fileno())`**: **Critical Durability Line**. Forces the operating system kernel to flush write buffer caches directly to physical disk sectors before returning.

```python
    def read_all(self):
        entries = []
        if not os.path.exists(self.filename):
            return entries
        with open(self.filename, "rb") as f:
            for line in f:
                if line.strip():
                    entries.append(json.loads(line.decode('utf-8')))
        return entries
```
* **`read_all()`**: Reads the entire WAL upon system restart to recover memory state up to the last committed transaction.

---

### TIER 4: Consensus Engine Core (`RaftNode`)

```python
STATE_FOLLOWER = "FOLLOWER"
STATE_CANDIDATE = "CANDIDATE"
STATE_LEADER = "LEADER"

class RaftNode:
    def __init__(self, node_id: int, peers: dict, wal_path: str):
        self.node_id = node_id
        self.peers = peers
        self.wal = WAL(wal_path)

        self.current_term = 0
        self.voted_for = None
        self.log = []

        self.commit_index = 0
        self.last_applied = 0

        self.next_index = {}
        self.match_index = {}

        self.state = STATE_FOLLOWER
        self.leader_id = None
        self.kv_store = {}

        self.lock = threading.Lock()
        self.last_heartbeat_time = time.time()
        self.running = True

        self._recover_from_wal()
```
* **`STATE_*`**: Constants representing Raft node roles.
* **`self.current_term = 0`**: Highest election term number seen by the node (monotonically increasing).
* **`self.voted_for = None`**: Candidate node ID that received this node's vote in the current term.
* **`self.log = []`**: In-memory sequence of committed and uncommitted log entries.
* **`self.commit_index`**: Index of the highest log entry known to be committed cluster-wide.
* **`self.last_applied`**: Index of the highest log entry applied to the local key-value store.
* **`self.lock = threading.Lock()`**: Protects internal node state from race conditions during concurrent RPC execution.
* **`self._recover_from_wal()`**: Replays historical log entries from disk during initialization.

```python
    def apply_command(self, cmd: dict):
        op = cmd.get("op")
        key = cmd.get("key")
        val = cmd.get("value")
        if op == "SET":
            self.kv_store[key] = val
        elif op == "DELETE":
            self.kv_store.pop(key, None)
        elif op == "APPEND":
            self.kv_store[key] = self.kv_store.get(key, "") + val
```
* **`apply_command(...)`**: Mutates the underlying key-value memory state machine deterministically once consensus is reached.

```python
    def start(self, host: str, port: int):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((host, port))
        server_sock.listen(128)

        threading.Thread(target=self._accept_loop, args=(server_sock,), daemon=True).start()
        threading.Thread(target=self._election_timer_loop, daemon=True).start()
```
* **`start(...)`**: Binds the node listener socket and starts background threads for incoming network connections and the election timer loop.

```python
    def _handle_client_cmd(self, opcode: int, payload: dict) -> dict:
        if opcode == OP_GET:
            key = payload.get("key")
            return {"success": True, "value": self.kv_store.get(key), "leader_id": self.leader_id}

        if self.state != STATE_LEADER:
            leader_addr = self.peers.get(self.leader_id) if self.leader_id is not None else None
            return {"success": False, "error": "Not leader", "leader_id": self.leader_id, "leader_addr": leader_addr}
```
* Reads (`OP_GET`) are served directly from memory. Write operations (`SET`, `DELETE`, `APPEND`) sent to followers return a redirection payload pointing to the cluster leader.

```python
        new_index = len(self.log) + 1
        entry = {"term": self.current_term, "index": new_index, "command": cmd}
        self.log.append(entry)
        self.wal.append_log(self.current_term, new_index, cmd)

        acks = 1
        total_nodes = len(self.peers) + 1
        needed_acks = (total_nodes // 2) + 1
```
* **Leader Phase**: Appends new entry locally, writes to the local WAL, and calculates the required quorum ($N/2 + 1$).

```python
        for t in threads:
            t.join(timeout=1.0)

        if acks >= needed_acks:
            self.commit_index = new_index
            self.apply_command(cmd)
            self.last_applied = new_index
            return {"success": True, "index": new_index}
```
* **Two-Phase Commit**: Collects replica acknowledgments. If a majority quorum is reached, the entry is committed, applied to the local state machine, and a success response is returned to the client.

```python
    def _start_election(self):
        self.state = STATE_CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat_time = time.time()
        votes = 1
        ...
        if votes >= needed_votes and self.state == STATE_CANDIDATE:
            self.state = STATE_LEADER
            self.leader_id = self.node_id
            self._send_heartbeats()
```
* **Leader Election**: Candidate increments term, votes for itself, and broadcasts RequestVote RPCs across all peers concurrently. If it collects a majority of votes, it transitions to `LEADER` and begins broadcasting heartbeats.

---

### TIER 5: Transparent CLI Client

```python
class RaftClient:
    def __init__(self, cluster_nodes: list):
        self.cluster_nodes = cluster_nodes
        self.leader_addr = None

    def execute(self, opcode: int, payload: dict):
        targets = [self.leader_addr] + self.cluster_nodes if self.leader_addr else list(self.cluster_nodes)
        for addr in targets:
            if not addr:
                continue
            try:
                s = self._connect(addr[0], addr[1])
                send_message(s, opcode, payload)
                _, resp = recv_message(s)
                s.close()

                if resp.get("success"):
                    self.leader_addr = addr
                    return resp
                elif resp.get("leader_addr"):
                    self.leader_addr = tuple(resp["leader_addr"])
                    return self.execute(opcode, payload)
            except Exception:
                continue

        return {"success": False, "error": "Could not connect to cluster leader"}
```
* **`RaftClient`**: Manages transparent cluster interaction.
* **Auto-Routing**: Caches the active leader address. If a request hits a follower node, the client follows the returned redirection payload automatically.

---

## Quickstart & Execution Guide

Save both `raft_cluster.py` and the runner code below in the same directory.

```python
# runner.py
import time
from raft_cluster import RaftNode, RaftClient, OP_SET, OP_GET, OP_APPEND

peers_n1 = {2: ("127.0.0.1", 8002), 3: ("127.0.0.1", 8003)}
peers_n2 = {1: ("127.0.0.1", 8001), 3: ("127.0.0.1", 8003)}
peers_n3 = {1: ("127.0.0.1", 8001), 2: ("127.0.0.1", 8002)}

n1 = RaftNode(1, peers_n1, "n1.wal")
n2 = RaftNode(2, peers_n2, "n2.wal")
n3 = RaftNode(3, peers_n3, "n3.wal")

n1.start("127.0.0.1", 8001)
n2.start("127.0.0.1", 8002)
n3.start("127.0.0.1", 8003)

time.sleep(3)  # Wait for leader election

client = RaftClient([("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)])

print(client.execute(OP_SET, {"key": "alpha", "value": "100"}))
print(client.execute(OP_GET, {"key": "alpha"}))
```

Run the cluster with:
```bash
python runner.py
```