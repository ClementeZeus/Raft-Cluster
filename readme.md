# Distributed Raft Key-Value Store: Technical Architecture & Line-by-Line Codebase Guide

---

## Executive Summary

This document provides comprehensive technical documentation for a fault-tolerant, fault-recovering distributed **Key-Value Store** built upon the **Raft Consensus Protocol**. The implementation encompasses leader election, log replication, Write-Ahead Logging (WAL) persistence, state machine snapshots, client session deduplication (linearizable semantics), and ReadIndex read-optimizations.

---

## Architectural Overview

```
                        +----------------------+
                        |     RaftClient       |
                        +----------+-----------+
                                   |
                     RPC Operations (SET, GET, etc.)
                                   |
      +----------------------------+----------------------------+
      |                            |                            |
      v                            v                            v
+-----------+                +-----------+                +-----------+
| Node 1    |  AppendEntries | Node 2    |  AppendEntries | Node 3    |
| (Leader)  |<-------------->| (Follower)|<-------------->| (Follower)|
+-----+-----+  RequestVote   +-----+-----+  RequestVote   +-----+-----+
      |                            |                            |
  +---+---+                    +---+---+                    +---+---+
  | WAL   |                    | WAL   |                    | WAL   |
  | Snap  |                    | Snap  |                    | Snap  |
  +-------+                    +-------+                    +-------+
```

### Protocol Mechanics

1. **Leader Election**:
   * Nodes operate as **Follower**, **Candidate**, or **Leader**.
   * Randomized heartbeat timeouts ($1.5\text{s} - 3.0\text{s}$) trigger Candidate election cycles.
   * Quorum voting ($\lfloor N/2 \rfloor + 1$) guarantees at most one leader per term.

2. **Log Replication & State Machine**:
   * Write commands are logged locally in a WAL and replicated to followers.
   * Entries are committed once stored on a majority of nodes and applied sequentially to the state machine (`kv_store`).

3. **Snapshointing & Log Compaction**:
   * Logs exceeding `snapshot_threshold` trigger compaction.
   * State machine snapshots and session maps are stored atomically (`.snap`), truncating the WAL.

4. **Linearizable Reads (ReadIndex Protocol)**:
   * Read requests (`OP_GET`) query the leader.
   * The leader verifies ongoing leadership via heartbeat quorum before serving state machine reads, avoiding stale reads without writing dummy log entries.

---

## File Index

1. **`kv_engine.py`**: Isolated, thread-safe state machine and session tracker.
2. **`raft_cluster.py`**: Core Raft node implementation, custom binary RPC protocol, WAL engine, snapshot manager, and client library.
3. **`cli_shell.py`**: Real-time terminal control panel and cluster dashboard.
4. **`run_cluster.py`**: Integration testing, cluster bootstrap, and snapshot verification engine.
5. **`benchmark.py`**: Multi-threaded concurrency benchmark tool measuring operations per second (OPS).

---

## Line-by-Line File Explanations

---

### File 1: `kv_engine.py`

#### Purpose
Defines the memory-backed `KeyValueEngine` state machine. Supports key mutations (`SET`, `DELETE`, `APPEND`), queries (`GET`), and client session deduplication to ensure exactly-once semantics.

#### Line-by-Line Breakdown

```python
 1: import threading
```
* **Line 1**: Imports Python's `threading` module to enable thread synchronization locks.

```python
 3: class KeyValueEngine:
 4:     def __init__(self):
 5:         self.store = {}
 6:         self.sessions = {}
 7:         self.lock = threading.Lock()
```
* **Line 3**: Declares the `KeyValueEngine` class representing the storage engine.
* **Line 4-5**: `self.store`: Dictionary containing key-value pairs.
* **Line 6**: `self.sessions`: Maps `client_id` to `{"seq": sequence_number, "response": cached_response}` to prevent duplicate executions.
* **Line 7**: `self.lock`: Primitive mutex lock ensuring atomicity across concurrent mutations.

```python
 9:     def execute(self, op: str, key: str, value: str = None, client_id: str = None, seq: int = None) -> dict:
10:         with self.lock:
11:             if client_id and seq:
12:                 session = self.sessions.get(client_id)
13:                 if session and session["seq"] >= seq:
14:                     return session["response"]
```
* **Line 9**: Method signature for state mutations and queries.
* **Line 10**: Acquires `self.lock` context manager.
* **Lines 11-14**: **Deduplication Check**: If the operation includes `client_id` and sequence `seq`, checks if this sequence was already processed. If processed, returns the cached result immediately.

```python
16:             res = {"success": True}
17:             if op == "SET":
18:                 self.store[key] = value
19:             elif op == "GET":
20:                 res["value"] = self.store.get(key)
21:             elif op == "DELETE":
22:                 self.store.pop(key, None)
23:             elif op == "APPEND":
24:                 self.store[key] = self.store.get(key, "") + value
```
* **Lines 16-24**: Executes state mutations based on operation opcode:
  * `"SET"`: Assigns `value` to `key`.
  * `"GET"`: Fetches value associated with `key`.
  * `"DELETE"`: Removes `key` if present.
  * `"APPEND"`: Appends string `value` to existing string at `key`.

```python
26:             if client_id and seq:
27:                 self.sessions[client_id] = {"seq": seq, "response": res}
28: 
29:             return res
```
* **Lines 26-27**: Records client `seq` and result payload `res` in `self.sessions`.
* **Line 29**: Returns execution result.

```python
31:     def get_snapshot(self) -> dict:
32:         with self.lock:
33:             return {"store": dict(self.store), "sessions": dict(self.sessions)}
```
* **Lines 31-33**: Thread-safe deep snapshot extraction of storage dictionary and session tables.

```python
35:     def restore_snapshot(self, snapshot_data: dict):
36:         with self.lock:
37:             self.store = dict(snapshot_data.get("store", {}))
38:             self.sessions = dict(snapshot_data.get("sessions", {}))
```
* **Lines 35-38**: Restores internal state and session caches from a provided snapshot object under lock protection.

---

### File 2: `raft_cluster.py`

#### Purpose
The core consensus layer. Implements binary socket protocols, connection pooling, write-ahead logging (WAL), candidate voting, leader replication, snapshot installation, and client interaction.

#### Line-by-Line Breakdown

```python
 1: # raft_cluster.py
 2: import socket
 3: import struct
 4: import json
 5: import os
 6: import time
 7: import random
 8: import threading
```
* **Lines 1-8**: Standard system imports: low-level networking (`socket`), binary encoding (`struct`), serialization (`json`), OS primitives (`os`), time (`time`), pseudo-randomness (`random`), and multithreading (`threading`).

```python
10: MAGIC_BYTES = b"RAFT"
11: HEADER_FORMAT = "!4sB I"
12: HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
```
* **Line 10**: Magic bytes header (`RAFT`) for socket wire-protocol packet identification.
* **Line 11**: Binary struct header format: `!` (Big-Endian), `4s` (4-byte string), `B` (1-byte unsigned char opcode), `I` (4-byte unsigned int payload length).
* **Line 12**: Pre-calculates exact header byte length (9 bytes).

```python
14: OP_SET = 1
15: OP_GET = 2
16: OP_DELETE = 3
17: OP_APPEND = 4
18: OP_RAFT_VOTE = 5
19: OP_RAFT_VOTE_RESP = 6
20: OP_RAFT_APPEND_ENTRIES = 7
21: OP_RAFT_APPEND_RESP = 8
22: OP_RAFT_INSTALL_SNAPSHOT = 9
23: OP_RAFT_INSTALL_SNAPSHOT_RESP = 10
24: OP_RESPONSE = 11
```
* **Lines 14-24**: Opcode constants defining RPC wire commands for state mutations, client operations, elections, log replication, snapshot transfer, and responses.

```python
26: def send_message(sock, opcode: int, payload: dict):
27:     data = json.dumps(payload).encode('utf-8')
28:     header = struct.pack(HEADER_FORMAT, MAGIC_BYTES, opcode, len(data))
29:     sock.sendall(header + data)
```
* **Lines 26-29**: Serializes JSON dictionary payload, packs standard binary header, and guarantees complete socket transmission via `sendall`.

```python
31: def recv_exact(sock, n_bytes: int):
32:     buf = bytearray()
33:     while len(buf) < n_bytes:
34:         chunk = sock.recv(n_bytes - len(buf))
35:         if not chunk:
36:             return None
37:         buf.extend(chunk)
38:     return bytes(buf)
```
* **Lines 31-38**: Helper routine loop that reads from a non-buffered TCP socket until exactly `n_bytes` are accumulated. Handles TCP packet fragmentation.

```python
40: def recv_message(sock):
41:     header_bytes = recv_exact(sock, HEADER_SIZE)
42:     if not header_bytes:
43:         return None, None
44:     magic, opcode, length = struct.unpack(HEADER_FORMAT, header_bytes)
45:     if magic != MAGIC_BYTES:
46:         raise ValueError(f"Invalid magic bytes: {magic}")
47:     payload_bytes = recv_exact(sock, length)
48:     if payload_bytes is None:
49:         raise ConnectionError("Truncated payload read")
50:     payload = json.loads(payload_bytes.decode('utf-8'))
51:     return opcode, payload
```
* **Lines 40-51**: Reads wire message header, verifies `MAGIC_BYTES`, extracts payload length, fetches exact payload content, and deserializes JSON payload.

```python
53: class ConnectionPool:
54:     def __init__(self):
55:         self.conns = {}
56:         self.lock = threading.Lock()
```
* **Lines 53-56**: Reusable connection pool maintaining persistent TCP socket handles to peer nodes to eliminate connection overhead during frequent replication heartbeats.

```python
58:     def get_connection(self, host: str, port: int):
59:         key = (host, port)
60:         with self.lock:
61:             sock = self.conns.get(key)
62:             if sock:
63:                 return sock
64:             try:
65:                 sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
66:                 sock.settimeout(1.0)
67:                 sock.connect((host, port))
68:                 self.conns[key] = sock
69:                 return sock
70:             except Exception:
71:                 return None
```
* **Lines 58-71**: Returns cached connection if active; otherwise initializes a TCP connection with a 1.0s timeout and updates pool dictionary under lock.

```python
73:     def close_connection(self, host: str, port: int):
74:         key = (host, port)
75:         with self.lock:
76:             sock = self.conns.pop(key, None)
77:             if sock:
78:                 try:
79:                     sock.close()
80:                 except Exception:
81:                     pass
```
* **Lines 73-81**: Safely pops socket reference from dictionary and closes underlying file descriptor during network errors or node isolation.

```python
83: class WAL:
84:     def __init__(self, filename: str):
85:         self.filename = filename
86:         self.file = open(filename, "a+b", buffering=0)
```
* **Lines 83-86**: Write-Ahead Log class using unbuffered binary append mode (`a+b`, `buffering=0`).

```python
88:     def append_log(self, term: int, index: int, command: dict):
89:         entry = {"term": term, "index": index, "command": command}
90:         line = json.dumps(entry).encode('utf-8') + b"\n"
91:         self.file.write(line)
92:         os.fsync(self.file.fileno())
```
* **Lines 88-92**: Appends entry line to disk and calls `os.fsync` to flush OS file system buffers directly to underlying physical storage (durability guarantee).

```python
94:     def read_all(self):
95:         entries = []
96:         if not os.path.exists(self.filename):
97:             return entries
98:         with open(self.filename, "rb") as f:
99:             for line in f:
100:                 if line.strip():
101:                     entries.append(json.loads(line.decode('utf-8')))
102:         return entries
```
* **Lines 94-102**: Replays entire WAL line-by-line during node boot/recovery and returns array of dictionary log entries.

```python
104:     def truncate_and_write(self, entries):
105:         self.file.close()
106:         with open(self.filename, "wb") as f:
107:             for entry in entries:
108:                 f.write(json.dumps(entry).encode('utf-8') + b"\n")
109:             f.flush()
110:             os.fsync(f.fileno())
111:         self.file = open(self.filename, "a+b", buffering=0)
```
* **Lines 104-111**: Rewrites WAL file atomically on snapshot compaction or log divergence reconciliation. Re-opens append pointer following sync.

```python
113: STATE_FOLLOWER = "FOLLOWER"
114: STATE_CANDIDATE = "CANDIDATE"
115: STATE_LEADER = "LEADER"
```
* **Lines 113-115**: String enumeration constants representing finite states of Raft state machines.

```python
117: class RaftNode:
118:     def __init__(self, node_id: int, peers: dict, wal_path: str, host: str = "127.0.0.1", port: int = 8000, snapshot_threshold: int = 100):
119:         self.node_id = node_id
120:         self.host = host
121:         self.port = port
122:         self.peers = peers
123:         self.wal_path = wal_path
124:         self.meta_path = wal_path + ".meta"
125:         self.snapshot_path = wal_path + ".snap"
126:         self.wal = WAL(wal_path)
127:         self.snapshot_threshold = snapshot_threshold
128:         self.conn_pool = ConnectionPool()
```
* **Lines 117-128**: Initializes identity, network bindings, peer addresses, file paths (`.wal`, `.meta`, `.snap`), log infrastructure, compaction limit threshold, and connection pooling.

```python
130:         self.current_term = 0
131:         self.voted_for = None
132:         self.log = []
133:         self.last_included_index = 0
134:         self.last_included_term = 0
135:         self.commit_index = 0
136:         self.last_applied = 0
137:         self.next_index = {}
138:         self.match_index = {}
139:         self.state = STATE_FOLLOWER
140:         self.leader_id = None
141:         self.kv_store = {}
142:         self.client_sessions = {}
```
* **Lines 130-142**: Raft Volatile & Persistent State Variables:
  * Persistent: `current_term`, `voted_for`, `log`.
  * Snapshot Metadata: `last_included_index`, `last_included_term`.
  * Volatile: `commit_index`, `last_applied`.
  * Leader Volatile: `next_index` (next entry to send), `match_index` (highest index known replicated).
  * State Machine: `kv_store`, `client_sessions`.

```python
144:         self.lock = threading.Lock()
145:         self.last_heartbeat_time = time.time()
146:         self.running = False
147:         self.server_sock = None
148: 
149:         self._load_meta()
150:         self._load_snapshot()
151:         self._recover_from_wal()
```
* **Lines 144-151**: Sets process concurrency lock, heartbeat timestamp, and executes startup recovery steps: `_load_meta()`, `_load_snapshot()`, and `_recover_from_wal()`.

```python
153:     def _load_meta(self):
154:         if os.path.exists(self.meta_path):
155:             with open(self.meta_path, "r") as f:
156:                 data = json.load(f)
157:                 self.current_term = data.get("current_term", 0)
158:                 self.voted_for = data.get("voted_for", None)
```
* **Lines 153-158**: Restores metadata (`current_term`, `voted_for`) from `.meta` on node boot.

```python
160:     def _persist_meta(self):
161:         tmp_file = self.meta_path + ".tmp"
162:         with open(tmp_file, "w") as f:
163:             json.dump({"current_term": self.current_term, "voted_for": self.voted_for}, f)
164:             f.flush()
165:             os.fsync(f.fileno())
166:         os.replace(tmp_file, self.meta_path)
```
* **Lines 160-166**: Persists `current_term` and `voted_for` to a `.tmp` file using atomic replacement (`os.replace`) to avoid metadata corruption during unexpected crashes.

```python
168:     def _load_snapshot(self):
169:         if os.path.exists(self.snapshot_path):
170:             with open(self.snapshot_path, "r") as f:
171:                 data = json.load(f)
172:                 self.last_included_index = data["last_included_index"]
173:                 self.last_included_term = data["last_included_term"]
174:                 self.kv_store = data["kv_store"]
175:                 self.client_sessions = data.get("client_sessions", {})
176:                 self.commit_index = self.last_included_index
177:                 self.last_applied = self.last_included_index
```
* **Lines 168-177**: Restores compacted state snapshot (`.snap`) into memory and updates state machine pointers (`commit_index`, `last_applied`).

```python
179:     def _save_snapshot(self):
180:         data = {
181:             "last_included_index": self.last_included_index,
182:             "last_included_term": self.last_included_term,
183:             "kv_store": self.kv_store,
184:             "client_sessions": self.client_sessions,
185:         }
186:         tmp_file = self.snapshot_path + ".tmp"
187:         with open(tmp_file, "w") as f:
188:             json.dump(data, f)
189:             f.flush()
190:             os.fsync(f.fileno())
191:         os.replace(tmp_file, self.snapshot_path)
```
* **Lines 179-191**: Performs atomic write and flush of state snapshot metadata, key-value table, and session entries.

```python
193:     def _recover_from_wal(self):
194:         with self.lock:
195:             self.log = self.wal.read_all()
196:             for entry in self.log:
197:                 self.apply_command(entry["command"])
198:                 self.commit_index = max(self.commit_index, entry["index"])
199:                 self.last_applied = max(self.last_applied, entry["index"])
```
* **Lines 193-199**: Replays uncompacted log entries in sequence directly into `apply_command()` to reconstruct memory state following a restart.

```python
201:     def apply_command(self, cmd: dict):
202:         client_id = cmd.get("client_id")
203:         seq = cmd.get("seq")
204:         if client_id and seq:
205:             session = self.client_sessions.get(client_id)
206:             if session and session["seq"] >= seq:
207:                 return session["response"]
208: 
209:         op = cmd.get("op")
210:         key = cmd.get("key")
211:         val = cmd.get("value")
212:         res = {"success": True}
213: 
214:         if op == "SET":
215:             self.kv_store[key] = val
216:         elif op == "DELETE":
217:             self.kv_store.pop(key, None)
218:         elif op == "APPEND":
219:             self.kv_store[key] = self.kv_store.get(key, "") + val
220: 
221:         if client_id and seq:
222:             self.client_sessions[client_id] = {"seq": seq, "response": res}
223:         return res
```
* **Lines 201-223**: Commits commands directly to state machine memory (`self.kv_store`), enforcing session deduplication guarantees via `self.client_sessions`.

```python
225:     def start(self, host: str = None, port: int = None):
226:         with self.lock:
227:             if self.running:
228:                 return
229:             if host: self.host = host
230:             if port: self.port = port
231:             self.running = True
232:             self.last_heartbeat_time = time.time()
233: 
234:         self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
235:         self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
236:         self.server_sock.bind((self.host, self.port))
237:         self.server_sock.listen(128)
238: 
239:         threading.Thread(target=self._accept_loop, daemon=True).start()
240:         threading.Thread(target=self._election_timer_loop, daemon=True).start()
241:         threading.Thread(target=self._replication_loop, daemon=True).start()
```
* **Lines 225-241**: Binds TCP socket server (`SO_REUSEADDR`), starts listening, and spawns three background daemon threads: `_accept_loop`, `_election_timer_loop`, and `_replication_loop`.

```python
243:     def stop(self):
244:         with self.lock:
245:             self.running = False
246:         if self.server_sock:
247:             try:
248:                 self.server_sock.close()
249:             except Exception:
250:                 pass
```
* **Lines 243-250**: Stops server background loops by setting `self.running = False` and closing listener socket.

```python
252:     def _accept_loop(self):
253:         while self.running:
254:             try:
255:                 conn, addr = self.server_sock.accept()
256:                 threading.Thread(target=self._client_handler, args=(conn,), daemon=True).start()
257:             except Exception:
258:                 break
```
* **Lines 252-258**: Accepts inbound TCP connections and delegates handling to isolated worker threads.

```python
260:     def _client_handler(self, conn):
261:         with conn:
262:             while self.running:
263:                 try:
264:                     opcode, payload = recv_message(conn)
265:                     if opcode is None:
266:                         break
267:                     resp = self.handle_rpc(opcode, payload)
268:                     send_message(conn, OP_RESPONSE, resp)
269:                 except Exception:
270:                     break
```
* **Lines 260-270**: Reads messages over a persistent TCP socket session, processes them via `handle_rpc`, and returns binary encoded responses.

```python
272:     def handle_rpc(self, opcode: int, payload: dict) -> dict:
273:         with self.lock:
274:             if opcode in (OP_SET, OP_GET, OP_DELETE, OP_APPEND):
275:                 return self._handle_client_cmd(opcode, payload)
276:             elif opcode == OP_RAFT_VOTE:
277:                 return self._handle_request_vote(payload)
278:             elif opcode == OP_RAFT_APPEND_ENTRIES:
279:                 return self._handle_append_entries(payload)
280:             elif opcode == OP_RAFT_INSTALL_SNAPSHOT:
281:                 return self._handle_install_snapshot(payload)
282:             return {"success": False, "error": "Unknown opcode"}
```
* **Lines 272-282**: Internal RPC dispatcher routing client mutations, voting requests, append entries heartbeats, and snapshot installations under lock protection.

```python
284:     def _get_log_term(self, index: int) -> int:
285:         if index == 0:
286:             return 0
287:         if index == self.last_included_index:
288:             return self.last_included_term
289:         relative_idx = index - self.last_included_index - 1
290:         if 0 <= relative_idx < len(self.log):
291:             return self.log[relative_idx]["term"]
292:         return 0
```
* **Lines 284-292**: Helper method to fetch the term at a absolute log index, translating local offsets if log compaction has taken place.

```python
294:     def _verify_leadership(self) -> bool:
295:         if self.state != STATE_LEADER:
296:             return False
297:         term = self.current_term
298:         acks = 1
299:         needed = (len(self.peers) + 1) // 2 + 1
300:         ack_lock = threading.Lock()
301: 
302:         def check_peer(pid, addr):
303:             nonlocal acks
304:             sock = self.conn_pool.get_connection(addr[0], addr[1])
305:             if not sock:
306:                 return
307:             req = {
308:                 "term": term,
309:                 "leader_id": self.node_id,
310:                 "prev_log_index": self.last_included_index + len(self.log),
311:                 "prev_log_term": self._get_log_term(self.last_included_index + len(self.log)),
312:                 "entries": [],
313:                 "leader_commit": self.commit_index
314:             }
315:             try:
316:                 send_message(sock, OP_RAFT_APPEND_ENTRIES, req)
317:                 _, res = recv_message(sock)
318:                 if res and res.get("term") == term:
319:                     with ack_lock:
320:                         acks += 1
321:             except Exception:
322:                 self.conn_pool.close_connection(addr[0], addr[1])
323: 
324:         threads = [threading.Thread(target=check_peer, args=(pid, addr)) for pid, addr in self.peers.items()]
325:         for t in threads: t.start()
326:         for t in threads: t.join(timeout=0.2)
327:         return acks >= needed
```
* **Lines 294-327**: **ReadIndex Implementation**: Broadcasts an empty `AppendEntries` payload to confirm majority quorum reachability before returning read results. Guarantees linearizability without modifying the log.

```python
329:     def _handle_client_cmd(self, opcode: int, payload: dict) -> dict:
330:         if opcode == OP_GET:
331:             if self.state == STATE_LEADER:
332:                 if not self._verify_leadership():
333:                     return {"success": False, "error": "Leadership lost during ReadIndex check"}
334:                 key = payload.get("key")
335:                 return {"success": True, "value": self.kv_store.get(key), "leader_id": self.node_id}
336:             else:
337:                 leader_addr = self.peers.get(self.leader_id) if self.leader_id is not None else None
338:                 return {"success": False, "error": "Not leader", "leader_id": self.leader_id, "leader_addr": leader_addr}
```
* **Lines 329-338**: Processes `OP_GET` queries. If leader, validates quorum reachability via `_verify_leadership()`. If follower, rejects read and returns leader network address redirect hints.

```python
340:         if self.state != STATE_LEADER:
341:             leader_addr = self.peers.get(self.leader_id) if self.leader_id is not None else None
342:             return {"success": False, "error": "Not leader", "leader_id": self.leader_id, "leader_addr": leader_addr}
343: 
344:         client_id = payload.get("client_id")
345:         seq = payload.get("seq")
346:         if client_id and seq:
347:             session = self.client_sessions.get(client_id)
348:             if session and session["seq"] >= seq:
349:                 return session["response"]
350: 
351:         op_names = {OP_SET: "SET", OP_DELETE: "DELETE", OP_APPEND: "APPEND"}
352:         cmd = {
353:             "op": op_names[opcode],
354:             "key": payload.get("key"),
355:             "value": payload.get("value"),
356:             "client_id": client_id,
357:             "seq": seq
358:         }
359: 
360:         last_log_idx = self.last_included_index + len(self.log)
361:         new_index = last_log_idx + 1
362:         entry = {"term": self.current_term, "index": new_index, "command": cmd}
363:         self.log.append(entry)
364:         self.wal.append_log(self.current_term, new_index, cmd)
365:         self.match_index[self.node_id] = new_index
366: 
367:         self._check_snapshot()
368:         return {"success": True, "index": new_index}
```
* **Lines 340-368**: Processes mutations on Leader: checks session deduplication map, appends command to local log memory array, writes to persistent disk WAL, updates leader's internal `match_index`, and triggers background snapshot check.

```python
370:     def _handle_request_vote(self, payload: dict) -> dict:
371:         term = payload["term"]
372:         candidate_id = payload["candidate_id"]
373:         last_log_index = payload["last_log_index"]
374:         last_log_term = payload["last_log_term"]
375: 
376:         if term > self.current_term:
377:             self.current_term = term
378:             self.state = STATE_FOLLOWER
379:             self.voted_for = None
380:             self._persist_meta()
381: 
382:         vote_granted = False
383:         my_last_index = self.last_included_index + len(self.log)
384:         my_last_term = self._get_log_term(my_last_index)
385: 
386:         log_ok = (last_log_term > my_last_term) or \
387:                  (last_log_term == my_last_term and last_log_index >= my_last_index)
388: 
389:         if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
390:             vote_granted = True
391:             self.voted_for = candidate_id
392:             self._persist_meta()
393:             self.last_heartbeat_time = time.time()
394: 
395:         return {"term": self.current_term, "vote_granted": vote_granted}
```
* **Lines 370-395**: **RequestVote RPC Handler**:
  * Updates term and steps down to Follower if `term > current_term`.
  * Verifies candidate's log up-to-date criteria ($last\_log\_term > my\_last\_term$ OR equal terms with $last\_log\_index \ge my\_last\_index$).
  * Grants vote if term matches, candidate voted for is null/candidate_id, and log is up-to-date. Reset election timer.

```python
397:     def _handle_append_entries(self, payload: dict) -> dict:
398:         term = payload["term"]
399:         leader_id = payload["leader_id"]
400:         prev_log_index = payload["prev_log_index"]
401:         prev_log_term = payload["prev_log_term"]
402:         entries = payload.get("entries", [])
403:         leader_commit = payload.get("leader_commit", 0)
404: 
405:         if term < self.current_term:
406:             return {"term": self.current_term, "success": False}
407: 
408:         if term > self.current_term or self.state == STATE_CANDIDATE:
409:             self.current_term = term
410:             self.state = STATE_FOLLOWER
411:             self.voted_for = None
412:             self._persist_meta()
413: 
414:         self.leader_id = leader_id
415:         self.last_heartbeat_time = time.time()
```
* **Lines 397-415**: **AppendEntries Handler**:
  * Rejects request if `term < current_term`.
  * Converts Candidate to Follower and updates term if `term > current_term`.
  * Resets heartbeat election timer and stores leader identity.

```python
417:         my_last_idx = self.last_included_index + len(self.log)
418:         if prev_log_index > my_last_idx:
419:             return {"term": self.current_term, "success": False, "conflict_index": my_last_idx + 1, "conflict_term": None}
420: 
421:         if prev_log_index > 0 and prev_log_index > self.last_included_index:
422:             my_term_at_prev = self._get_log_term(prev_log_index)
423:             if my_term_at_prev != prev_log_term:
424:                 conflict_term = my_term_at_prev
425:                 conflict_index = prev_log_index
426:                 while conflict_index > self.last_included_index + 1 and self._get_log_term(conflict_index - 1) == conflict_term:
427:                     conflict_index -= 1
428:                 return {"term": self.current_term, "success": False, "conflict_index": conflict_index, "conflict_term": conflict_term}
```
* **Lines 417-428**: **Fast Log Reconciliation Optimization**:
  * Rejects if `prev_log_index` extends beyond local log array.
  * Detects term mismatch at `prev_log_index`, computing `conflict_index` and `conflict_term` to allow the leader to bypass multiple single-step log alignment roundtrips.

```python
430:         for entry in entries:
431:             idx = entry["index"]
432:             relative_idx = idx - self.last_included_index - 1
433:             if relative_idx >= 0:
434:                 if relative_idx < len(self.log):
435:                     if self.log[relative_idx]["term"] != entry["term"]:
436:                         self.log = self.log[:relative_idx]
437:                         self.log.append(entry)
438:                         self.wal.truncate_and_write(self.log)
439:                 else:
440:                     self.log.append(entry)
441:                     self.wal.append_log(entry["term"], entry["index"], entry["command"])
```
* **Lines 430-441**: Appends new entries or resolves log divergence by truncating local uncommitted logs and overwriting WAL.

```python
443:         if leader_commit > self.commit_index:
444:             self.commit_index = min(leader_commit, self.last_included_index + len(self.log))
445:             while self.last_applied < self.commit_index:
446:                 self.last_applied += 1
447:                 if self.last_applied > self.last_included_index:
448:                     rel_idx = self.last_applied - self.last_included_index - 1
449:                     if 0 <= rel_idx < len(self.log):
450:                         self.apply_command(self.log[rel_idx]["command"])
451: 
452:         return {"term": self.current_term, "success": True}
```
* **Lines 443-452**: Advances follower's `commit_index` to match leader commit level and applies newly committed entries sequentially to `kv_store`.

```python
454:     def _handle_install_snapshot(self, payload: dict) -> dict:
455:         term = payload["term"]
456:         if term < self.current_term:
457:             return {"term": self.current_term}
458: 
459:         if term > self.current_term:
460:             self.current_term = term
461:             self.state = STATE_FOLLOWER
462:             self.voted_for = None
463:             self._persist_meta()
464: 
465:         self.leader_id = payload["leader_id"]
466:         self.last_heartbeat_time = time.time()
467: 
468:         self.last_included_index = payload["last_included_index"]
469:         self.last_included_term = payload["last_included_term"]
470:         self.kv_store = payload["kv_store"]
471:         self.client_sessions = payload.get("client_sessions", {})
472:         self.log = []
473:         self.wal.truncate_and_write([])
474:         self._save_snapshot()
475: 
476:         self.commit_index = max(self.commit_index, self.last_included_index)
477:         self.last_applied = max(self.last_applied, self.last_included_index)
478:         return {"term": self.current_term}
```
* **Lines 454-478**: **InstallSnapshot RPC Handler**:
  * Called when a lagging follower's next index has been compacted on the leader.
  * Overwrites local store and session state with snapshot payload, truncates uncompacted WAL entries, updates snapshot metadata, and persists snapshot file (`.snap`).

```python
480:     def _election_timer_loop(self):
481:         while self.running:
482:             timeout = random.uniform(1.5, 3.0)
483:             time.sleep(0.1)
484:             should_start_election = False
485:             with self.lock:
486:                 if self.state != STATE_LEADER and self.running:
487:                     if time.time() - self.last_heartbeat_time > timeout:
488:                         should_start_election = True
489: 
490:             if should_start_election:
491:                 self._start_election()
```
* **Lines 480-491**: Continuously checks for election timeout. Uses randomized intervals ($1.5\text{s} - 3.0\text{s}$) to split votes. Triggers `_start_election()` if elapsed time exceeds randomized timeout limit.

```python
493:     def _replication_loop(self):
494:         while self.running:
495:             time.sleep(0.05)
496:             with self.lock:
497:                 if self.state != STATE_LEADER or not self.running:
498:                     continue
499: 
500:             for pid, addr in self.peers.items():
501:                 threading.Thread(target=self._send_replication_to_peer, args=(pid, addr), daemon=True).start()
```
* **Lines 493-501**: Ticker loop running every 50ms on the Leader node. Spawns threads to replicate log entries or send heartbeats (`_send_replication_to_peer`) to all configured peers.

```python
503:     def _send_replication_to_peer(self, peer_id, peer_addr):
504:         with self.lock:
505:             if self.state != STATE_LEADER or not self.running:
506:                 return
507:             prev_index = self.next_index.get(peer_id, self.last_included_index + 1) - 1
508: 
509:             if prev_index < self.last_included_index:
510:                 req = {
511:                     "term": self.current_term,
512:                     "leader_id": self.node_id,
513:                     "last_included_index": self.last_included_index,
514:                     "last_included_term": self.last_included_term,
515:                     "kv_store": self.kv_store,
516:                     "client_sessions": self.client_sessions
517:                 }
518:                 is_snapshot = True
519:             else:
520:                 prev_term = self._get_log_term(prev_index)
521:                 entries_to_send = [e for e in self.log if e["index"] > prev_index]
522:                 req = {
523:                     "term": self.current_term,
524:                     "leader_id": self.node_id,
525:                     "prev_log_index": prev_index,
526:                     "prev_log_term": prev_term,
527:                     "entries": entries_to_send,
528:                     "leader_commit": self.commit_index
529:                 }
530:                 is_snapshot = False
```
* **Lines 503-530**: Constructs replication payload. If target `prev_index` fell behind due to log compaction, selects `InstallSnapshot` mode; otherwise constructs an `AppendEntries` payload.

```python
532:         sock = self.conn_pool.get_connection(peer_addr[0], peer_addr[1])
533:         if not sock:
534:             return
535: 
536:         try:
537:             opcode = OP_RAFT_INSTALL_SNAPSHOT if is_snapshot else OP_RAFT_APPEND_ENTRIES
538:             send_message(sock, opcode, req)
539:             _, res = recv_message(sock)
540:             if not res:
541:                 return
542: 
543:             with self.lock:
544:                 if res.get("term") > self.current_term:
545:                     self.current_term = res["term"]
546:                     self.state = STATE_FOLLOWER
547:                     self.voted_for = None
548:                     self._persist_meta()
549:                     return
```
* **Lines 532-549**: Transmits replication payload over connection pool socket. Step-down check: reverts to Follower if target peer term is higher.

```python
551:                 if self.state == STATE_LEADER:
552:                     if is_snapshot:
553:                         self.match_index[peer_id] = self.last_included_index
554:                         self.next_index[peer_id] = self.last_included_index + 1
555:                     else:
556:                         if res.get("success"):
557:                             if entries_to_send:
558:                                 last_sent_index = entries_to_send[-1]["index"]
559:                                 self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), last_sent_index)
560:                                 self.next_index[peer_id] = self.match_index[peer_id] + 1
561:                             self._try_advance_commit_index()
562:                         else:
563:                             conflict_index = res.get("conflict_index")
564:                             if conflict_index:
565:                                 self.next_index[peer_id] = max(1, conflict_index)
566:                             else:
567:                                 self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)
568:         except Exception:
569:             self.conn_pool.close_connection(peer_addr[0], peer_addr[1])
```
* **Lines 551-569**: Evaluates replication RPC response:
  * Updates `match_index` and `next_index` on success, then attempts to advance global commit index (`_try_advance_commit_index`).
  * On failure, decrements `next_index` or jumps directly back to the returned `conflict_index`.

```python
571:     def _try_advance_commit_index(self):
572:         total_nodes = len(self.peers) + 1
573:         needed_quorum = (total_nodes // 2) + 1
574:         last_log_idx = self.last_included_index + len(self.log)
575:         for n in range(last_log_idx, self.commit_index, -1):
576:             if self._get_log_term(n) == self.current_term:
577:                 matches = 1
578:                 for pid in self.peers:
579:                     if self.match_index.get(pid, 0) >= n:
580:                         matches += 1
581:                 if matches >= needed_quorum:
582:                     self.commit_index = n
583:                     while self.last_applied < self.commit_index:
584:                         self.last_applied += 1
585:                         if self.last_applied > self.last_included_index:
586:                             rel_idx = self.last_applied - self.last_included_index - 1
587:                             if 0 <= rel_idx < len(self.log):
588:                                 self.apply_command(self.log[rel_idx]["command"])
589:                     break
```
* **Lines 571-589**: Iterates backward from highest log index to current commit index. If entry $n$ is from `current_term` and verified replicated on a quorum majority ($\ge \lfloor N/2 \rfloor + 1$), updates `commit_index = n` and applies intermediate committed commands to the state machine.

```python
591:     def _start_election(self):
592:         with self.lock:
593:             self.state = STATE_CANDIDATE
594:             self.current_term += 1
595:             self.voted_for = self.node_id
596:             self._persist_meta()
597:             self.last_heartbeat_time = time.time()
598:             votes = 1
599:             term = self.current_term
600:             last_log_idx = self.last_included_index + len(self.log)
601:             last_log_term = self._get_log_term(last_log_idx)
602: 
603:         total_nodes = len(self.peers) + 1
604:         needed_votes = (total_nodes // 2) + 1
605:         vote_lock = threading.Lock()
```
* **Lines 591-605**: Triggers Candidate state transition: increments term, votes for self, updates metadata, and initializes vote counter to 1.

```python
607:         def request_vote(pid, addr):
608:             nonlocal votes
609:             sock = self.conn_pool.get_connection(addr[0], addr[1])
610:             if not sock:
611:                 return
612:             req = {
613:                 "term": term,
614:                 "candidate_id": self.node_id,
615:                 "last_log_index": last_log_idx,
616:                 "last_log_term": last_log_term
617:             }
618:             try:
619:                 send_message(sock, OP_RAFT_VOTE, req)
620:                 _, res = recv_message(sock)
621:                 if res:
622:                     with self.lock:
623:                         if res.get("term") > self.current_term:
624:                             self.current_term = res["term"]
625:                             self.state = STATE_FOLLOWER
626:                             self.voted_for = None
627:                             self._persist_meta()
628:                             return
629:                     if res.get("vote_granted"):
630:                         with vote_lock:
631:                             votes += 1
632:             except Exception:
633:                 self.conn_pool.close_connection(addr[0], addr[1])
634: 
635:         threads = [threading.Thread(target=request_vote, args=(pid, addr)) for pid, addr in self.peers.items()]
636:         for t in threads: t.start()
637:         for t in threads: t.join(timeout=0.5)
```
* **Lines 607-637**: Concurrently sends `RequestVote` RPCs to all peer addresses in parallel threads. Collects votes under `vote_lock` protection.

```python
639:         with self.lock:
640:             if votes >= needed_votes and self.state == STATE_CANDIDATE:
641:                 self.state = STATE_LEADER
642:                 self.leader_id = self.node_id
643:                 last_idx = self.last_included_index + len(self.log)
644:                 for pid in self.peers:
645:                     self.next_index[pid] = last_idx + 1
646:                     self.match_index[pid] = self.last_included_index
```
* **Lines 639-646**: Promotes candidate to **Leader** if votes secured meet or exceed majority quorum ($\ge \lfloor N/2 \rfloor + 1$). Initializes `next_index` and `match_index` tracking tables for all peers.

```python
648:     def _check_snapshot(self):
649:         if len(self.log) > self.snapshot_threshold:
650:             snapshot_count = len(self.log) // 2
651:             compacted_entries = self.log[snapshot_count:]
652:             self.last_included_index = self.log[snapshot_count - 1]["index"]
653:             self.last_included_term = self.log[snapshot_count - 1]["term"]
654:             self.log = compacted_entries
655:             self.wal.truncate_and_write(self.log)
656:             self._save_snapshot()
```
* **Lines 648-656**: Evaluates log length against `snapshot_threshold`. When exceeded, compacts the older half of the log, updates `last_included_index` / `last_included_term`, truncates WAL, and persists the snapshot file.

```python
658: class RaftClient:
659:     def __init__(self, cluster_nodes: list, client_id: str = "client_cli"):
660:         self.cluster_nodes = cluster_nodes
661:         self.leader_addr = None
662:         self.client_id = client_id
663:         self.seq = 0
```
* **Lines 658-663**: Initializes the smart client tracking node endpoints, sequence numbers (`seq`), and cached leader address (`leader_addr`).

```python
665:     def _connect(self, host, port):
666:         s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
667:         s.settimeout(2.0)
668:         s.connect((host, port))
669:         return s
```
* **Lines 665-669**: Creates a socket connection to a target node host/port with a 2-second connection timeout.

```python
671:     def execute(self, opcode: int, payload: dict):
672:         if opcode != OP_GET:
673:             self.seq += 1
674:             payload["client_id"] = self.client_id
675:             payload["seq"] = self.seq
```
* **Lines 671-675**: Increments client sequence counter for mutation calls (`SET`, `DELETE`, `APPEND`), embedding `client_id` and `seq` headers into command payload.

```python
677:         targets = [self.leader_addr] + self.cluster_nodes if self.leader_addr else list(self.cluster_nodes)
678:         for addr in targets:
679:             if not addr:
680:                 continue
681:             try:
682:                 s = self._connect(addr[0], addr[1])
683:                 send_message(s, opcode, payload)
684:                 _, resp = recv_message(s)
685:                 s.close()
686: 
687:                 if resp.get("success"):
688:                     self.leader_addr = addr
689:                     return resp
690:                 elif resp.get("leader_addr"):
691:                     self.leader_addr = tuple(resp["leader_addr"])
692:                     return self.execute(opcode, payload)
693:             except Exception:
694:                 continue
695: 
696:         return {"success": False, "error": "Could not connect to cluster leader"}
```
* **Lines 677-696**: Tries contacting the cached leader first. If redirected or connection fails, falls back to cycling through remaining cluster nodes until finding the current leader.

---

### File 3: `cli_shell.py`

#### Purpose
An interactive Terminal Control Panel for managing local Raft nodes, viewing live cluster status, triggering simulated node crashes, and executing key-value commands.

#### Line-by-Line Breakdown

```python
 1: import os
 2: import sys
 3: import time
 4: import threading
 5: from raft_cluster import RaftNode, RaftClient, OP_SET, OP_GET, OP_DELETE, OP_APPEND
```
* **Lines 1-5**: Imports administrative system libraries along with Raft protocol structures.

```python
 7: class ClusterManager:
 8:     def __init__(self):
 9:         self.nodes = {}
10:         self.configs = {
11:             1: ("127.0.0.1", 8001, {2: ("127.0.0.1", 8002), 3: ("127.0.0.1", 8003)}, "node1.wal"),
12:             2: ("127.0.0.1", 8002, {1: ("127.0.0.1", 8001), 3: ("127.0.0.1", 8003)}, "node3.wal"),
13:             3: ("127.0.0.1", 8003, {1: ("127.0.0.1", 8001), 2: ("127.0.0.1", 8002)}, "node3.wal"),
14:         }
15:         self.client_addrs = [("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)]
16:         self.client = RaftClient(self.client_addrs)
```
* **Lines 7-16**: Encapsulates cluster management logic. Configures 3 cluster nodes on loopback ports 8001, 8002, and 8003 with respective WAL paths.

```python
18:     def start_all(self):
19:         for nid, (host, port, peers, wal) in self.configs.items():
20:             node = RaftNode(nid, host, port, peers, wal)
21:             node.start()
22:             self.nodes[nid] = node
```
* **Lines 18-22**: Instantiates and boots up all configured Raft node instances.

```python
24:     def stop_all(self):
25:         for node in self.nodes.values():
26:             node.stop()
```
* **Lines 24-26**: Stops all running nodes in the cluster cleanly.

```python
28:     def kill_node(self, nid: int):
29:         if nid in self.nodes:
30:             self.nodes[nid].stop()
31:             return True
32:         return False
```
* **Lines 28-32**: Simulates node failure/crash by calling `stop()` on a specific node ID without deleting its persistent storage files.

```python
34:     def restart_node(self, nid: int):
35:         if nid in self.nodes:
36:             self.nodes[nid].stop()
37:             host, port, peers, wal = self.configs[nid]
38:             new_node = RaftNode(nid, host, port, peers, wal)
39:             new_node.start()
40:             self.nodes[nid] = new_node
41:             return True
42:         return False
```
* **Lines 34-42**: Restarts a crashed node by creating a new `RaftNode` instance using the same configuration and disk WAL/snapshot files.

```python
44: def clear_screen():
45:     os.system('cls' if os.name == 'nt' else 'clear')
```
* **Lines 44-45**: Screen clear helper covering Windows (`cls`) and POSIX (`clear`).

```python
47: def render_dashboard(manager: ClusterManager):
48:     clear_screen()
49:     print("┌────────────────────────────────────────────────────────────────────────┐")
50:     print("│                  DISTRIBUTED RAFT KV-STORE CONTROL PANEL               │")
51:     print("├────────────────────────────────────────────────────────────────────────┤")
52:     print(f"│ {'ID':<4} │ {'HOST:PORT':<15} │ {'STATE':<10} │ {'TERM':<6} │ {'COMMIT':<8} │ {'KEYS':<6} │")
53:     print("├──────┼─────────────────┼────────────┼────────┼──────────┼──────────┤")
54:     
55:     for nid, node in sorted(manager.nodes.items()):
56:         with node.lock:
57:             state = node.state
58:             term = node.current_term
59:             commit = node.commit_index
60:             kcount = len(node.kv_store)
61:             running = node.running
62:         
63:         if not running:
64:             state = "OFFLINE"
65:         
66:         host_port = f"{node.host}:{node.port}"
67:         print(f"│ #{nid:<3} │ {host_port:<15} │ {state:<10} │ {term:<6} │ {commit:<8} │ {kcount:<6} │")
68:     
69:     print("└──────┴─────────────────┴────────────┴────────┴──────────┴──────────┘")
70:     print(" COMMANDS:                                                               ")
71:     print("   set <key> <val>     - Write key-value pair                            ")
72:     print("   get <key>           - Retrieve value (ReadIndex linearizable)         ")
73:     print("   del <key>           - Delete key                                      ")
74:     print("   app <key> <val>     - Append string to key                            ")
75:     print("   kill <node_id>      - Simulate node crash / network isolation         ")
76:     print("   start <node_id>     - Restart offline node                            ")
77:     print("   refresh             - Redraw dashboard                                ")
78:     print("   exit                - Terminate cluster and exit                      ")
79:     print("─────────────────────────────────────────────────────────────────────────")
```
* **Lines 47-79**: Renders an ASCII text dashboard displaying Node ID, Socket Host/Port, Raft State (`LEADER`, `FOLLOWER`, `CANDIDATE`, `OFFLINE`), Term counter, Commit Index, and total keys stored.

```python
81: def main():
82:     manager = ClusterManager()
83:     manager.start_all()
84:     time.sleep(1.5)
```
* **Lines 81-84**: CLI entry point initializing cluster manager, booting all nodes, and waiting 1.5 seconds for initial leader election.

```python
86:     while True:
87:         render_dashboard(manager)
88:         try:
89:             cmd_line = input("RAFT-CLI> ").strip()
90:         except (KeyboardInterrupt, EOFError):
91:             break
92: 
93:         if not cmd_line:
94:             continue
95: 
96:         parts = cmd_line.split(" ", 2)
97:         action = parts[0].lower()
```
* **Lines 86-97**: Interactive Command Line REPL loop capturing keyboard input and parsing action arguments.

```python
99:         if action == "exit":
100:             print("\nShutting down cluster nodes...")
101:             manager.stop_all()
102:             break
103:         elif action == "refresh":
104:             continue
105:         elif action == "kill":
106:             if len(parts) > 1 and parts[1].isdigit():
107:                 nid = int(parts[1])
108:                 success = manager.kill_node(nid)
109:                 print(f"Node #{nid} shutdown: {success}")
110:             else:
111:                 print("Usage: kill <node_id>")
112:             time.sleep(1)
113:         elif action == "start":
114:             if len(parts) > 1 and parts[1].isdigit():
115:                 nid = int(parts[1])
116:                 success = manager.restart_node(nid)
117:                 print(f"Node #{nid} started: {success}")
118:             else:
119:                 print("Usage: start <node_id>")
120:             time.sleep(1)
```
* **Lines 99-120**: Command branches for cluster lifecycle commands (`exit`, `refresh`, `kill`, `start`).

```python
121:         elif action == "set":
122:             if len(parts) == 3:
123:                 key, val = parts[1], parts[2]
124:                 res = manager.client.execute(OP_SET, {"key": key, "value": val})
125:                 print(f"RESULT: {res}")
126:             else:
127:                 print("Usage: set <key> <value>")
128:             input("\nPress Enter to continue...")
129:         elif action == "get":
130:             if len(parts) == 2:
131:                 key = parts[1]
132:                 res = manager.client.execute(OP_GET, {"key": key})
133:                 print(f"RESULT: {res}")
134:             else:
135:                 print("Usage: get <key>")
136:             input("\nPress Enter to continue...")
137:         elif action == "del":
138:             if len(parts) == 2:
139:                 key = parts[1]
140:                 res = manager.client.execute(OP_DELETE, {"key": key})
141:                 print(f"RESULT: {res}")
142:             else:
143:                 print("Usage: del <key>")
144:             input("\nPress Enter to continue...")
145:         elif action == "app":
146:             if len(parts) == 3:
147:                 key, val = parts[1], parts[2]
148:                 res = manager.client.execute(OP_APPEND, {"key": key, "value": val})
149:                 print(f"RESULT: {res}")
150:             else:
151:                 print("Usage: app <key> <value>")
152:             input("\nPress Enter to continue...")
153:         else:
154:             print(f"Unknown command: {action}")
155:             time.sleep(1)
```
* **Lines 121-155**: Command handlers mapping CLI key-value interactions (`set`, `get`, `del`, `app`) to client execution operations.

```python
157: if __name__ == "__main__":
158:     main()
```
* **Lines 157-158**: Script entry point protection block.

---

### File 4: `run_cluster.py`

#### Purpose
Integrates end-to-end cluster bootstrapping, cleans legacy file artifacts, runs baseline sequential mutations to force snapshot compaction, and verifies storage engine functionality.

#### Line-by-Line Breakdown

```python
1: import time
2: import os
3: import shutil
4: from raft_cluster import RaftNode, RaftClient, OP_SET, OP_GET
```
* **Lines 1-4**: Imports timing, file operations, and Raft primitives.

```python
6: def cleanup():
7:     for f in os.listdir('.'):
8:         if f.endswith('.wal') or f.endswith('.meta') or f.endswith('.snap') or f.endswith('.tmp'):
9:             try:
10:                 os.remove(f)
11:             except Exception:
12:                 pass
```
* **Lines 6-12**: Purges temporary test files (`*.wal`, `*.meta`, `*.snap`, `*.tmp`) to guarantee clean, reproducible test conditions on startup.

```python
14: def main():
15:     cleanup()
16: 
17:     peers_node1 = {2: ("127.0.0.1", 8002), 3: ("127.0.0.1", 8003)}
18:     peers_node2 = {1: ("127.0.0.1", 8001), 3: ("127.0.0.1", 8003)}
19:     peers_node3 = {1: ("127.0.0.1", 8001), 2: ("127.0.0.1", 8002)}
20: 
21:     node1 = RaftNode(node_id=1, peers=peers_node1, wal_path="node1.wal", snapshot_threshold=10)
22:     node2 = RaftNode(node_id=2, peers=peers_node2, wal_path="node2.wal", snapshot_threshold=10)
23:     node3 = RaftNode(node_id=3, peers=peers_node3, wal_path="node3.wal", snapshot_threshold=10)
```
* **Lines 14-23**: Configures a 3-node Raft cluster with a low `snapshot_threshold=10` to force log compaction during tests.

```python
25:     print("Starting 3 Raft cluster nodes...")
26:     node1.start("127.0.0.1", 8001)
27:     node2.start("127.0.0.1", 8002)
28:     node3.start("127.0.0.1", 8003)
29: 
30:     print("Waiting for cluster leader election...")
31:     time.sleep(3)
```
* **Lines 25-31**: Starts socket listeners on all nodes and waits 3 seconds for leader election completion.

```python
33:     client = RaftClient(cluster_nodes=[("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)])
34: 
35:     print("\nExecuting baseline write operations...")
36:     for i in range(15):
37:         client.execute(OP_SET, {"key": f"key_{i}", "value": f"value_{i}"})
```
* **Lines 33-37**: Executes 15 write requests (`OP_SET`). Exceeding the `snapshot_threshold=10` forces the cluster to perform log compaction and write snapshot files.

```python
39:     print("Executing baseline read operation...")
40:     res = client.execute(OP_GET, {"key": "key_5"})
41:     print("GET key_5 Result:", res)
42: 
43:     print("\nCluster initialized successfully.")
```
* **Lines 39-43**: Performs a ReadIndex linearizable read call (`OP_GET key_5`), verifies state machine execution output, and prints initial status.

```python
45: if __name__ == "__main__":
46:     main()
```
* **Lines 45-46**: Entry point block executing integration testing routine.

---

### File 5: `benchmark.py`

#### Purpose
A concurrent stress-testing utility that launches worker threads against the Raft cluster to measure operations-per-second (OPS) throughput and evaluate write-path performance under thread contention.

#### Line-by-Line Breakdown

```python
1: import time
2: import threading
3: from raft_cluster import RaftClient, OP_SET, OP_GET, OP_APPEND
```
* **Lines 1-3**: Imports execution timers, thread management tools, and client interaction opcodes.

```python
5: CLUSTER_NODES = [("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)]
```
* **Line 5**: List of target cluster nodes for client requests.

```python
7: def run_benchmark(num_ops=100, num_threads=4):
8:     print(f"Running benchmark with {num_ops} ops across {num_threads} threads...")
9:     start_time = time.time()
10:     
11:     success_count = 0
12:     lock = threading.Lock()
```
* **Lines 7-12**: Benchmark runner initialization. Configures target operations (`num_ops`), thread count (`num_threads`), start timer, success counter, and counter protection lock.

```python
14:     def worker(thread_id):
15:         nonlocal success_count
16:         client = RaftClient(CLUSTER_NODES, client_id=f"bench_client_{thread_id}")
17:         for i in range(num_ops // num_threads):
18:             key = f"key_{thread_id}_{i}"
19:             val = f"val_{i}"
20:             res = client.execute(OP_SET, {"key": key, "value": val})
21:             if res.get("success"):
22:                 with lock:
23:                     success_count += 1
```
* **Lines 14-23**: Worker thread execution logic:
  * Instantiates a distinct `RaftClient` per thread with a unique `client_id`.
  * Computes equal work share `num_ops // num_threads`.
  * Executes synchronous write operations (`OP_SET`) and safely increments global success count upon success.

```python
25:     threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
26:     for t in threads: t.start()
27:     for t in threads: t.join()
```
* **Lines 25-27**: Spawns worker threads, starts execution in parallel, and waits for all threads to join completion.

```python
29:     total_time = time.time() - start_time
30:     ops_per_sec = success_count / total_time if total_time > 0 else 0
31:     print(f"Benchmark completed: {success_count}/{num_ops} ops succeeded in {total_time:.2f}s ({ops_per_sec:.2f} ops/sec)")
```
* **Lines 29-31**: Calculates elapsed time, computes operations per second ($\text{OPS} = \frac{\text{success\_count}}{\text{total\_time}}$), and prints benchmark metrics.

```python
33: if __name__ == "__main__":
34:     time.sleep(3)
35:     run_benchmark(num_ops=60, num_threads=3)
```
* **Lines 33-35**: Benchmark entry point. Pauses 3 seconds to ensure leader election completes, then executes 60 operations across 3 parallel client threads.

---

## Verification & Execution Guide

### 1. Booting Integration Test Suite
```bash
python run_cluster.py
```

### 2. Interactive Cluster Operations Panel
```bash
python cli_shell.py
```

### 3. Concurrency & Throughput Benchmark
Ensure cluster nodes are running, then launch:
```bash
python benchmark.py
```
