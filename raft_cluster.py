import socket
import struct
import json
import os
import time
import random
import threading

MAGIC_BYTES = b"RAFT"
HEADER_FORMAT = "!4sB I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

OP_SET = 1
OP_GET = 2
OP_DELETE = 3
OP_APPEND = 4
OP_RAFT_VOTE = 5
OP_RAFT_VOTE_RESP = 6
OP_RAFT_APPEND_ENTRIES = 7
OP_RAFT_APPEND_RESP = 8
OP_RESPONSE = 9

def send_message(sock, opcode: int, payload: dict):
    data = json.dumps(payload).encode('utf-8')
    header = struct.pack(HEADER_FORMAT, MAGIC_BYTES, opcode, len(data))
    sock.sendall(header + data)

def recv_exact(sock, n_bytes: int):
    buf = bytearray()
    while len(buf) < n_bytes:
        chunk = sock.recv(n_bytes - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)

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

class WAL:
    def __init__(self, filename: str):
        self.filename = filename
        self.file = open(filename, "a+b", buffering=0)

    def append_log(self, term: int, index: int, command: dict):
        entry = {"term": term, "index": index, "command": command}
        line = json.dumps(entry).encode('utf-8') + b"\n"
        self.file.write(line)
        os.fsync(self.file.fileno())

    def read_all(self):
        entries = []
        if not os.path.exists(self.filename):
            return entries
        with open(self.filename, "rb") as f:
            for line in f:
                if line.strip():
                    entries.append(json.loads(line.decode('utf-8')))
        return entries

    def truncate_and_write(self, entries):
        self.file.close()
        with open(self.filename, "wb") as f:
            for entry in entries:
                f.write(json.dumps(entry).encode('utf-8') + b"\n")
            f.flush()
            os.fsync(f.fileno())
        self.file = open(self.filename, "a+b", buffering=0)

STATE_FOLLOWER = "FOLLOWER"
STATE_CANDIDATE = "CANDIDATE"
STATE_LEADER = "LEADER"

class RaftNode:
    def __init__(self, node_id: int, peers: dict, wal_path: str, snapshot_threshold: int = 100):
        self.node_id = node_id
        self.peers = peers
        self.wal_path = wal_path
        self.wal = WAL(wal_path)
        self.snapshot_threshold = snapshot_threshold

        self.current_term = 0
        self.voted_for = None
        self.log = []

        # Snapshot state
        self.last_included_index = 0
        self.last_included_term = 0

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

    def _recover_from_wal(self):
        with self.lock:
            self.log = self.wal.read_all()
            for entry in self.log:
                self.apply_command(entry["command"])
                self.commit_index = max(self.commit_index, entry["index"])
                self.last_applied = max(self.last_applied, entry["index"])

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

    def start(self, host: str, port: int):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((host, port))
        server_sock.listen(128)

        threading.Thread(target=self._accept_loop, args=(server_sock,), daemon=True).start()
        threading.Thread(target=self._election_timer_loop, daemon=True).start()
        threading.Thread(target=self._replication_loop, daemon=True).start()

    def _accept_loop(self, server_sock):
        while self.running:
            try:
                conn, addr = server_sock.accept()
                threading.Thread(target=self._client_handler, args=(conn,), daemon=True).start()
            except Exception:
                break

    def _client_handler(self, conn):
        with conn:
            while self.running:
                try:
                    opcode, payload = recv_message(conn)
                    if opcode is None:
                        break
                    resp = self.handle_rpc(opcode, payload)
                    send_message(conn, OP_RESPONSE, resp)
                except Exception:
                    break

    def handle_rpc(self, opcode: int, payload: dict) -> dict:
        with self.lock:
            if opcode in (OP_SET, OP_GET, OP_DELETE, OP_APPEND):
                return self._handle_client_cmd(opcode, payload)
            elif opcode == OP_RAFT_VOTE:
                return self._handle_request_vote(payload)
            elif opcode == OP_RAFT_APPEND_ENTRIES:
                return self._handle_append_entries(payload)
            return {"success": False, "error": "Unknown opcode"}

    def _get_log_term(self, index: int) -> int:
        if index == self.last_included_index:
            return self.last_included_term
        relative_idx = index - self.last_included_index - 1
        if 0 <= relative_idx < len(self.log):
            return self.log[relative_idx]["term"]
        return 0

    def _handle_client_cmd(self, opcode: int, payload: dict) -> dict:
        if opcode == OP_GET:
            key = payload.get("key")
            return {"success": True, "value": self.kv_store.get(key), "leader_id": self.leader_id}

        if self.state != STATE_LEADER:
            leader_addr = self.peers.get(self.leader_id) if self.leader_id is not None else None
            return {"success": False, "error": "Not leader", "leader_id": self.leader_id, "leader_addr": leader_addr}

        op_names = {OP_SET: "SET", OP_DELETE: "DELETE", OP_APPEND: "APPEND"}
        cmd = {"op": op_names[opcode], "key": payload.get("key"), "value": payload.get("value")}

        last_log_idx = self.last_included_index + len(self.log)
        new_index = last_log_idx + 1
        entry = {"term": self.current_term, "index": new_index, "command": cmd}
        self.log.append(entry)
        self.wal.append_log(self.current_term, new_index, cmd)
        self.match_index[self.node_id] = new_index

        self._check_snapshot()
        return {"success": True, "index": new_index}

    def _handle_request_vote(self, payload: dict) -> dict:
        term = payload["term"]
        candidate_id = payload["candidate_id"]
        last_log_index = payload["last_log_index"]
        last_log_term = payload["last_log_term"]

        if term > self.current_term:
            self.current_term = term
            self.state = STATE_FOLLOWER
            self.voted_for = None

        vote_granted = False
        my_last_index = self.last_included_index + len(self.log)
        my_last_term = self._get_log_term(my_last_index)

        log_ok = (last_log_term > my_last_term) or \
                 (last_log_term == my_last_term and last_log_index >= my_last_index)

        if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            vote_granted = True
            self.voted_for = candidate_id
            self.last_heartbeat_time = time.time()

        return {"term": self.current_term, "vote_granted": vote_granted}

    def _handle_append_entries(self, payload: dict) -> dict:
        term = payload["term"]
        leader_id = payload["leader_id"]
        prev_log_index = payload["prev_log_index"]
        prev_log_term = payload["prev_log_term"]
        entries = payload.get("entries", [])
        leader_commit = payload.get("leader_commit", 0)

        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        if term > self.current_term or self.state == STATE_CANDIDATE:
            self.current_term = term
            self.state = STATE_FOLLOWER
            self.voted_for = None

        self.leader_id = leader_id
        self.last_heartbeat_time = time.time()

        # Enforce Log Matching Property (§5.3)
        if prev_log_index > 0:
            my_term_at_prev = self._get_log_term(prev_log_index)
            if my_term_at_prev != prev_log_term:
                return {"term": self.current_term, "success": False}

        # Process and reconcile entries
        for entry in entries:
            idx = entry["index"]
            relative_idx = idx - self.last_included_index - 1
            if relative_idx >= 0:
                if relative_idx < len(self.log):
                    if self.log[relative_idx]["term"] != entry["term"]:
                        self.log = self.log[:relative_idx]
                        self.log.append(entry)
                        self.wal.truncate_and_write(self.log)
                else:
                    self.log.append(entry)
                    self.wal.append_log(entry["term"], entry["index"], entry["command"])

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.last_included_index + len(self.log))
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                if self.last_applied > self.last_included_index:
                    rel_idx = self.last_applied - self.last_included_index - 1
                    if 0 <= rel_idx < len(self.log):
                        self.apply_command(self.log[rel_idx]["command"])

        return {"term": self.current_term, "success": True}

    def _election_timer_loop(self):
        while self.running:
            timeout = random.uniform(1.5, 3.0)
            time.sleep(0.1)

            should_start_election = False
            with self.lock:
                if self.state != STATE_LEADER:
                    if time.time() - self.last_heartbeat_time > timeout:
                        should_start_election = True

            if should_start_election:
                self._start_election()

    def _replication_loop(self):
        while self.running:
            time.sleep(0.05)
            with self.lock:
                if self.state != STATE_LEADER:
                    continue

            for pid, addr in self.peers.items():
                threading.Thread(target=self._send_append_entries_to_peer, args=(pid, addr), daemon=True).start()

    def _send_append_entries_to_peer(self, peer_id, peer_addr):
        with self.lock:
            if self.state != STATE_LEADER:
                return
            prev_index = self.next_index.get(peer_id, self.last_included_index + 1) - 1
            prev_term = self._get_log_term(prev_index)
            
            entries_to_send = []
            for entry in self.log:
                if entry["index"] > prev_index:
                    entries_to_send.append(entry)

            req = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": entries_to_send,
                "leader_commit": self.commit_index
            }

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect((peer_addr[0], peer_addr[1]))
            send_message(s, OP_RAFT_APPEND_ENTRIES, req)
            _, res = recv_message(s)
            s.close()

            if res:
                with self.lock:
                    if res.get("term") > self.current_term:
                        self.current_term = res["term"]
                        self.state = STATE_FOLLOWER
                        self.voted_for = None
                        return

                    if self.state == STATE_LEADER:
                        if res.get("success"):
                            if entries_to_send:
                                last_sent_index = entries_to_send[-1]["index"]
                                self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), last_sent_index)
                                self.next_index[peer_id] = self.match_index[peer_id] + 1
                            self._try_advance_commit_index()
                        else:
                            self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)
        except Exception:
            pass

    def _try_advance_commit_index(self):
        total_nodes = len(self.peers) + 1
        needed_quorum = (total_nodes // 2) + 1
        
        last_log_idx = self.last_included_index + len(self.log)
        for n in range(last_log_idx, self.commit_index, -1):
            if self._get_log_term(n) == self.current_term:
                matches = 1  
                for pid in self.peers:
                    if self.match_index.get(pid, 0) >= n:
                        matches += 1
                if matches >= needed_quorum:
                    self.commit_index = n
                    while self.last_applied < self.commit_index:
                        self.last_applied += 1
                        if self.last_applied > self.last_included_index:
                            rel_idx = self.last_applied - self.last_included_index - 1
                            if 0 <= rel_idx < len(self.log):
                                self.apply_command(self.log[rel_idx]["command"])
                    break

    def _start_election(self):
        with self.lock:
            self.state = STATE_CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat_time = time.time()
            votes = 1
            term = self.current_term
            last_log_idx = self.last_included_index + len(self.log)
            last_log_term = self._get_log_term(last_log_idx)

        total_nodes = len(self.peers) + 1
        needed_votes = (total_nodes // 2) + 1
        vote_lock = threading.Lock()

        def request_vote(pid, addr):
            nonlocal votes
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(0.5)
                s.connect((addr[0], addr[1]))
                req = {
                    "term": term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_idx,
                    "last_log_term": last_log_term
                }
                send_message(s, OP_RAFT_VOTE, req)
                _, res = recv_message(s)
                s.close()
                if res:
                    with self.lock:
                        if res.get("term") > self.current_term:
                            self.current_term = res["term"]
                            self.state = STATE_FOLLOWER
                            self.voted_for = None
                            return
                    if res.get("vote_granted"):
                        with vote_lock:
                            votes += 1
            except Exception:
                pass

        threads = []
        for pid, addr in self.peers.items():
            t = threading.Thread(target=request_vote, args=(pid, addr))
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=0.5)

        with self.lock:
            if votes >= needed_votes and self.state == STATE_CANDIDATE:
                self.state = STATE_LEADER
                self.leader_id = self.node_id
                last_idx = self.last_included_index + len(self.log)
                for pid in self.peers:
                    self.next_index[pid] = last_idx + 1
                    self.match_index[pid] = self.last_included_index

    def _check_snapshot(self):
        if len(self.log) > self.snapshot_threshold:
            snapshot_count = len(self.log) // 2
            compacted_entries = self.log[snapshot_count:]
            self.last_included_index = self.log[snapshot_count - 1]["index"]
            self.last_included_term = self.log[snapshot_count - 1]["term"]
            self.log = compacted_entries
            self.wal.truncate_and_write(self.log)

class RaftClient:
    def __init__(self, cluster_nodes: list):
        self.cluster_nodes = cluster_nodes
        self.leader_addr = None

    def _connect(self, host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect((host, port))
        return s

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
