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
OP_RAFT_INSTALL_SNAPSHOT = 9
OP_RAFT_INSTALL_SNAPSHOT_RESP = 10
OP_RESPONSE = 11

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

class ConnectionPool:
    def __init__(self):
        self.conns = {}
        self.lock = threading.Lock()

    def get_connection(self, host: str, port: int):
        key = (host, port)
        with self.lock:
            sock = self.conns.get(key)
            if sock:
                return sock
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.0)
                sock.connect((host, port))
                self.conns[key] = sock
                return sock
            except Exception:
                return None

    def close_connection(self, host: str, port: int):
        key = (host, port)
        with self.lock:
            sock = self.conns.pop(key, None)
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

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
    def __init__(self, node_id: int, peers: dict, wal_path: str, snapshot_threshold: int = 50):
        self.node_id = node_id
        self.peers = peers
        self.wal_path = wal_path
        self.meta_path = wal_path + ".meta"
        self.snapshot_path = wal_path + ".snap"
        self.wal = WAL(wal_path)
        self.snapshot_threshold = snapshot_threshold
        self.conn_pool = ConnectionPool()

        self.current_term = 0
        self.voted_for = None
        self.log = []

        self.last_included_index = 0
        self.last_included_term = 0

        self.commit_index = 0
        self.last_applied = 0

        self.next_index = {}
        self.match_index = {}

        self.state = STATE_FOLLOWER
        self.leader_id = None
        self.kv_store = {}
        self.client_sessions = {}

        self.lock = threading.Lock()
        self.last_heartbeat_time = time.time()
        self.running = True

        self._load_meta()
        self._load_snapshot()
        self._recover_from_wal()

    def _load_meta(self):
        if os.path.exists(self.meta_path):
            with open(self.meta_path, "r") as f:
                data = json.load(f)
                self.current_term = data.get("current_term", 0)
                self.voted_for = data.get("voted_for", None)

    def _persist_meta(self):
        tmp_file = self.meta_path + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump({"current_term": self.current_term, "voted_for": self.voted_for}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, self.meta_path)

    def _load_snapshot(self):
        if os.path.exists(self.snapshot_path):
            with open(self.snapshot_path, "r") as f:
                data = json.load(f)
                self.last_included_index = data["last_included_index"]
                self.last_included_term = data["last_included_term"]
                self.kv_store = data["kv_store"]
                self.client_sessions = data.get("client_sessions", {})
                self.commit_index = self.last_included_index
                self.last_applied = self.last_included_index

    def _save_snapshot(self):
        data = {
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "kv_store": self.kv_store,
            "client_sessions": self.client_sessions,
        }
        tmp_file = self.snapshot_path + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, self.snapshot_path)

    def _recover_from_wal(self):
        with self.lock:
            self.log = self.wal.read_all()
            for entry in self.log:
                self.apply_command(entry["command"])
                self.commit_index = max(self.commit_index, entry["index"])
                self.last_applied = max(self.last_applied, entry["index"])

    def apply_command(self, cmd: dict):
        client_id = cmd.get("client_id")
        seq = cmd.get("seq")

        if client_id and seq:
            session = self.client_sessions.get(client_id)
            if session and session["seq"] >= seq:
                return session["response"]

        op = cmd.get("op")
        key = cmd.get("key")
        val = cmd.get("value")
        res = {"success": True}

        if op == "SET":
            self.kv_store[key] = val
        elif op == "DELETE":
            self.kv_store.pop(key, None)
        elif op == "APPEND":
            self.kv_store[key] = self.kv_store.get(key, "") + val

        if client_id and seq:
            self.client_sessions[client_id] = {"seq": seq, "response": res}
        return res

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
            elif opcode == OP_RAFT_INSTALL_SNAPSHOT:
                return self._handle_install_snapshot(payload)
            return {"success": False, "error": "Unknown opcode"}

    def _get_log_term(self, index: int) -> int:
        if index == 0:
            return 0
        if index == self.last_included_index:
            return self.last_included_term
        relative_idx = index - self.last_included_index - 1
        if 0 <= relative_idx < len(self.log):
            return self.log[relative_idx]["term"]
        return 0

    def _verify_leadership(self) -> bool:
        if self.state != STATE_LEADER:
            return False
        
        term = self.current_term
        acks = 1
        needed = (len(self.peers) + 1) // 2 + 1
        ack_lock = threading.Lock()

        def check_peer(pid, addr):
            nonlocal acks
            sock = self.conn_pool.get_connection(addr[0], addr[1])
            if not sock:
                return
            req = {
                "term": term,
                "leader_id": self.node_id,
                "prev_log_index": self.last_included_index + len(self.log),
                "prev_log_term": self._get_log_term(self.last_included_index + len(self.log)),
                "entries": [],
                "leader_commit": self.commit_index
            }
            try:
                send_message(sock, OP_RAFT_APPEND_ENTRIES, req)
                _, res = recv_message(sock)
                if res and res.get("term") == term:
                    with ack_lock:
                        acks += 1
            except Exception:
                self.conn_pool.close_connection(addr[0], addr[1])

        threads = [threading.Thread(target=check_peer, args=(pid, addr)) for pid, addr in self.peers.items()]
        for t in threads: t.start()
        for t in threads: t.join(timeout=0.2)
        return acks >= needed

    def _handle_client_cmd(self, opcode: int, payload: dict) -> dict:
        if opcode == OP_GET:
            if self.state == STATE_LEADER:
                if not self._verify_leadership():
                    return {"success": False, "error": "Leader validation failed"}
                key = payload.get("key")
                return {"success": True, "value": self.kv_store.get(key), "leader_id": self.node_id}
            else:
                leader_addr = self.peers.get(self.leader_id) if self.leader_id is not None else None
                return {"success": False, "error": "Not leader", "leader_id": self.leader_id, "leader_addr": leader_addr}

        if self.state != STATE_LEADER:
            leader_addr = self.peers.get(self.leader_id) if self.leader_id is not None else None
            return {"success": False, "error": "Not leader", "leader_id": self.leader_id, "leader_addr": leader_addr}

        client_id = payload.get("client_id")
        seq = payload.get("seq")
        if client_id and seq:
            session = self.client_sessions.get(client_id)
            if session and session["seq"] >= seq:
                return session["response"]

        op_names = {OP_SET: "SET", OP_DELETE: "DELETE", OP_APPEND: "APPEND"}
        cmd = {
            "op": op_names[opcode],
            "key": payload.get("key"),
            "value": payload.get("value"),
            "client_id": client_id,
            "seq": seq
        }

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
            self._persist_meta()

        vote_granted = False
        my_last_index = self.last_included_index + len(self.log)
        my_last_term = self._get_log_term(my_last_index)

        log_ok = (last_log_term > my_last_term) or \
                 (last_log_term == my_last_term and last_log_index >= my_last_index)

        if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            vote_granted = True
            self.voted_for = candidate_id
            self._persist_meta()
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
            self._persist_meta()

        self.leader_id = leader_id
        self.last_heartbeat_time = time.time()

        my_last_idx = self.last_included_index + len(self.log)
        if prev_log_index > my_last_idx:
            return {"term": self.current_term, "success": False, "conflict_index": my_last_idx + 1, "conflict_term": None}

        if prev_log_index > 0 and prev_log_index > self.last_included_index:
            my_term_at_prev = self._get_log_term(prev_log_index)
            if my_term_at_prev != prev_log_term:
                conflict_term = my_term_at_prev
                conflict_index = prev_log_index
                while conflict_index > self.last_included_index + 1 and self._get_log_term(conflict_index - 1) == conflict_term:
                    conflict_index -= 1
                return {"term": self.current_term, "success": False, "conflict_index": conflict_index, "conflict_term": conflict_term}

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

    def _handle_install_snapshot(self, payload: dict) -> dict:
        term = payload["term"]
        if term < self.current_term:
            return {"term": self.current_term}

        if term > self.current_term:
            self.current_term = term
            self.state = STATE_FOLLOWER
            self.voted_for = None
            self._persist_meta()

        self.leader_id = payload["leader_id"]
        self.last_heartbeat_time = time.time()

        self.last_included_index = payload["last_included_index"]
        self.last_included_term = payload["last_included_term"]
        self.kv_store = payload["kv_store"]
        self.client_sessions = payload.get("client_sessions", {})
        self.log = []
        self.wal.truncate_and_write([])
        self._save_snapshot()

        self.commit_index = max(self.commit_index, self.last_included_index)
        self.last_applied = max(self.last_applied, self.last_included_index)
        return {"term": self.current_term}

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
                threading.Thread(target=self._send_replication_to_peer, args=(pid, addr), daemon=True).start()

    def _send_replication_to_peer(self, peer_id, peer_addr):
        with self.lock:
            if self.state != STATE_LEADER:
                return
            prev_index = self.next_index.get(peer_id, self.last_included_index + 1) - 1

            if prev_index < self.last_included_index:
                req = {
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "last_included_index": self.last_included_index,
                    "last_included_term": self.last_included_term,
                    "kv_store": self.kv_store,
                    "client_sessions": self.client_sessions
                }
                is_snapshot = True
            else:
                prev_term = self._get_log_term(prev_index)
                entries_to_send = [e for e in self.log if e["index"] > prev_index]
                req = {
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_index,
                    "prev_log_term": prev_term,
                    "entries": entries_to_send,
                    "leader_commit": self.commit_index
                }
                is_snapshot = False

        sock = self.conn_pool.get_connection(peer_addr[0], peer_addr[1])
        if not sock:
            return

        try:
            opcode = OP_RAFT_INSTALL_SNAPSHOT if is_snapshot else OP_RAFT_APPEND_ENTRIES
            send_message(sock, opcode, req)
            _, res = recv_message(sock)
            if not res:
                return

            with self.lock:
                if res.get("term") > self.current_term:
                    self.current_term = res["term"]
                    self.state = STATE_FOLLOWER
                    self.voted_for = None
                    self._persist_meta()
                    return

                if self.state == STATE_LEADER:
                    if is_snapshot:
                        self.match_index[peer_id] = self.last_included_index
                        self.next_index[peer_id] = self.last_included_index + 1
                    else:
                        if res.get("success"):
                            if entries_to_send:
                                last_sent_index = entries_to_send[-1]["index"]
                                self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), last_sent_index)
                                self.next_index[peer_id] = self.match_index[peer_id] + 1
                            self._try_advance_commit_index()
                        else:
                            conflict_index = res.get("conflict_index")
                            if conflict_index:
                                self.next_index[peer_id] = max(1, conflict_index)
                            else:
                                self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)
        except Exception:
            self.conn_pool.close_connection(peer_addr[0], peer_addr[1])

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
            self._persist_meta()
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
            sock = self.conn_pool.get_connection(addr[0], addr[1])
            if not sock:
                return
            req = {
                "term": term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_idx,
                "last_log_term": last_log_term
            }
            try:
                send_message(sock, OP_RAFT_VOTE, req)
                _, res = recv_message(sock)
                if res:
                    with self.lock:
                        if res.get("term") > self.current_term:
                            self.current_term = res["term"]
                            self.state = STATE_FOLLOWER
                            self.voted_for = None
                            self._persist_meta()
                            return
                    if res.get("vote_granted"):
                        with vote_lock:
                            votes += 1
            except Exception:
                self.conn_pool.close_connection(addr[0], addr[1])

        threads = [threading.Thread(target=request_vote, args=(pid, addr)) for pid, addr in self.peers.items()]
        for t in threads: t.start()
        for t in threads: t.join(timeout=0.5)

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
            self._save_snapshot()

class RaftClient:
    def __init__(self, cluster_nodes: list, client_id: str = "client_1"):
        self.cluster_nodes = cluster_nodes
        self.leader_addr = None
        self.client_id = client_id
        self.seq = 0

    def _connect(self, host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect((host, port))
        return s

    def execute(self, opcode: int, payload: dict):
        if opcode != OP_GET:
            self.seq += 1
            payload["client_id"] = self.client_id
            payload["seq"] = self.seq

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
