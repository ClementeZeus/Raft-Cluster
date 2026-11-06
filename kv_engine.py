import threading

class KeyValueEngine:
    def __init__(self):
        self.store = {}
        self.sessions = {}
        self.lock = threading.Lock()

    def execute(self, op: str, key: str, value: str = None, client_id: str = None, seq: int = None) -> dict:
        with self.lock:
            if client_id and seq:
                session = self.sessions.get(client_id)
                if session and session["seq"] >= seq:
                    return session["response"]

            res = {"success": True}
            if op == "SET":
                self.store[key] = value
            elif op == "GET":
                res["value"] = self.store.get(key)
            elif op == "DELETE":
                self.store.pop(key, None)
            elif op == "APPEND":
                self.store[key] = self.store.get(key, "") + value

            if client_id and seq:
                self.sessions[client_id] = {"seq": seq, "response": res}

            return res

    def get_snapshot(self) -> dict:
        with self.lock:
            return {"store": dict(self.store), "sessions": dict(self.sessions)}

    def restore_snapshot(self, snapshot_data: dict):
        with self.lock:
            self.store = dict(snapshot_data.get("store", {}))
            self.sessions = dict(snapshot_data.get("sessions", {}))
