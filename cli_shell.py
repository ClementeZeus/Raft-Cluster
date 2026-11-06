import os
import sys
import time
import threading
from raft_cluster import RaftNode, RaftClient, OP_SET, OP_GET, OP_DELETE, OP_APPEND

class ClusterManager:
    def __init__(self):
        self.nodes = {}
        self.configs = {
            1: ("127.0.0.1", 8001, {2: ("127.0.0.1", 8002), 3: ("127.0.0.1", 8003)}, "node1.wal"),
            2: ("127.0.0.1", 8002, {1: ("127.0.0.1", 8001), 3: ("127.0.0.1", 8003)}, "node2.wal"),
            3: ("127.0.0.1", 8003, {1: ("127.0.0.1", 8001), 2: ("127.0.0.1", 8002)}, "node3.wal"),
        }
        self.client_addrs = [("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)]
        self.client = RaftClient(self.client_addrs)

    def start_all(self):
        for nid, (host, port, peers, wal) in self.configs.items():
            node = RaftNode(nid, host, port, peers, wal)
            node.start()
            self.nodes[nid] = node

    def stop_all(self):
        for node in self.nodes.values():
            node.stop()

    def kill_node(self, nid: int):
        if nid in self.nodes:
            self.nodes[nid].stop()
            return True
        return False

    def restart_node(self, nid: int):
        if nid in self.nodes:
            self.nodes[nid].stop()
            host, port, peers, wal = self.configs[nid]
            new_node = RaftNode(nid, host, port, peers, wal)
            new_node.start()
            self.nodes[nid] = new_node
            return True
        return False

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def render_dashboard(manager: ClusterManager):
    clear_screen()
    print("┌────────────────────────────────────────────────────────────────────────┐")
    print("│                  DISTRIBUTED RAFT KV-STORE CONTROL PANEL               │")
    print("├────────────────────────────────────────────────────────────────────────┤")
    print(f"│ {'ID':<4} │ {'HOST:PORT':<15} │ {'STATE':<10} │ {'TERM':<6} │ {'COMMIT':<8} │ {'KEYS':<6} │")
    print("├──────┼─────────────────┼────────────┼────────┼──────────┼──────────┤")
    
    for nid, node in sorted(manager.nodes.items()):
        with node.lock:
            state = node.state
            term = node.current_term
            commit = node.commit_index
            kcount = len(node.kv_store)
            running = node.running
        
        if not running:
            state = "OFFLINE"
        
        host_port = f"{node.host}:{node.port}"
        print(f"│ #{nid:<3} │ {host_port:<15} │ {state:<10} │ {term:<6} │ {commit:<8} │ {kcount:<6} │")
    
    print("└──────┴─────────────────┴────────────┴────────┴──────────┴──────────┘")
    print(" COMMANDS:                                                               ")
    print("   set <key> <val>     - Write key-value pair                            ")
    print("   get <key>           - Retrieve value (ReadIndex linearizable)         ")
    print("   del <key>           - Delete key                                      ")
    print("   app <key> <val>     - Append string to key                            ")
    print("   kill <node_id>      - Simulate node crash / network isolation         ")
    print("   start <node_id>     - Restart offline node                            ")
    print("   refresh             - Redraw dashboard                                ")
    print("   exit                - Terminate cluster and exit                      ")
    print("─────────────────────────────────────────────────────────────────────────")

def main():
    manager = ClusterManager()
    manager.start_all()
    time.sleep(1.5)

    while True:
        render_dashboard(manager)
        try:
            cmd_line = input("RAFT-CLI> ").strip()
        except (KeyboardInterrupt, EOFError):
            break

        if not cmd_line:
            continue

        parts = cmd_line.split(" ", 2)
        action = parts[0].lower()

        if action == "exit":
            print("\nShutting down cluster nodes...")
            manager.stop_all()
            break
        elif action == "refresh":
            continue
        elif action == "kill":
            if len(parts) > 1 and parts[1].isdigit():
                nid = int(parts[1])
                success = manager.kill_node(nid)
                print(f"Node #{nid} shutdown: {success}")
            else:
                print("Usage: kill <node_id>")
            time.sleep(1)
        elif action == "start":
            if len(parts) > 1 and parts[1].isdigit():
                nid = int(parts[1])
                success = manager.restart_node(nid)
                print(f"Node #{nid} started: {success}")
            else:
                print("Usage: start <node_id>")
            time.sleep(1)
        elif action == "set":
            if len(parts) == 3:
                key, val = parts[1], parts[2]
                res = manager.client.execute(OP_SET, {"key": key, "value": val})
                print(f"RESULT: {res}")
            else:
                print("Usage: set <key> <value>")
            input("\nPress Enter to continue...")
        elif action == "get":
            if len(parts) == 2:
                key = parts[1]
                res = manager.client.execute(OP_GET, {"key": key})
                print(f"RESULT: {res}")
            else:
                print("Usage: get <key>")
            input("\nPress Enter to continue...")
        elif action == "del":
            if len(parts) == 2:
                key = parts[1]
                res = manager.client.execute(OP_DELETE, {"key": key})
                print(f"RESULT: {res}")
            else:
                print("Usage: del <key>")
            input("\nPress Enter to continue...")
        elif action == "app":
            if len(parts) == 3:
                key, val = parts[1], parts[2]
                res = manager.client.execute(OP_APPEND, {"key": key, "value": val})
                print(f"RESULT: {res}")
            else:
                print("Usage: app <key> <value>")
            input("\nPress Enter to continue...")
        else:
            print(f"Unknown command: {action}")
            time.sleep(1)

if __name__ == "__main__":
    main()
