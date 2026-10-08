#!/usr/bin/env python3
"""
Raft node (3.2 distributed-systems, Phase 3).

Independent Raft implementation in Python, stdlib only.
One process per node; nodes talk to each other via HTTP JSON RPC.

  usage: raft_node.py <node_id 0|1|2> [--data-dir DIR]

Ports: id 0 -> 16001, 1 -> 16002, 2 -> 16003 (localhost).

Implements Raft per Figure 2 of Ongaro & Ousterhout
("In Search of an Understandable Consensus Algorithm"):
  - leader election with randomized timeouts
  - log replication with consistency check + conflict truncation
  - commit advancement (leader: majority matchIndex on current-term entries)
  - state machine = simple KV dict (idempotent `set` ops)
  - persistence of (currentTerm, votedFor, log, commitIndex) to JSON file,
    atomically (write temp + rename); state machine rebuilt by replay on start.

Honest limitations (also listed in the skill notes):
  * No log compaction / snapshots.
  * No cluster membership changes.
  * Reads (get) are local and may be stale after a leadership change
    (no linearizable read / read-index implemented).
  * Network partition is simulated with an in-code message-drop switch
    (POST /control {"drop":[peer_ids]}) because the sandbox kernel lacks
    the xt_tcp module, so iptables DROP rules never actually take effect
    (verified: `iptables -S` shows no rule after `-A`).
"""
import json
import os
import random
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE_PORT = 16001
PEERS = {0: 16001, 1: 16002, 2: 16003}
N = len(PEERS)
MAJORITY = N // 2 + 1

ELECTION_TIMEOUT_MIN = 0.150  # seconds
ELECTION_TIMEOUT_MAX = 0.300
HEARTBEAT_INTERVAL = 0.050
RPC_TIMEOUT = 0.25
CLIENT_PUT_TIMEOUT = 3.0

LOG_PREFIX_LEN = 1  # log[0] is a dummy entry (term 0); real entries start at index 1


def log(*a):
    print(f"[{time.strftime('%H:%M:%S')}.{int(time.time()*1000)%1000:03d}]", *a,
          flush=True)


class RaftNode:
    def __init__(self, node_id, data_dir):
        self.id = node_id
        self.port = PEERS[node_id]
        self.data_dir = data_dir
        self.persist_path = os.path.join(data_dir, f"node-{node_id}.json")
        self.lock = threading.RLock()

        # ---- persistent state ----
        self.current_term = 0
        self.voted_for = None
        self.log = [{"term": 0, "op": None}]  # index 0 dummy
        self.commit_index = 0

        # ---- volatile state ----
        self.role = "follower"          # follower | candidate | leader
        self.votes = 0
        self.leader_id = None
        self.next_index = {}            # leader: next log index to send per peer
        self.match_index = {}           # leader: highest replicated index per peer
        self.last_applied = 0
        self.kv = {}                    # state machine
        self.drop = set()               # partition simulation: peer ids to drop msgs to/from
        self._election_deadline = time.time() + self._rand_timeout()

        os.makedirs(data_dir, exist_ok=True)
        self._load()

    # ---------------- persistence ----------------
    def _save(self):
        tmp = self.persist_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"term": self.current_term, "voted_for": self.voted_for,
                       "log": self.log, "commit_index": self.commit_index}, f)
        os.replace(tmp, self.persist_path)

    def _load(self):
        if not os.path.exists(self.persist_path):
            return
        with open(self.persist_path) as f:
            d = json.load(f)
        self.current_term = d["term"]
        self.voted_for = d["voted_for"]
        self.log = d["log"]
        self.commit_index = d.get("commit_index", 0)
        # rebuild state machine by replaying committed prefix (set ops idempotent)
        for i in range(1, self.commit_index + 1):
            self._apply_entry(i)
        self.last_applied = self.commit_index
        log(f"node {self.id}: recovered term={self.current_term} "
            f"log_len={len(self.log)-1} commit={self.commit_index}")

    # ---------------- helpers ----------------
    def _rand_timeout(self):
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _reset_election_deadline(self):
        self._election_deadline = time.time() + self._rand_timeout()

    def _last_log_index(self):
        return len(self.log) - 1

    def _last_log_term(self):
        return self.log[-1]["term"]

    def _become_follower(self, term):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.role = "follower"
        self.leader_id = None
        self._reset_election_deadline()
        self._save()

    def _rpc(self, peer_id, path, payload):
        """POST JSON to a peer. Returns parsed response dict or None on
        failure / dropped (partition simulation)."""
        if peer_id in self.drop:
            return None  # simulated partition: message dropped
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{PEERS[peer_id]}{path}",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=RPC_TIMEOUT) as r:
                return json.loads(r.read().decode())
        except Exception:
            return None

    # ---------------- RequestVote ----------------
    def handle_request_vote(self, req):
        if req.get("sender") in self.drop:
            return None  # drop inbound from partitioned peer
        with self.lock:
            term = req["term"]
            if term < self.current_term:
                return {"term": self.current_term, "granted": False}
            if term > self.current_term:
                self._become_follower(term)
            cand = req["candidate"]
            last_term, last_idx = req["last_log_term"], req["last_log_index"]
            up_to_date = (last_term > self._last_log_term() or
                          (last_term == self._last_log_term() and
                           last_idx >= self._last_log_index()))
            if (self.voted_for is None or self.voted_for == cand) and up_to_date:
                self.voted_for = cand
                self._save()
                self._reset_election_deadline()  # granted a vote -> reset
                log(f"node {self.id}: voted for {cand} in term {self.current_term}")
                return {"term": self.current_term, "granted": True}
            return {"term": self.current_term, "granted": False}

    # ---------------- AppendEntries ----------------
    def handle_append_entries(self, req):
        if req.get("sender") in self.drop:
            return None
        with self.lock:
            term = req["term"]
            if term < self.current_term:
                return {"term": self.current_term, "success": False}
            if term > self.current_term:
                self._become_follower(term)
            # valid AppendEntries from current leader -> reset election timer
            self.leader_id = req["leader"]
            self._reset_election_deadline()

            pli, plt = req["prev_log_index"], req["prev_log_term"]
            if pli > self._last_log_index():
                return {"term": self.current_term, "success": False}
            if self.log[pli]["term"] != plt:
                return {"term": self.current_term, "success": False}
            # append new entries; truncate on conflict (Figure 2, exact rule:
            # only delete when an existing entry conflicts)
            idx = pli + 1
            for e in req["entries"]:
                if idx <= self._last_log_index():
                    if self.log[idx]["term"] != e["term"]:
                        self.log = self.log[:idx]
                        self.log.append(e)
                else:
                    self.log.append(e)
                idx += 1
            self._save()
            if req["leader_commit"] > self.commit_index:
                self.commit_index = min(req["leader_commit"], self._last_log_index())
                self._apply_new()
            return {"term": self.current_term, "success": True}

    def _apply_entry(self, i):
        op = self.log[i].get("op")
        if isinstance(op, dict) and op.get("op") == "set":
            self.kv[op["key"]] = op["value"]

    def _apply_new(self):
        while self.last_applied < self.commit_index:
            # CAPSTONE: 先 apply, 成功后才推进 last_applied.
            # 若 _apply_entry 抛异常 (如存储层报错), 停在此处不推进:
            # 重启后会从 applied 游标重放, 而不是静默跳过这条已提交日志.
            # (2026-10-09 实测: 曾因先+1后apply, 失败的条目被永久跳过.)
            self._apply_entry(self.last_applied + 1)
            self.last_applied += 1

    # ---------------- election ----------------
    def start_election(self):
        with self.lock:
            self.current_term += 1
            self.voted_for = self.id
            self.role = "candidate"
            self.leader_id = None
            self._save()
            self._reset_election_deadline()
            term = self.current_term
            last_term, last_idx = self._last_log_term(), self._last_log_index()
            log(f"node {self.id}: starting election term {term}")
        votes = 1
        for p in PEERS:
            if p == self.id:
                continue
            resp = self._rpc(p, "/request_vote", {
                "sender": self.id, "term": term, "candidate": self.id,
                "last_log_term": last_term, "last_log_index": last_idx})
            if resp is None:
                continue
            with self.lock:
                if resp["term"] > self.current_term:
                    self._become_follower(resp["term"])
                    return
                if self.role != "candidate" or self.current_term != term:
                    return  # stepped down / newer election meanwhile
                if resp.get("granted"):
                    votes += 1
                    if votes >= MAJORITY:
                        self._become_leader()
                        return

    def _become_leader(self):
        self.role = "leader"
        self.leader_id = self.id
        last = self._last_log_index()
        for p in PEERS:
            if p != self.id:
                self.next_index[p] = last + 1
                self.match_index[p] = 0
        log(f"node {self.id}: became LEADER term {self.current_term}")
        # send immediate heartbeats so followers learn fast
        threading.Thread(target=self._replicate_all, daemon=True).start()

    # ---------------- replication ----------------
    def _replicate_to(self, peer):
        with self.lock:
            if self.role != "leader":
                return
            term = self.current_term
            ni = self.next_index.get(peer, 1)
            prev_idx = ni - 1
            prev_term = self.log[prev_idx]["term"] if prev_idx <= self._last_log_index() else 0
            entries = [dict(e) for e in self.log[ni:]]
            commit = self.commit_index
        resp = self._rpc(peer, "/append_entries", {
            "sender": self.id, "term": term, "leader": self.id,
            "prev_log_index": prev_idx, "prev_log_term": prev_term,
            "entries": entries, "leader_commit": commit})
        if resp is None:
            return
        with self.lock:
            if resp["term"] > self.current_term:
                self._become_follower(resp["term"])
                return
            if self.role != "leader" or self.current_term != term:
                return
            if resp.get("success"):
                self.match_index[peer] = prev_idx + len(entries)
                self.next_index[peer] = self.match_index[peer] + 1
                self._advance_commit()
            else:
                self.next_index[peer] = max(1, self.next_index.get(peer, 1) - 1)

    def _replicate_all(self):
        threads = []
        for p in PEERS:
            if p != self.id:
                t = threading.Thread(target=self._replicate_to, args=(p,), daemon=True)
                t.start()
                threads.append(t)
        for t in threads:
            t.join(timeout=RPC_TIMEOUT + 0.2)

    def _advance_commit(self):
        # leader rule: commit highest N with majority matchIndex and log[N].term == currentTerm
        for i in range(self._last_log_index(), self.commit_index, -1):
            if self.log[i]["term"] != self.current_term:
                continue
            n = 1 + sum(1 for p in self.match_index if self.match_index[p] >= i)
            if n >= MAJORITY:
                self.commit_index = i
                self._save()
                self._apply_new()
                break

    # ---------------- client API ----------------
    def client_put(self, key, value):
        with self.lock:
            if self.role != "leader":
                return {"ok": False, "leader": self.leader_id}
            idx = self._last_log_index() + 1
            self.log.append({"term": self.current_term,
                             "op": {"op": "set", "key": key, "value": value}})
            self._save()
            # note: leader counts itself via the `1 +` in _advance_commit;
            # do NOT insert self.id into match_index (would double-count).
        threading.Thread(target=self._replicate_all, daemon=True).start()
        deadline = time.time() + CLIENT_PUT_TIMEOUT
        while time.time() < deadline:
            with self.lock:
                if self.last_applied >= idx:
                    return {"ok": True, "leader": self.id}
                if self.role != "leader":
                    return {"ok": False, "leader": self.leader_id}
            time.sleep(0.02)
        return {"ok": False, "leader": self.leader_id, "error": "commit timeout"}

    def client_get(self, key):
        with self.lock:
            # NOTE: local read; may be stale briefly after leadership change.
            return {"ok": True, "value": self.kv.get(key),
                    "leader": self.leader_id, "role": self.role}


class Handler(BaseHTTPRequestHandler):
    node = None

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n).decode() or "{}")

    def do_POST(self):
        try:
            if self.path == "/request_vote":
                r = self.node.handle_request_vote(self._read_json())
                self._json(r or {"term": -1, "granted": False, "dropped": True})
            elif self.path == "/append_entries":
                r = self.node.handle_append_entries(self._read_json())
                self._json(r or {"term": -1, "success": False, "dropped": True})
            elif self.path == "/put":
                b = self._read_json()
                self._json(self.node.client_put(b["key"], b["value"]))
            elif self.path == "/control":
                b = self._read_json()
                with self.node.lock:
                    if "drop" in b:
                        self.node.drop = set(b["drop"])
                self._json({"ok": True, "drop": sorted(self.node.drop)})
            else:
                self._json({"error": "unknown path"}, 404)
        except BrokenPipeError:
            pass

    def do_GET(self):
        try:
            if self.path == "/status":
                nd = self.node
                with nd.lock:
                    self._json({"id": nd.id, "role": nd.role, "term": nd.current_term,
                                "leader": nd.leader_id, "commit_index": nd.commit_index,
                                "last_applied": nd.last_applied,
                                "log_len": len(nd.log) - 1, "kv_size": len(nd.kv),
                                "drop": sorted(nd.drop)})
            elif self.path == "/kv":
                with self.node.lock:
                    self._json({"kv": dict(self.node.kv),
                                "commit_index": self.node.commit_index})
            elif self.path.startswith("/get"):
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(self.path).query)
                key = q.get("key", [""])[0]
                self._json(self.node.client_get(key))
            elif self.path.startswith("/debug/log"):
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(self.path).query)
                n = int(q.get("last", ["5"])[0])
                with self.node.lock:
                    tail = [{"index": i, "term": e["term"], "op": e["op"]}
                            for i, e in enumerate(self.node.log[-n:], start=len(self.node.log) - n)]
                self._json({"entries": tail})
            else:
                self._json({"error": "unknown path"}, 404)
        except BrokenPipeError:
            pass

    def log_message(self, *a):
        pass  # keep stderr for Raft event log


def main():
    node_id = int(sys.argv[1])
    data_dir = sys.argv[3] if len(sys.argv) > 3 and sys.argv[2] == "--data-dir" else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "var", "data")
    data_dir = os.path.abspath(data_dir)

    node = RaftNode(node_id, data_dir)
    Handler.node = node
    srv = ThreadingHTTPServer(("127.0.0.1", node.port), Handler)

    def election_loop():
        while True:
            time.sleep(0.01)
            with node.lock:
                if node.role != "leader" and time.time() >= node._election_deadline:
                    threading.Thread(target=node.start_election, daemon=True).start()
                    node._reset_election_deadline()  # avoid tight loop; start_election resets anyway
            # NOTE: election work runs in its own thread; deadline re-armed here
            # only to keep the loop from spinning while the election RPCs fly.

    def heartbeat_loop():
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with node.lock:
                is_leader = node.role == "leader"
            if is_leader:
                node._replicate_all()

    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    log(f"node {node_id}: listening on 127.0.0.1:{node.port}, data={data_dir}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
