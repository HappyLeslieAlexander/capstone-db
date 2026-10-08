#!/usr/bin/env python3
"""分布式 SQL 节点: Raft(复制日志) + MiniDB(本地状态机).

复用: DBNode 继承 vendored 的 RaftNode, Raft 核心 (选主/日志复制/提交/持久化)
一字未动, 只重写两个状态机钩子:
  _apply_entry(i): 日志条目 {"op": "sql", "stmt": plan} -> TableStore.apply_committed
  _load(): 先打开本地存储读出 applied 游标, 只重放游标之后的已提交条目.

HTTP: 继承基类 Handler 的 /status /debug/log /control, 新增 POST /exec.
端口: 17001/17002/17003 (与上游 skill 的 16001-16003 错开, 互不干扰).

  usage: db_node.py <node_id 0|1|2> [--data-dir DIR]
"""
import faulthandler
import json
import os
import signal
import sys
import threading
import time
from http.server import ThreadingHTTPServer

# 运维钩子: kill -USR1 <pid> 即把全线程栈打到 stderr (节点 hang 时诊断用)
faulthandler.register(signal.SIGUSR1)

SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SRC)
sys.path.insert(0, os.path.join(SRC, "vendor", "raft"))

import raft_node
from raft_node import RaftNode, Handler, HEARTBEAT_INTERVAL, CLIENT_PUT_TIMEOUT

# 端口与上游 skill 错开
raft_node.PEERS = {0: 17001, 1: 17002, 2: 17003}
raft_node.BASE_PORT = 17001

from sql.executor import prepare, query, SQLError  # noqa: E402
from storage.store import TableStore  # noqa: E402


def log(*a):
    print(f"[{time.strftime('%H:%M:%S')}.{int(time.time()*1000)%1000:03d}]", *a,
          flush=True)


class DBNode(RaftNode):
    """Raft 节点, 状态机 = 本地 MiniDB 表存储."""

    # ---------- 状态机钩子 ----------
    def _load(self):
        # 先开本地存储: MiniDB 自带 WAL 恢复, 恢复后读出 applied 游标
        self.store = TableStore(os.path.join(self.data_dir, f"db-{self.id}"))
        self.apply_results = {}
        applied = self.store.get_applied_index()
        if os.path.exists(self.persist_path):
            with open(self.persist_path) as f:
                d = json.load(f)
            self.current_term = d["term"]
            self.voted_for = d["voted_for"]
            self.log = d["log"]
            self.commit_index = d.get("commit_index", 0)
            # 只重放 applied 之后、已提交的条目: 已 apply 的不再执行
            # (幂等性不靠"语句可重入", 靠 applied 游标; 语句与游标同事务提交)
            for i in range(applied + 1, self.commit_index + 1):
                self._apply_entry(i)
            # applied 可能 > commit_index: store 事务已提交、Raft 的 _save
            # 还没落盘就 crash. 取较大者, 避免重启后重复 apply 同一条目.
            self.last_applied = max(self.commit_index, applied)
            log(f"node {self.id}: recovered term={self.current_term} "
                f"log_len={len(self.log)-1} commit={self.commit_index} "
                f"applied={applied}")
        else:
            log(f"node {self.id}: fresh start")

    def _apply_entry(self, i):
        op = self.log[i].get("op")
        if isinstance(op, dict) and op.get("op") == "sql":
            result = self.store.apply_committed(i, op["stmt"])
            self.apply_results[i] = result
            # apply_results 只给刚提交的客户端读结果用, 定期裁剪防膨胀
            # (Raft log 本身无压缩, 见 README 诚实标注)
            if len(self.apply_results) > 2048:
                for k in sorted(self.apply_results)[:1024]:
                    del self.apply_results[k]

    # ---------- 写路径: SQL -> plan -> Raft 日志 ----------
    def client_exec(self, plan):
        """把 plan 追加为 Raft 日志条目, 等待提交+apply, 返回执行结果."""
        with self.lock:
            if self.role != "leader":
                return {"ok": False, "leader": self.leader_id}
            idx = self._last_log_index() + 1
            self.log.append({"term": self.current_term,
                             "op": {"op": "sql", "stmt": plan}})
            self._save()
        threading.Thread(target=self._replicate_all, daemon=True).start()
        deadline = time.time() + CLIENT_PUT_TIMEOUT
        while time.time() < deadline:
            with self.lock:
                if self.last_applied >= idx:
                    res = self.apply_results.get(idx, {"ok": True})
                    if res.get("ok"):
                        out = {"ok": True, "leader": self.id}
                        for k, v in res.items():
                            if k != "ok":
                                out[k] = v
                        return out
                    # 语句级失败 (如主键冲突): 终端错误, 不带 leader hint,
                    # 客户端看到 "error" 且无 "leader" 即知不应重试
                    return {"ok": False,
                            "error": res.get("error", "statement failed")}
                if self.role != "leader":
                    return {"ok": False, "leader": self.leader_id}
            time.sleep(0.02)
        return {"ok": False, "leader": self.leader_id, "error": "commit timeout"}

    def exec_sql(self, sql_text, forwarded=False):
        """入口: 解析 -> SELECT 走读路径, 写语句走 Raft. 不在持锁时调 client_exec."""
        with self.lock:
            txn = self.store.db.begin("RC")
            try:
                plan = prepare(sql_text,
                               lambda t: self.store.get_schema(txn, t))
            except SQLError as e:
                return {"ok": False, "error": str(e)}
            finally:
                txn.abort()
        if plan["kind"] == "select":
            return self._read_path(sql_text, forwarded)
        return self.client_exec(plan)

    # ---------- 读路径: 本地读, follower 尽量转发给 leader ----------
    def _read_path(self, sql_text, forwarded=False):
        with self.lock:
            target = None
            if self.role != "leader" and not forwarded \
                    and self.leader_id is not None:
                target = self.leader_id
        if target is not None:
            resp = self._rpc(target, "/exec",
                             {"sql": sql_text, "forwarded": True})
            if isinstance(resp, dict) and resp.get("ok"):
                resp["forwarded_to_leader"] = target
                return resp
            # 转发失败 (leader 失联/假 leader): 降级为本地读
        with self.lock:
            try:
                result = query(self.store, sql_text)
            except SQLError as e:
                return {"ok": False, "error": str(e)}
            result["served_by"] = self.id
            result["role"] = self.role
            # 诚实标注: 未实现 linearizable read, 本地读可能略旧
            # (与上游 Raft skill 一致; 转发给 leader 只能减小、不能消除)
            result["stale_read_possible"] = True
            return result


class DBHandler(Handler):
    node = None

    def do_POST(self):
        if self.path == "/exec":
            try:
                b = self._read_json()
                result = self.node.exec_sql(b.get("sql", ""),
                                            forwarded=bool(b.get("forwarded")))
                self._json(result)
            except BrokenPipeError:
                pass
            except Exception as e:  # noqa: BLE001 -- 顶层兜底, 节点不死
                self._json({"ok": False, "error": f"internal: {e}"}, 500)
        else:
            super().do_POST()


def main():
    node_id = int(sys.argv[1])
    if len(sys.argv) > 3 and sys.argv[2] == "--data-dir":
        data_dir = os.path.abspath(sys.argv[3])
    else:
        data_dir = os.path.abspath(os.path.join(SRC, "..", "var", "data"))

    node = DBNode(node_id, data_dir)
    DBHandler.node = node
    srv = ThreadingHTTPServer(("127.0.0.1", node.port), DBHandler)

    def election_loop():
        while True:
            time.sleep(0.01)
            with node.lock:
                if node.role != "leader" and time.time() >= node._election_deadline:
                    threading.Thread(target=node.start_election,
                                     daemon=True).start()
                    node._reset_election_deadline()

    def heartbeat_loop():
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with node.lock:
                is_leader = node.role == "leader"
            if is_leader:
                node._replicate_all()

    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    log(f"db-node {node_id}: listening on 127.0.0.1:{node.port}, data={data_dir}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
