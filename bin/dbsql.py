#!/usr/bin/env python3
"""dbsql: 分布式 SQL 集群的客户端 (纯标准库).

usage:
  dbsql.py exec "INSERT INTO t VALUES (1,'a',30)"   # 写语句: 自动跟随 leader
  dbsql.py query "SELECT * FROM t WHERE age > 20"   # 读语句: 节点内转发给 leader
  dbsql.py shell                                    # 交互式 REPL
  dbsql.py status                                   # 三节点 role/term/leader
"""
import json
import sys
import urllib.request

PORTS = [17001, 17002, 17003]


def req(port, path, data=None, timeout=6.0):
    r = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=(json.dumps(data).encode() if data else None),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def exec_write(sql):
    """写语句: 轮询节点, 跟随 leader hint 重试."""
    order = list(range(3))
    last_err = None
    for _ in range(6):
        for i in order:
            try:
                r = req(PORTS[i], "/exec", {"sql": sql}, timeout=8.0)
            except Exception as e:
                last_err = e
                continue
            if r.get("ok"):
                return r
            if "error" in r and "leader" not in r:
                return r  # 语句本身错误 (语法/主键冲突...), 不重试
            lead = r.get("leader")
            if lead is not None and lead != i:
                order = [lead] + [x for x in order if x != lead]
    return {"ok": False, "error": f"no leader reachable ({last_err})"}


def exec_query(sql):
    for i in range(3):
        try:
            r = req(PORTS[i], "/exec", {"sql": sql}, timeout=8.0)
            if r.get("ok") or "error" in r:
                return r
        except Exception:
            continue
    return {"ok": False, "error": "no node reachable"}


def fmt_value(v):
    return "NULL" if v is None else str(v)


def print_table(columns, rows):
    widths = [len(c) for c in columns]
    str_rows = [[fmt_value(v) for v in r] for r in rows]
    for r in str_rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))
    def line(cells):
        return "| " + " | ".join(c.ljust(widths[i])
                                 for i, c in enumerate(cells)) + " |"
    print(line(columns))
    print("|-" + "-|-".join("-" * w for w in widths) + "-|")
    for r in str_rows:
        print(line(r))
    print(f"({len(rows)} rows)")


def show_result(r):
    if not r.get("ok"):
        print(f"ERROR: {r.get('error')}")
        return False
    if "columns" in r:
        print_table(r["columns"], r["rows"])
        extra = []
        if r.get("forwarded_to_leader") is not None:
            extra.append(f"forwarded to leader node {r['forwarded_to_leader']}")
        if r.get("stale_read_possible"):
            extra.append(f"served locally by node {r.get('served_by')} "
                         f"({r.get('role')}, may be slightly stale)")
        if extra:
            print("[" + "; ".join(extra) + "]")
    else:
        n = r.get("rows_affected")
        if n is not None:
            print(f"OK ({n} rows affected)")
        else:
            print("OK")
    return True


def status():
    for i in range(3):
        try:
            s = req(PORTS[i], "/status", timeout=2.0)
            print(f"node {i}: role={s['role']} term={s['term']} "
                  f"leader={s['leader']} commit={s['commit_index']} "
                  f"log_len={s['log_len']}")
        except Exception as e:
            print(f"node {i}: unreachable ({e})")


def shell():
    print("dbsql shell -- 输入 SQL, 空行执行; QUIT 退出.")
    print("提示: 写语句自动找 leader, 读语句节点会转发.")
    buf = []
    while True:
        try:
            line = input("dbsql> " if not buf else "  ...> ")
        except EOFError:
            print()
            break
        if not buf and line.strip().upper() in ("QUIT", "EXIT"):
            break
        buf.append(line)
        if line.strip().endswith(";") or not line.strip():
            sql = "\n".join(buf).strip()
            buf = []
            if not sql:
                continue
            is_select = sql.lstrip().upper().startswith("SELECT")
            r = exec_query(sql) if is_select else exec_write(sql)
            show_result(r)


def exec_on_node(node_id, sql):
    """直接打到指定节点 (不跟随 leader; 供测试/演示点名验证)."""
    try:
        return req(PORTS[node_id], "/exec", {"sql": sql}, timeout=8.0)
    except Exception as e:
        return {"ok": False, "error": str(e)}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    cmd = sys.argv[1]
    # 可选: --node N 指定目标节点 (如 dbsql.py --node 1 query "SELECT ...")
    node = None
    args = sys.argv[1:]
    if args[0] == "--node" and len(args) >= 4:
        node = int(args[1])
        args = [args[2], args[3]]
    cmd = args[0]
    if cmd == "exec" and len(args) == 2:
        r = exec_on_node(node, args[1]) if node is not None else exec_write(args[1])
        sys.exit(0 if show_result(r) else 1)
    elif cmd == "query" and len(args) == 2:
        r = exec_on_node(node, args[1]) if node is not None else exec_query(args[1])
        sys.exit(0 if show_result(r) else 1)
    elif cmd == "shell":
        shell()
    elif cmd == "status":
        status()
    else:
        print(__doc__)
        sys.exit(1)
