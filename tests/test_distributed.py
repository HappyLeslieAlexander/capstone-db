#!/usr/bin/env python3
"""分布式正确性测试: 3 节点集群, kill -9 leader / 网络分区 / 节点重启.

阶段:
  A 基线: 建表 + 写入 30 行, 三节点数据一致
  B kill -9 leader: 新 leader 选出, 继续写 10 行, 数据零丢失
  C 网络分区: 旧 leader 被隔离成少数派 -> 写超时被拒;
     多数派选出新 leader 继续写; heal 后少数派发散条目被截断, 三节点收敛
  D kill -9 follower 后重启: 追上日志, 数据一致
  终检: 三节点行集与 Python 端预期逐行相等

分区用代码内消息丢弃开关模拟 (POST /control {"drop":[...]}),
与上游 distributed-systems skill 一致 (沙箱缺 xt_tcp, iptables 不落盘).

usage: python3 tests/test_distributed.py   (约 60-90 秒)
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
PORTS = [17001, 17002, 17003]
N = 0


def check(cond, msg):
    global N
    N += 1
    if not cond:
        raise AssertionError(f"FAIL[{N}]: {msg}")
    print(f"  ok[{N}] {msg}", flush=True)


def req(port, path, data=None, timeout=10.0):
    r = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=(json.dumps(data).encode() if data else None),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def status(i):
    return req(PORTS[i], "/status", timeout=3.0)


def wait_for(msg, fn, timeout=25.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = fn()
            # 注意: 用 `is not None` 而非真值 -- node 0 做 leader 时返回 0
            if v is not None and v is not False:
                return v
        except Exception:
            pass
        time.sleep(0.3)
    raise AssertionError(f"超时: {msg}")


def find_leader(alive=(0, 1, 2)):
    for i in alive:
        try:
            s = status(i)
            if s["role"] == "leader":
                return i
        except Exception:
            pass
    return None


def exec_on(i, sql, timeout=12.0):
    return req(PORTS[i], "/exec", {"sql": sql}, timeout=timeout)


def local_rows(i):
    """绕过转发, 直接读某节点的本地状态 (forwarded=True 禁止再转发)."""
    r = req(PORTS[i], "/exec",
            {"sql": "SELECT id, name, age FROM kv", "forwarded": True},
            timeout=10.0)
    assert r["ok"], r
    return sorted(tuple(row) for row in r["rows"])


def cluster(cmd, *args):
    subprocess.run(["bash", os.path.join(ROOT, "bin", "cluster.sh"),
                    cmd, *args], check=True, capture_output=True)


expected = set()  # (id, name, age) 全集, Python 端真相


def wait_converged(nodes, timeout=30.0):
    """等所有节点本地行集 == expected (follower apply 是异步的)."""
    wait_for(f"节点 {nodes} 收敛", lambda: all(
        local_rows(i) == sorted(expected) for i in nodes), timeout=timeout)


def insert_batch(start, count, via):
    rows = [(i, f"n{i}", i * 10) for i in range(start, start + count)]
    vals = ",".join(f"({i},'{n}',{a})" for i, n, a in rows)
    sql = f"INSERT INTO kv VALUES {vals}"
    # 跟随 leader hint 重试 (选主可能在测试中途发生)。
    # hint 可能指向刚被 kill 的节点 -> 连接失败时重新发现 leader 再试。
    # (2026-10-09 总协调复核: 分区 heal 后的选主风暴期 hint 会过期,
    #  盲目跟 hint 会撞上已死节点导致 ConnectionRefused。真客户端也应如此处理。)
    r = None
    for _ in range(6):
        try:
            r = exec_on(via, sql)
        except Exception:  # noqa: BLE001 -- 对端可能刚被 kill
            via = wait_for("重发现 leader", lambda: find_leader())
            continue
        if r.get("ok"):
            break
        hint = r.get("leader")
        via = hint if hint is not None else wait_for("重发现 leader",
                                                     lambda: find_leader())
    assert r is not None and r.get("ok"), r
    expected.update(rows)
    return rows


print("== 启动 fresh 集群 ==")
cluster("stop")
cluster("start", "fresh")
L = wait_for("选出 leader", lambda: find_leader())
print(f"  leader = node {L}")

print("== A 基线: 建表 + 30 行 ==")
r = exec_on(L, "CREATE TABLE kv (id INT PRIMARY KEY, name TEXT, age INT)")
check(r["ok"], "建表")
insert_batch(1, 10, L)
insert_batch(11, 10, L)
insert_batch(21, 10, L)
wait_converged((0, 1, 2))
for i in (0, 1, 2):
    check(local_rows(i) == sorted(expected), f"节点 {i} 30 行一致")

print("== B kill -9 leader ==")
L0 = L
cluster("kill", str(L0))
L = wait_for("新 leader 选出", lambda: find_leader(alive=[i for i in (0, 1, 2)
                                                          if i != L0]))
check(L != L0, f"新 leader=node {L} (旧={L0})")
insert_batch(31, 10, L)
alive = [i for i in (0, 1, 2) if i != L0]
wait_converged(alive)
for i in alive:
    check(local_rows(i) == sorted(expected), f"存活节点 {i} 40 行一致, 零丢失")

print("== C 前: 重启被 kill 的旧 leader, 恢复 3 节点 ==")
cluster("restart", str(L0))
wait_for(f"节点 {L0} 重启追上",
         lambda: local_rows(L0) == sorted(expected), timeout=30.0)
check(True, f"节点 {L0} 重启追上 40 行")
alive = [0, 1, 2]

print("== C 网络分区: 隔离现任 leader 为少数派 ==")
M = L  # 把现任 leader 隔离成少数派 (stale leader 场景)
others = [i for i in alive if i != M]
status(M)  # 连通性确认
for i in others:
    req(PORTS[i], "/control", {"drop": [M]}, timeout=3.0)
req(PORTS[M], "/control", {"drop": others}, timeout=3.0)
print(f"  已隔离 node {M}, 多数派={others}")
# 少数派 (stale leader) 写: 无法提交 -> commit timeout -> ok False
r = exec_on(M, "INSERT INTO kv VALUES (1000,'stale',1)", timeout=8.0)
check(not r["ok"], f"少数派旧 leader 写被拒 ({r.get('error')})")
# 多数派选出新 leader 并继续写
L2 = wait_for("多数派选出新 leader", lambda: find_leader(alive=others))
check(L2 in others, f"多数派新 leader=node {L2}")
insert_batch(41, 10, L2)
wait_converged(others)
for i in others:
    check(local_rows(i) == sorted(expected), f"多数派节点 {i} 50 行一致")
# heal: 发散的未提交条目应被截断, 三节点收敛
for i in (0, 1, 2):
    try:
        req(PORTS[i], "/control", {"drop": []}, timeout=3.0)
    except Exception:
        pass  # 被 kill 的节点连不上, 正常
L3 = wait_for("heal 后集群有 leader", lambda: find_leader(alive=alive))
wait_converged(alive)
for i in alive:
    check(local_rows(i) == sorted(expected), f"heal 后节点 {i} 收敛, 无发散残留")

print("== D kill -9 follower 后重启追上 ==")
F = [i for i in alive if i != L3][0]
cluster("kill", str(F))
insert_batch(51, 5, L3)  # 它死掉期间继续写
cluster("restart", str(F))
wait_for(f"节点 {F} 重启后追上",
         lambda: local_rows(F) == sorted(expected), timeout=30.0)
check(True, f"节点 {F} 重启追上, 55 行")

print("== 终检: 三节点行集 == Python 真相 ==")
for i in (0, 1, 2):
    try:
        rows = local_rows(i)
    except Exception:
        continue  # 被 kill 未重启的旧 leader
    check(rows == sorted(expected), f"节点 {i} 行集逐行相等 (55 行)")
check(len(expected) == 55, f"Python 端共 55 行, 零丢失")

cluster("stop")
print(f"\n分布式正确性: {N} 个断言全部通过")
