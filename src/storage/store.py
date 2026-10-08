#!/usr/bin/env python3
"""TableStore: 把 SQL 的"表"映射到 MiniDB(vendored db-engine)的 B+ 树上.

键编码 (B+ 树用原生 tuple 比较, 同一棵树内保持全序):
  目录项  ("\\x00catalog", table_name) -> {"columns": [...], "pk": ...}
  数据行  (table_name, tag, pk_value)  -> {col: val, ...}
    tag = "i" (INT 主键) / "s" (TEXT 主键), 保证 int/str 主键不混比.
  元数据  ("\\x00meta", "applied")     -> {"index": N}
    N = 本节点已 apply 的最大 Raft 日志 index. 它和语句的写入在**同一个**
    MiniDB 事务里提交 -- 这是"Raft 提交 + WAL 双保险咬合"的关键
    (crash 发生在两者之间不可能, 见 docs/blogs/03).

复用说明: 所有 B+ 树/WAL/MVCC/恢复逻辑都在 src/vendor/minidb (已验证资产);
本文件是新增的适配层 (~230 行): 表目录、行编解码、applied 游标.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "..", "vendor", "minidb"))
from engine import MiniDB  # noqa: E402  (vendored db-engine)

CATALOG_NS = "\x00catalog"
META_NS = "\x00meta"
APPLIED_KEY = (META_NS, "applied")
HI = "􏿿"  # U+10FFFF, 扫描上界


class StoreError(Exception):
    pass


def row_key(table, pk_val):
    if isinstance(pk_val, bool):
        raise StoreError("主键不支持 bool")
    if isinstance(pk_val, int):
        return (table, "i", pk_val)
    if isinstance(pk_val, str):
        return (table, "s", pk_val)
    raise StoreError(f"主键值类型不支持: {type(pk_val).__name__}")


def catalog_key(table):
    return (CATALOG_NS, table)


class TableStore:
    def __init__(self, path):
        os.makedirs(path, exist_ok=True)
        # 同一数据目录只允许一个实例打开: 防止运维误操作 (如 restart 时旧进程
        # 未死就起新进程) 导致两个 MiniDB 写同一批文件 (2026-10-09 实测抓到
        # 因此类并发写引发的 B+ 树页号复用与成环).
        # 注意: 这防不住 `rm -rf 数据目录` 后重建 (inode 已换), 那种操作本身
        # 就是禁止的, 见 README 运维红线.
        import fcntl
        self._lock_f = open(os.path.join(path, "LOCK"), "w")
        try:
            fcntl.flock(self._lock_f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise StoreError(
                f"数据目录 {path} 已被另一个进程锁定, 拒绝重复打开 "
                f"(先确认旧节点进程已退出)")
        self.db = MiniDB(path)

    # ---------- 目录 ----------
    def get_schema(self, txn, table):
        rec = txn.get(catalog_key(table))
        if rec is None:
            return None
        return {"table": table, "columns": rec["columns"], "pk": rec["pk"]}

    def create_table_txn(self, txn, table, columns, pk):
        if self.get_schema(txn, table) is not None:
            raise StoreError(f"表 {table!r} 已存在")
        txn.put(catalog_key(table),
                {"columns": [{"name": n, "type": t} for n, t in columns],
                 "pk": pk})

    def drop_table_txn(self, txn, table):
        schema = self.get_schema(txn, table)
        if schema is None:
            raise StoreError(f"表 {table!r} 不存在")
        # 先物化 key 列表: 边扫描边删会破坏 B+ 树叶链迭代 (db-engine 同款教训)
        keys = [k for k, _ in self.db.pk.scan(lo=(table,), hi=(table, HI))]
        for k in keys:
            txn.delete(k)
        txn.delete(catalog_key(table))

    # ---------- 行 ----------
    def get_row(self, txn, table, pk_val):
        return txn.get(row_key(table, pk_val))

    def put_row(self, txn, table, pk_val, row):
        txn.put(row_key(table, pk_val), dict(row))

    def delete_row(self, txn, table, pk_val):
        txn.delete(row_key(table, pk_val))

    def scan_rows(self, txn, table):
        """产出 (pk_val, row_dict), 按主键 tag+值排序."""
        for (t, tag, pk_val), _ in self.db.pk.scan(lo=(table,), hi=(table, HI)):
            row = txn.get((t, tag, pk_val))
            if row is not None:
                yield pk_val, row

    def count_rows(self, txn, table):
        return sum(1 for _ in self.scan_rows(txn, table))

    # ---------- applied 游标 (Raft 日志 apply 进度, 与语句同事务提交) ----------
    def get_applied_index(self):
        txn = self.db.begin("RC")
        try:
            rec = txn.get(APPLIED_KEY)
            return rec["index"] if rec else 0
        finally:
            txn.abort()

    def apply_committed(self, index, plan):
        """执行一条已提交的 Raft 日志条目 (plan dict), 返回 result dict.

        语句级失败 (SQLError, 如主键冲突) 被记录为 {"ok": False, ...} 的
        result, 数据无残留 (先校验后写), applied 游标照常推进 --
        失败的语句本身也是一条被复制的事实, 不是静默丢弃.
        非 SQLError 的异常 (代码 bug) 直接上抛: 宁可 loudly 失败也不藏.
        """
        from sql.executor import apply_plan, SQLError
        txn = self.db.begin("RC")
        try:
            result = apply_plan(self, txn, plan)
        except SQLError as e:
            txn.abort()
            txn = self.db.begin("RC")
            txn.put(APPLIED_KEY, {"index": index})
            txn.commit()
            return {"ok": False, "error": str(e)}
        txn.put(APPLIED_KEY, {"index": index})
        txn.commit()
        result["applied_index"] = index
        return result

    def close(self):
        self.db.close()
        # 释放数据目录互斥锁 (否则同进程 close 后重开会被自己挡住)
        try:
            self._lock_f.close()
        except Exception:
            pass
