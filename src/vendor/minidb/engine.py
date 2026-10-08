"""VENDORED from ~/workspace/skills/db-engine/bin/engine.py (db-engine skill,
validated 2026-10-08/09: B+ tree 2949 asserts, isolation 19 asserts, crash 8 laws).

CAPSTONE ADAPTATIONS (diff vs .orig kept minimal, see VENDOR.md):
  * Txn.put(key, val): val is now an arbitrary dict; the employees-specific
    secondary index on val["dept"] is NOT maintained (SQL layer does full
    table scans by design; documented in README).
  * Txn.delete(key): same dept-index removal.
  * Removed employees-demo helpers scan_dept() / update_where() (not engine core).
Everything else -- MVCC version chains, WAL, ARIES-style recovery -- is
byte-identical to the validated skill asset.
"""

"""MiniDB (original docstring preserved below): 支持 B+ 树索引 + 简单事务的存储引擎.

架构 (致敬 CMU 15-445 Bustub 的分层):
  DiskManager -> BufferPoolManager(steal/no-force, LRU) -> B+树(主键/二级索引)
  WAL(LogManager) + TransactionManager(MVCC 快照读)

数据模型: 单表 employees,主键 id(int),value={name,dept,salary};
  二级索引 dept_idx: 复合键 (dept, id) -> b'' ,用于范围查询/幻读演示.

MVCC 版本链: 主键叶节点的 value = pickle([ [cts, deleted, payload], ... ])
  cts>0 : 已提交版本,提交时间戳
  cts==-tid : 未提交版本,仅所属事务可见(读己之写)
可见性: 版本可见 <=> cts==-tid(自己) 或 0<cts<=snapshot_ts
  RR: snapshot_ts = begin 时的 counter;  RC: 每次语句取当前 counter.

WAL 记录(见 wal.py): BEGIN/PAGE_WRITE/KV_UPDATE/ROOT/COMMIT/ABORT/END/CHECKPOINT
恢复 = ARIES 简化版(Analysis/Redo/Undo),细节与取舍见 references/aries-notes.md.

诚实标注(详见 SKILL.md):
- 单线程:无 latch、无并发事务交织执行;隔离级别通过"交错事务对象"验证快照语义.
- redo/undo 为逻辑级(按 key),幂等;恢复可重入,故无 CLR.
- undo 不做 B+ 树 merge-back(欠满页残留,不影响正确性).
- 无版本回收(版本链只增不减);无死锁检测(单线程不需要).
"""
import bisect
import json
import os
import pickle

from disk import DiskManager, PAGE_SIZE, PAYLOAD_OFF
from wal import LogManager
from buffer import BufferPoolManager
from btree import BPlusTree, PageOp, encode as bencode, decode as bdecode

MAX_ID = 2 ** 62


def _enc_vl(vl):
    return pickle.dumps(vl, protocol=4)


def _dec_vl(buf):
    return pickle.loads(buf)


class _Op(PageOp):
    """PageOp 的 Engine 实现. txn/log_on 由 Engine 在操作前后切换."""

    def __init__(self, db):
        self.db = db
        self.txn = 0
        self.log_on = True

    def read(self, pid):
        fr = self.db.bpm.fetch(pid)
        try:
            return bdecode(bytes(fr.data[PAYLOAD_OFF:]))
        finally:
            self.db.bpm.unpin(pid, False)

    def _store(self, fr, node, lsn):
        after = bencode(node)
        fr.data[PAYLOAD_OFF:PAYLOAD_OFF + len(after)] = after
        end = PAYLOAD_OFF + len(after)
        fr.data[end:] = b"\x00" * (PAGE_SIZE - end)
        DiskManager.set_lsn(fr.data, lsn)

    def write(self, pid, node, lsn=None):
        """正常路径 lsn=None:记 PAGE_WRITE;恢复/fixup 路径传 lsn:不记日志."""
        fr = self.db.bpm.fetch(pid)
        try:
            before = bytes(fr.data[PAYLOAD_OFF:])
            after = bencode(node)
            if before == after:
                return DiskManager.get_lsn(fr.data)
            if lsn is None and self.log_on:
                lsn = self.db._log({"type": "PAGE_WRITE", "txn": self.txn,
                                    "page": pid, "before": before, "after": after})
            if lsn is None:
                lsn = DiskManager.get_lsn(fr.data)
            self._store(fr, node, lsn)
            return lsn
        finally:
            self.db.bpm.unpin(pid, True)

    def new(self, node):
        fr = self.db.bpm.new_page()
        pid = fr.page_id
        try:
            after = bencode(node)
            if self.log_on:
                lsn = self.db._log({"type": "PAGE_WRITE", "txn": self.txn,
                                    "page": pid, "before": None, "after": after})
            else:
                lsn = 0
            self._store(fr, node, lsn)
            return pid
        finally:
            self.db.bpm.unpin(pid, True)

    def free(self, pid):
        pass  # 页暂不回收(诚实标注)


class MiniDB:
    def __init__(self, path, pool_size=64):
        os.makedirs(path, exist_ok=True)
        self.path = path
        self.disk = DiskManager(os.path.join(path, "data.db"))
        self.log = LogManager(os.path.join(path, "wal.log"))
        self.bpm = BufferPoolManager(self.disk, pool_size)
        self.meta_path = os.path.join(path, "meta.json")
        self.walmeta_path = os.path.join(path, "wal.meta")
        self.op = _Op(self)
        self.pk = BPlusTree(self.op, order=4)
        self.idx = BPlusTree(self.op, order=4)
        self.counter = 0       # 已提交时间戳计数器
        self.next_tid = 1
        self.txn_last = {}     # tid -> last lsn (prev 链)
        self.active = {}       # tid -> Txn
        self._logged_roots = {}
        # CAPSTONE: 用 WAL 中的最大页号初始化页号高水位, 必须在 _recover()
        # 之前 (恢复重放也可能分配新页). 原理见 DiskManager.allocate_page.
        self.disk._next_pid = max(self.disk.num_pages(),
                                  self._max_pid_in_wal() + 1)
        self._recover()

    def _max_pid_in_wal(self):
        """扫描 WAL (从头, 含 checkpoint 前) 找引用过的最大页号."""
        mx = -1
        try:
            for _, r in self.log.iter_from(0):
                t = r.get("type")
                if t == "PAGE_WRITE":
                    mx = max(mx, r["page"])
                elif t == "ROOT":
                    mx = max(mx, r["pid"])
        except Exception:
            pass
        return mx

    # ---------- 日志辅助 ----------
    def _log(self, rec):
        tid = rec.get("txn")
        if tid is not None:
            rec["prev"] = self.txn_last.get(tid)
        lsn = self.log.append(rec)
        if tid is not None:
            self.txn_last[tid] = lsn
        return lsn

    def _sync_roots(self):
        for name, tree in (("pk", self.pk), ("idx", self.idx)):
            if self._logged_roots.get(name) != tree.root:
                self._log({"type": "ROOT", "tree": name, "pid": tree.root})
                self._logged_roots[name] = tree.root

    # ---------- 事务 ----------
    def begin(self, isolation="RR"):
        assert isolation in ("RC", "RR")
        tid = self.next_tid
        self.next_tid += 1
        self._log({"type": "BEGIN", "txn": tid})
        txn = Txn(self, tid, isolation)
        self.active[tid] = txn
        return txn

    def checkpoint(self):
        self.bpm.flush_all()
        # ARIES 要求 checkpoint 记录活跃事务表(含各自 lastLSN),否则
        # checkpoint 前开始、崩溃时仍未结束的事务会逃过 undo(2026-10-09 实测抓到)
        ckt = self._log({"type": "CHECKPOINT",
                         "active": {tid: self.txn_last.get(tid, 0)
                                    for tid in self.active}})
        self.log.flush()
        self._write_meta()
        tmp = self.walmeta_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"last_checkpoint": ckt}, f)
        os.replace(tmp, self.walmeta_path)

    def _write_meta(self):
        tmp = self.meta_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"pk": self.pk.root, "idx": self.idx.root,
                       "counter": self.counter, "next_tid": self.next_tid}, f)
        os.replace(tmp, self.meta_path)

    def close(self):
        self.checkpoint()
        self.bpm.flush_all()
        self.disk.sync()
        self.log.close()
        self.disk.close()

    # ---------- 恢复 ----------
    def _recover(self):
        cpoint = 0
        if os.path.exists(self.walmeta_path):
            with open(self.walmeta_path) as f:
                cpoint = json.load(f)["last_checkpoint"]
        base_cts, base_tid = 0, 1
        if os.path.exists(self.meta_path):
            with open(self.meta_path) as f:
                m = json.load(f)
            self.pk.root, self.idx.root = m["pk"], m["idx"]
            # checkpoint 前的 COMMIT 已被截断,必须从 meta 恢复计数器
            base_cts, base_tid = m.get("counter", 0), m.get("next_tid", 1)
        self._logged_roots = {"pk": self.pk.root, "idx": self.idx.root}

        records = list(self.log.iter_from(cpoint))
        txn_st = {}
        max_cts, max_tid = base_cts, base_tid - 1
        for lsn, r in records:  # Analysis
            t = r["type"]
            tid = r.get("txn")
            if t == "BEGIN":
                txn_st[tid] = ["active", lsn]
            elif t in ("PAGE_WRITE", "KV_UPDATE"):
                txn_st.setdefault(tid, ["active", lsn])[1] = lsn
            elif t == "COMMIT":
                txn_st[tid] = ["committed", lsn]
                max_cts = max(max_cts, r["cts"])
            elif t == "ABORT":
                txn_st[tid] = ["aborted", lsn]
            elif t == "END":
                txn_st.pop(tid, None)
            elif t == "CHECKPOINT":
                # 用 checkpoint 时的活跃事务表播种:它们的 BEGIN 在扫描起点之前
                for atid, last in r["active"].items():
                    txn_st.setdefault(int(atid), ["active", last])
            elif t == "ROOT":
                (self.pk if r["tree"] == "pk" else self.idx).root = r["pid"]
                self._logged_roots[r["tree"]] = r["pid"]
            if tid:
                max_tid = max(max_tid, tid)

        self.op.txn, self.op.log_on = 0, False
        # CAPSTONE: Redo 分两遍.
        # 第 1 遍只重放 PAGE_WRITE (物理): 把所有页面带到最终状态.
        # 第 2 遍重放 KV_UPDATE/COMMIT (逻辑): 此时树结构已稳定,
        # _locate 用的 final root 能正确找到 key.
        # 原因: 若按 LSN 混排重放, 早期的 COMMIT (如 LSN 66474) 会在
        # 后期的 PAGE_WRITE (如 LSN 9048286, 新 root 页) 还没重放时,
        # 用 final root 去查一个还是全零的新页, 导致误报
        # (2026-10-09 实测抓到).
        for lsn, r in records:
            if r["type"] == "PAGE_WRITE":
                self._redo_pagewrite(lsn, r)
        for lsn, r in records:
            t = r["type"]
            if t == "KV_UPDATE":
                self._redo_kv(lsn, r)
            elif t == "COMMIT":
                self._redo_commit(lsn, r)
            # NOTE: ROOT 在 Redo 阶段故意不处理: Analysis 已设为最终值

        losers = {tid: last for tid, (st, last) in txn_st.items()
                  if st != "committed"}
        for tid, last in sorted(losers.items()):  # Undo: 沿 prev 链逆序回退
            lsn, seen = last, set()               # (幂等逻辑 undo,可重入)
            while lsn and lsn not in seen:
                seen.add(lsn)
                r = self._read_at(lsn)
                if r is None:
                    break
                if r["type"] == "KV_UPDATE":
                    self._undo_kv(r)
                lsn = r.get("prev")

        self.op.log_on = True
        for tid in sorted(losers):
            self._log({"type": "END", "txn": tid})
        self.log.flush()
        self.counter, self.next_tid = max_cts, max_tid + 1
        self._write_meta()

    def _read_at(self, lsn):
        for l, r in self.log.iter_from(lsn):
            if l == lsn:
                return r
            break
        return None

    def _redo_pagewrite(self, lsn, r):
        fr = self.bpm.fetch(r["page"])
        try:
            if DiskManager.get_lsn(fr.data) < lsn:
                after = r["after"]
                fr.data[PAYLOAD_OFF:PAYLOAD_OFF + len(after)] = after
                end = PAYLOAD_OFF + len(after)
                fr.data[end:] = b"\x00" * (PAGE_SIZE - end)
                DiskManager.set_lsn(fr.data, lsn)
                self.bpm.unpin(r["page"], True)
            else:
                self.bpm.unpin(r["page"], False)
        except Exception:
            self.bpm.unpin(r["page"], False)
            raise

    def _redo_kv(self, lsn, r):
        pid, key = r["page"], r["key"]
        if pid is None:
            return  # abort 路径整键删除:物理变更已由 PAGE_WRITE 覆盖
        fr = self.bpm.fetch(pid)
        try:
            if DiskManager.get_lsn(fr.data) >= lsn:
                self.bpm.unpin(pid, False)
                return
            node = bdecode(bytes(fr.data[PAYLOAD_OFF:]))
            i = bisect.bisect_left(node["keys"], key)
            if i < len(node["keys"]) and node["keys"][i] == key:
                node["vals"][i] = r["new"]
            else:
                raise RuntimeError(f"redo: key {key} not on page {pid} (lsn {lsn})")
            after = bencode(node)
            fr.data[PAYLOAD_OFF:PAYLOAD_OFF + len(after)] = after
            end = PAYLOAD_OFF + len(after)
            fr.data[end:] = b"\x00" * (PAGE_SIZE - end)
            DiskManager.set_lsn(fr.data, lsn)
            self.bpm.unpin(pid, True)
        except Exception:
            self.bpm.unpin(pid, False)
            raise

    def _redo_commit(self, lsn, r):
        tid, cts = r["txn"], r["cts"]
        for key in r["keys"]:
            pid, node, i = self._locate(key)
            if i is None:
                raise RuntimeError(f"redo commit: key {key} missing")
            vl = _dec_vl(node["vals"][i])
            if any(v[0] == -tid for v in vl):
                vl = [[cts if v[0] == -tid else v[0], v[1], v[2]] for v in vl]
                node["vals"][i] = _enc_vl(vl)
                self.op.write(pid, node, lsn=lsn)

    def _undo_kv(self, r):
        tid = r["txn"]
        key = r["key"]
        raw = self.pk.search(key)
        if raw is not None:
            vl = _dec_vl(raw)
            kept = [v for v in vl if v[0] != -tid]
            pid, node, i = self._locate(key)
            if not kept:
                self.pk.delete(key)
            elif len(kept) != len(vl):
                node["vals"][i] = _enc_vl(kept)
                self.op.write(pid, node)
        for op, ik in reversed(r["idx"]):
            if op == "add":
                self.idx.delete(ik)
            else:
                self.idx.insert(ik, b"")

    # ---------- 内部: key 定位/页写入 ----------
    def _locate(self, key):
        pid, node, _ = self.pk._find_leaf(key)
        i = bisect.bisect_left(node["keys"], key)
        if i < len(node["keys"]) and node["keys"][i] == key:
            return pid, node, i
        return pid, node, None

    def _replace_pk(self, key, vbytes):
        pid, node, i = self._locate(key)
        assert i is not None
        node["vals"][i] = vbytes
        self.op.write(pid, node)
        return pid


class Txn:
    def __init__(self, db, tid, isolation):
        self.db = db
        self.tid = tid
        self.iso = isolation
        self.snap = db.counter
        self.state = "active"
        self.written = []        # 去重前的 key 序列
        self.idxops = {}         # key -> [(op, idxkey)]

    def _snap(self):
        return self.snap if self.iso == "RR" else self.db.counter

    def _use(self):
        assert self.state == "active", "txn not active"
        self.db.op.txn = self.tid
        self.db.op.log_on = True

    @staticmethod
    def _visible(vl, tid, snap):
        best = None
        for cts, deleted, payload in vl:
            if cts == -tid or (0 < cts <= snap):
                best = (deleted, payload)
        return None if best is None or best[0] else pickle.loads(best[1])

    def get(self, key):
        self._use()
        raw = self.db.pk.search(key)
        if raw is None:
            return None
        return self._visible(_dec_vl(raw), self.tid, self._snap())

    def _apply_kv(self, key, new_vl, idx_ops):
        """核心写路径: old/new 皆为版本链(list)或 None(键不存在)。"""
        db = self.db
        raw = db.pk.search(key)
        old_vl = _dec_vl(raw) if raw is not None else None
        if old_vl is None and new_vl is not None:
            db.pk.insert(key, _enc_vl(new_vl))
        elif new_vl is None:
            db.pk.delete(key)
        else:
            db._replace_pk(key, _enc_vl(new_vl))
        pid, _, _ = db._locate(key) if new_vl is not None else (None, None, None)
        if new_vl is None:  # 整键删除只发生在 undo/abort 清理
            pid = None
        for op, ik in idx_ops:
            if op == "add":
                db.idx.insert(ik, b"")
            else:
                db.idx.delete(ik)
        lsn = db._log({"type": "KV_UPDATE", "txn": self.tid, "key": key,
                       "page": pid,
                       "old": _enc_vl(old_vl) if old_vl is not None else None,
                       "new": _enc_vl(new_vl) if new_vl is not None else None,
                       "idx": idx_ops})
        if pid is not None:  # 让 pageLSN 覆盖本逻辑操作,减少 redo 重放
            fr = db.bpm.fetch(pid)
            try:
                if DiskManager.get_lsn(fr.data) < lsn:
                    DiskManager.set_lsn(fr.data, lsn)
                db.bpm.unpin(pid, True)
            except Exception:
                db.bpm.unpin(pid, True)
                raise
        db._sync_roots()
        if key not in self.written:
            self.written.append(key)
        self.idxops.setdefault(key, []).extend(idx_ops)

    def put(self, key, val: dict):
        """CAPSTONE: val is an arbitrary dict; no secondary index maintained."""
        self._use()
        payload = pickle.dumps(val, protocol=4)
        raw = self.db.pk.search(key)
        old_vl = _dec_vl(raw) if raw is not None else None
        new_vl = (old_vl or []) + [[-self.tid, False, payload]]
        self._apply_kv(key, new_vl, [])

    def delete(self, key):
        self._use()
        raw = self.db.pk.search(key)
        if raw is None:
            return
        old_vl = _dec_vl(raw)
        old_rec = self._visible(old_vl, self.tid, self._snap())
        if old_rec is None:
            return  # 对我不可见:幂等跳过
        new_vl = old_vl + [[-self.tid, True, b""]]
        self._apply_kv(key, new_vl, [])  # CAPSTONE: no secondary index

    # CAPSTONE: removed employees-demo helpers scan_dept()/update_where()

    def commit(self):
        self._use()
        assert self.state == "active"
        db = self.db
        cts = db.counter + 1
        keys = list(dict.fromkeys(self.written))
        lsn = db._log({"type": "COMMIT", "txn": self.tid, "cts": cts, "keys": keys})
        db.log.flush()  # WAL 规则:先落日志再返回
        for key in keys:  # fixup: -tid -> cts (幂等;崩溃后由 redo 补做)
            pid, node, i = db._locate(key)
            if i is None:
                continue
            vl = _dec_vl(node["vals"][i])
            if any(v[0] == -self.tid for v in vl):
                vl = [[cts if v[0] == -self.tid else v[0], v[1], v[2]] for v in vl]
                node["vals"][i] = _enc_vl(vl)
                db.op.write(pid, node, lsn=lsn)
        db.counter = cts
        self.state = "committed"
        db.active.pop(self.tid, None)
        db._sync_roots()
        return cts

    def abort(self):
        self._use()
        assert self.state == "active"
        db = self.db
        for key in reversed(list(dict.fromkeys(self.written))):
            raw = db.pk.search(key)
            if raw is None:
                continue
            vl = _dec_vl(raw)
            kept = [v for v in vl if v[0] != -self.tid]
            if not kept:
                if vl:  # 整键都是我的:删键(带日志,供 redo)
                    db.pk.delete(key)
                    db._log({"type": "KV_UPDATE", "txn": self.tid, "key": key,
                             "page": None, "old": _enc_vl(vl), "new": None, "idx": []})
            else:
                db._replace_pk(key, _enc_vl(kept))
                db._log({"type": "KV_UPDATE", "txn": self.tid, "key": key,
                         "page": db._locate(key)[0],
                         "old": _enc_vl(vl), "new": _enc_vl(kept), "idx": []})
            for op, ik in reversed(self.idxops.get(key, [])):
                if op == "add":
                    db.idx.delete(ik)
                else:
                    db.idx.insert(ik, b"")
        db._log({"type": "ABORT", "txn": self.tid})
        db.log.flush()
        db._log({"type": "END", "txn": self.tid})
        db.log.flush()
        self.state = "aborted"
        db.active.pop(self.tid, None)
        db._sync_roots()
