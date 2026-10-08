"""B+ 树索引:CMU 15-445 Bustub Project 2 思路的 Python 实现.

- 节点 = 磁盘页(经 BufferPool),payload 为 pickle 化的 dict:
    {'leaf':bool,'keys':[...],'children':[...]|'vals':[...],'next':pid|None}
- 内部节点 keys[i] 为"右子树最小 key"(Bustub 风格,含重复分隔处理)
- insert: 上溢分裂(leaf/internal),分裂逐级向上传播
- delete: 下溢时先向兄弟借,借不到则合并;根收缩处理
- 叶节点经 'next' 串成链表,支持范围扫描 iterator
- 所有页修改经 PageOp 接口,由 Engine 负责记 WAL(崩溃恢复见 wal.py)

与 Bustub 的差异(诚实标注,见 SKILL.md):
- 单线程,无 latch crabbing / page guard
- key 为 Python 对象(int / tuple),比较用原生 <,未做定长编码
- 删除 undo 不做 merge-back(见 engine.py)
"""
import bisect
import pickle


class PageOp:
    """Engine 实现:read/write/new 全部走 WAL + BufferPool."""

    def read(self, pid) -> dict: raise NotImplementedError

    def write(self, pid, node): raise NotImplementedError

    def new(self, node) -> int: raise NotImplementedError

    def free(self, pid): raise NotImplementedError  # 可空实现(空间暂不回收)


def encode(node: dict) -> bytes:
    return pickle.dumps(node, protocol=4)


def decode(buf: bytes) -> dict:
    return pickle.loads(buf)


class BPlusTree:
    def __init__(self, op: PageOp, order: int = 4):
        self.op = op
        self.order = order
        self.max_keys = 2 * order
        self.min_keys = order  # 非根节点下限(ceil(max/2))
        self.root = None  # root page id; None = 空树

    # ---------- 查找 ----------
    def search(self, key):
        if self.root is None:
            return None
        pid = self.root
        node = self.op.read(pid)
        while not node["leaf"]:
            i = bisect.bisect_right(node["keys"], key)
            pid = node["children"][i]
            node = self.op.read(pid)
        i = bisect.bisect_left(node["keys"], key)
        if i < len(node["keys"]) and node["keys"][i] == key:
            return node["vals"][i]
        return None

    def _find_leaf(self, key):
        """返回 (leaf_pid, leaf_node, path),path=[(parent_pid,parent_node,child_idx)...]"""
        pid = self.root
        node = self.op.read(pid)
        path = []
        seen = set()
        while not node["leaf"]:
            # CAPSTONE: 防御性成环检测 -- 磁盘页损坏/并发误用导致 child 指针
            # 形成环时, 原本会无限循环卡死持锁线程 (2026-10-09 实测抓到).
            # 正确性仍由上层保证, 这里只把"卡死"变成"大声报错".
            if pid in seen:
                raise RuntimeError(f"B+tree cycle at page {pid} for key {key!r}")
            seen.add(pid)
            i = bisect.bisect_right(node["keys"], key)
            path.append((pid, node, i))
            pid = node["children"][i]
            node = self.op.read(pid)
        return pid, node, path

    # ---------- 插入 ----------
    def insert(self, key, val):
        if self.root is None:
            pid = self.op.new({"leaf": True, "keys": [key], "vals": [val],
                               "next": None})
            self.root = pid
            return
        pid, leaf, path = self._find_leaf(key)
        i = bisect.bisect_left(leaf["keys"], key)
        if i < len(leaf["keys"]) and leaf["keys"][i] == key:
            leaf["vals"][i] = val  # 已存在则覆盖(调用方保证语义)
        else:
            leaf["keys"].insert(i, key)
            leaf["vals"].insert(i, val)
        self.op.write(pid, leaf)
        if len(leaf["keys"]) > self.max_keys:
            self._split_leaf(pid, leaf, path)

    def _split_leaf(self, pid, leaf, path):
        mid = len(leaf["keys"]) // 2
        right = {"leaf": True, "keys": leaf["keys"][mid:], "vals": leaf["vals"][mid:],
                 "next": leaf["next"]}
        rpid = self.op.new(right)
        leaf["keys"] = leaf["keys"][:mid]
        leaf["vals"] = leaf["vals"][:mid]
        leaf["next"] = rpid
        self.op.write(pid, leaf)
        self._insert_in_parent(pid, right["keys"][0], rpid, path)

    def _split_internal(self, pid, node, path):
        mid = len(node["keys"]) // 2
        up_key = node["keys"][mid]
        right = {"leaf": False, "keys": node["keys"][mid + 1:],
                 "children": node["children"][mid + 1:]}
        rpid = self.op.new(right)
        node["keys"] = node["keys"][:mid]
        node["children"] = node["children"][:mid + 1]
        self.op.write(pid, node)
        self._insert_in_parent(pid, up_key, rpid, path)

    def _insert_in_parent(self, lpid, key, rpid, path):
        if not path:
            nroot = self.op.new({"leaf": False, "keys": [key],
                                 "children": [lpid, rpid]})
            self.root = nroot
            return
        ppid, pnode, idx = path.pop()
        pnode["keys"].insert(idx, key)
        pnode["children"].insert(idx + 1, rpid)
        self.op.write(ppid, pnode)
        if len(pnode["keys"]) > self.max_keys:
            self._split_internal(ppid, pnode, path)

    # ---------- 删除 ----------
    def delete(self, key) -> bool:
        """返回 True=删掉了(或本来就不存在,幂等)。"""
        if self.root is None:
            return True
        pid, leaf, path = self._find_leaf(key)
        i = bisect.bisect_left(leaf["keys"], key)
        if i >= len(leaf["keys"]) or leaf["keys"][i] != key:
            return True  # 幂等:恢复重放时可能已删
        leaf["keys"].pop(i)
        leaf["vals"].pop(i)
        self.op.write(pid, leaf)
        if pid == self.root:
            return True  # 根叶节点允许欠满
        if len(leaf["keys"]) < self.min_keys:
            self._fix_underflow(pid, leaf, path)
        return True

    def _fix_underflow(self, pid, node, path):
        ppid, pnode, idx = path.pop()
        # 找兄弟:优先左兄弟
        if idx > 0:
            sib_pid = pnode["children"][idx - 1]
            sib = self.op.read(sib_pid)
            if len(sib["keys"]) > self.min_keys:
                self._borrow_from_left(pid, node, ppid, pnode, idx, sib_pid, sib)
                return
        if idx < len(pnode["children"]) - 1:
            sib_pid = pnode["children"][idx + 1]
            sib = self.op.read(sib_pid)
            if len(sib["keys"]) > self.min_keys:
                self._borrow_from_right(pid, node, ppid, pnode, idx, sib_pid, sib)
                return
        # 借不到:合并
        if idx > 0:
            sib_pid = pnode["children"][idx - 1]
            sib = self.op.read(sib_pid)
            self._merge(pid, node, ppid, pnode, idx, sib_pid, sib, left=True)
        else:
            sib_pid = pnode["children"][idx + 1]
            sib = self.op.read(sib_pid)
            self._merge(pid, node, ppid, pnode, idx, sib_pid, sib, left=False)
        # 父节点可能因此下溢
        if ppid == self.root:
            if not pnode["leaf"] and len(pnode["keys"]) == 0:
                self.root = pnode["children"][0]  # 根收缩
            return
        if len(pnode["keys"]) < self.min_keys:
            # 需要祖父路径:重建(简单做法:从根重新定位;恢复/单线程下可接受)
            self._fix_underflow_up(ppid)

    def _fix_underflow_up(self, pid):
        # 从根重新找 pid 的父路径(删除场景低频,简单可靠优先)
        if pid == self.root:
            node = self.op.read(pid)
            if not node["leaf"] and len(node["keys"]) == 0:
                self.root = node["children"][0]
            return
        path = self._path_to(pid)
        if path is None:
            return
        node = self.op.read(pid)
        self._fix_underflow(pid, node, path)

    def _path_to(self, target):
        # BFS 找从 root 到 target 的父路径(用于合并后父节点下溢的递归)
        if self.root is None:
            return None
        stack = [(self.root, [])]
        while stack:
            pid, path = stack.pop()
            node = self.op.read(pid)
            if node["leaf"]:
                continue
            for i, c in enumerate(node["children"]):
                if c == target:
                    return path + [(pid, node, i)]
                stack.append((c, path + [(pid, node, i)]))
        return None

    def _borrow_from_left(self, pid, node, ppid, pnode, idx, sib_pid, sib):
        if node["leaf"]:
            k, v = sib["keys"].pop(), sib["vals"].pop()
            node["keys"].insert(0, k)
            node["vals"].insert(0, v)
            pnode["keys"][idx - 1] = node["keys"][0]
        else:
            sep = pnode["keys"][idx - 1]
            k = sib["keys"].pop()
            c = sib["children"].pop()
            node["keys"].insert(0, sep)
            node["children"].insert(0, c)
            pnode["keys"][idx - 1] = k
        self.op.write(sib_pid, sib)
        self.op.write(pid, node)
        self.op.write(ppid, pnode)

    def _borrow_from_right(self, pid, node, ppid, pnode, idx, sib_pid, sib):
        if node["leaf"]:
            k, v = sib["keys"].pop(0), sib["vals"].pop(0)
            node["keys"].append(k)
            node["vals"].append(v)
            pnode["keys"][idx] = sib["keys"][0]
        else:
            sep = pnode["keys"][idx]
            k = sib["keys"].pop(0)
            c = sib["children"].pop(0)
            node["keys"].append(sep)
            node["children"].append(c)
            pnode["keys"][idx] = k
        self.op.write(sib_pid, sib)
        self.op.write(pid, node)
        self.op.write(ppid, pnode)

    def _merge(self, pid, node, ppid, pnode, idx, sib_pid, sib, left: bool):
        if node["leaf"]:
            if left:
                sib["keys"].extend(node["keys"])
                sib["vals"].extend(node["vals"])
                sib["next"] = node["next"]
                self.op.write(sib_pid, sib)
                pnode["keys"].pop(idx - 1)
                pnode["children"].pop(idx)
            else:
                node["keys"].extend(sib["keys"])
                node["vals"].extend(sib["vals"])
                node["next"] = sib["next"]
                self.op.write(pid, node)
                pnode["keys"].pop(idx)
                pnode["children"].pop(idx + 1)
        else:
            sep = pnode["keys"][idx - 1] if left else pnode["keys"][idx]
            if left:
                sib["keys"].append(sep)
                sib["keys"].extend(node["keys"])
                sib["children"].extend(node["children"])
                self.op.write(sib_pid, sib)
                pnode["keys"].pop(idx - 1)
                pnode["children"].pop(idx)
            else:
                node["keys"].append(sep)
                node["keys"].extend(sib["keys"])
                node["children"].extend(sib["children"])
                self.op.write(pid, node)
                pnode["keys"].pop(idx)
                pnode["children"].pop(idx + 1)
        self.op.write(ppid, pnode)
        self.op.free(pid if left else sib_pid)  # 被合并掉的页:暂不回收(诚实标注)

    # ---------- 范围扫描 ----------
    def scan(self, lo=None, hi=None):
        """产出 [(key,val)]，lo<=key<hi；lo/hi 为 None 表无界。"""
        if self.root is None:
            return
        start = lo if lo is not None else None
        if start is None:
            pid = self.root
            node = self.op.read(pid)
            while not node["leaf"]:
                pid = node["children"][0]
                node = self.op.read(pid)
        else:
            pid, node, _ = self._find_leaf(start)
        seen = set()  # CAPSTONE: 叶链成环时抛错而非无限循环 (同 _find_leaf)
        while True:
            for k, v in zip(node["keys"], node["vals"]):
                if lo is not None and k < lo:
                    continue
                if hi is not None and k >= hi:
                    return
                yield k, v
            if node["next"] is None:
                return
            pid = node["next"]
            if pid in seen:
                raise RuntimeError(f"B+tree leaf-chain cycle at page {pid}")
            seen.add(pid)
            node = self.op.read(pid)

    # ---------- 校验(测试与崩溃恢复后用) ----------
    def validate(self):
        """返回错误列表;[] = 结构合法。检查:有序性/占用率/分隔键/叶链完整性。"""
        errs = []
        if self.root is None:
            return errs
        all_keys = []

        def rec(pid, lo, hi, depth, depths):
            node = self.op.read(pid)
            ks = node["keys"]
            if ks != sorted(ks):
                errs.append(f"page {pid}: keys not sorted")
            if any(not (lo is None or k >= lo) or (hi is not None and k >= hi)
                   for k in ks):
                errs.append(f"page {pid}: key out of range ({lo},{hi})")
            if node["leaf"]:
                depths.append(depth)
                all_keys.extend(ks)
                if pid != self.root and not (self.min_keys <= len(ks) <= self.max_keys):
                    errs.append(f"leaf {pid}: occupancy {len(ks)}")
            else:
                if len(node["children"]) != len(ks) + 1:
                    errs.append(f"internal {pid}: children/keys mismatch")
                if pid != self.root and not (self.min_keys <= len(ks) <= self.max_keys):
                    errs.append(f"internal {pid}: occupancy {len(ks)}")
                bounds = [lo] + ks + [hi]
                for i, c in enumerate(node["children"]):
                    rec(c, bounds[i], bounds[i + 1], depth + 1, depths)

        depths = []
        rec(self.root, None, None, 0, depths)
        if len(set(depths)) > 1:
            errs.append(f"unbalanced: leaf depths {set(depths)}")
        # 叶链表覆盖检查
        pid = self.root
        node = self.op.read(pid)
        while not node["leaf"]:
            pid = node["children"][0]
            node = self.op.read(pid)
        chain = []
        while True:
            chain.extend(node["keys"])
            if node["next"] is None:
                break
            node = self.op.read(node["next"])
        if chain != sorted(all_keys):
            errs.append("leaf chain != tree keys")
        if chain != sorted(chain):
            errs.append("leaf chain not sorted")
        return errs

    def count(self):
        return sum(1 for _ in self.scan())
