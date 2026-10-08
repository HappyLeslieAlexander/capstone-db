"""BufferPoolManager: Bustub Project 1 思路的极简版.

- 固定帧数,LRU 淘汰(pin_count==0 的帧才可淘汰)
- steal: 脏页可被淘汰(淘汰前先刷盘) —— 未提交事务的修改可能落盘,WAL 必须能 undo
- no-force: commit 只刷 WAL,不刷数据页 —— 已提交事务的修改可能没落盘,WAL 必须能 redo
- pageLSN: 每页记录"最后应用到本页的日志 LSN",用于 redo 幂等判断与教学演示

注意:单线程实现,无 latch(见 SKILL.md 诚实标注)。
"""
from collections import OrderedDict
from disk import DiskManager, PAGE_SIZE


class Frame:
    __slots__ = ("page_id", "data", "dirty", "pin")

    def __init__(self, page_id: int, data: bytearray):
        self.page_id = page_id
        self.data = data
        self.dirty = False
        self.pin = 0


class BufferPoolManager:
    def __init__(self, disk: DiskManager, pool_size: int = 64):
        self.disk = disk
        self.pool_size = pool_size
        self.frames: dict[int, Frame] = {}
        self.lru: OrderedDict[int, None] = OrderedDict()  # 仅含 pin==0 的页

    # ---- 基本操作 ----
    def fetch(self, page_id: int) -> Frame:
        fr = self.frames.get(page_id)
        if fr is None:
            fr = self._load(page_id)
        if fr.pin == 0:
            self.lru.pop(page_id, None)
        fr.pin += 1
        return fr

    def new_page(self) -> Frame:
        pid = self.disk.allocate_page()
        fr = self._make_room()
        fr2 = Frame(pid, bytearray(PAGE_SIZE))
        self.frames[pid] = fr2
        fr2.pin = 1
        fr2.dirty = True
        return fr2

    def unpin(self, page_id: int, is_dirty: bool):
        fr = self.frames[page_id]
        if is_dirty:
            fr.dirty = True
        assert fr.pin > 0
        fr.pin -= 1
        if fr.pin == 0:
            self.lru[page_id] = None
            self.lru.move_to_end(page_id, last=False)  # MRU 在前,LRU 在后

    # ---- 刷盘 ----
    def flush_page(self, page_id: int):
        fr = self.frames.get(page_id)
        if fr is None or not fr.dirty:
            return
        self.disk.write_page(page_id, bytes(fr.data))
        fr.dirty = False

    def flush_all(self):
        for pid in list(self.frames):
            self.flush_page(pid)
        self.disk.sync()

    def dirty_pages(self):
        return [pid for pid, fr in self.frames.items() if fr.dirty]

    # ---- 内部 ----
    def _load(self, page_id: int) -> Frame:
        fr = self._make_room()
        data = self.disk.read_page(page_id)
        fr2 = Frame(page_id, data)
        self.frames[page_id] = fr2
        return fr2

    def _make_room(self):
        if len(self.frames) < self.pool_size:
            return None
        # 找 LRU 的可淘汰帧
        for pid in reversed(self.lru):  # OrderedDict 尾部 = 最久未用
            fr = self.frames[pid]
            assert fr.pin == 0
            if fr.dirty:  # steal: 刷盘后再淘汰
                self.disk.write_page(pid, bytes(fr.data))
                fr.dirty = False
            del self.frames[pid]
            del self.lru[pid]
            return None
        raise RuntimeError("buffer pool exhausted: all pages pinned")
