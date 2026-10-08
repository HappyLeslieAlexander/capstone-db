"""DiskManager: fixed-size page file. Bustub DiskManager 的极简版.

页面大小 4096 字节。文件按需扩展；读越界页返回全零页（恢复时可能遇到
WAL 里引用了但从未落盘的新页——steal/no-force 下这是合法的）。
"""
import os
import struct

PAGE_SIZE = 4096
# 每页前 8 字节: pageLSN (由 BufferPool/恢复逻辑维护,小端 uint64)
LSN_OFF = 0
PAYLOAD_OFF = 8


class DiskManager:
    def __init__(self, path: str):
        self.path = path
        new = not os.path.exists(path)
        self.f = open(path, "r+b" if not new else "w+b")
        if new:
            self.f.truncate(0)
        # 页号高水位: 正常时 == num_pages(); MiniDB 启动时会用 WAL 中的
        # 最大页号把它抬高 (防 kill -9 丢文件扩展后的页号复用).
        self._next_pid = None

    def num_pages(self) -> int:
        self.f.seek(0, os.SEEK_END)
        return self.f.tell() // PAGE_SIZE

    def read_page(self, page_id: int) -> bytearray:
        """返回 PAGE_SIZE 字节;越界则返回全零页(不抛异常)."""
        buf = bytearray(PAGE_SIZE)
        off = page_id * PAGE_SIZE
        self.f.seek(0, os.SEEK_END)
        if off < self.f.tell():
            self.f.seek(off)
            n = self.f.readinto(buf)
            assert n == PAGE_SIZE, f"short read page {page_id}: {n}"
        return buf

    def write_page(self, page_id: int, data: bytes):
        assert len(data) == PAGE_SIZE, f"bad page size {len(data)}"
        self.f.seek(page_id * PAGE_SIZE)
        self.f.write(data)

    def allocate_page(self) -> int:
        # CAPSTONE: 页号高水位 (见 MiniDB.__init__ 的初始化).
        # allocate_page 只做 buffered write、不 sync, kill -9 可能丢掉这次
        # 文件扩展, 导致重启后 num_pages() 变小. 若仍用 num_pages() 分配页号,
        # 就会复用一个已存在的页 -> B+ 树成环 -> 持锁线程卡死
        # (2026-10-09 实测抓到). 高水位保证页号单调递增、永不复用.
        if self._next_pid is None:
            self._next_pid = self.num_pages()
        pid = self._next_pid
        self._next_pid += 1
        self.f.seek(pid * PAGE_SIZE)
        self.f.write(b"\x00" * PAGE_SIZE)
        return pid

    def sync(self):
        self.f.flush()
        os.fsync(self.f.fileno())

    def close(self):
        self.f.close()

    # ---- pageLSN helpers (header, 小端 u64) ----
    @staticmethod
    def get_lsn(page: bytearray) -> int:
        return struct.unpack_from("<Q", page, LSN_OFF)[0]

    @staticmethod
    def set_lsn(page: bytearray, lsn: int):
        struct.pack_into("<Q", page, LSN_OFF, lsn)
