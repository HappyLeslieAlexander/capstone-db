"""Write-Ahead Log: 追加写 + fsync,帧格式 [len:u32][crc32:u32][pickle].

LSN = 记录在文件中的起始字节偏移(单调递增,天然全序)。
日志记录类型(全部为 pickle 化的 dict):
  BEGIN    {txn}
  UPDATE   {txn, prev, key, old, new, idx_ops}
             old/new: 该 key 的版本链(before/after image,逻辑级)
             idx_ops: [(op, idxkey, page)] op in {'add','del'}
  COMMIT   {txn, prev, cts, keys}      # cts: 提交时间戳; keys: 本事务写过的 key
  ABORT    {txn, prev}
  END      {txn, prev}                # 事务彻底结束(abort 清理完成)
  CHECKPOINT {active:[txn...]}         # 脏页已全刷盘,active 应为空(只在事务间隙做)

崩溃恢复(ARIES 简化版,见 references/aries-notes.md):
  1. Analysis: 从 last_checkpoint 起扫描,建 txn_table{txn:(status,last_lsn)},
     收集 UPDATE 页集合(本实现 redo 为逻辑级,不严格需要 DPT,但保留记录)。
  2. Redo:   按 LSN 顺序重放所有 UPDATE(幂等逻辑重做)+ COMMIT 的 cts fixup。
  3. Undo:   loser(无 COMMIT 的 txn)按 LSN 逆序做逻辑 undo:
             从 key 的版本链里删掉 cts==-txn 的版本,撤销 idx_ops。
  redo/undo 都是幂等的 -> 恢复可重入,崩溃发生在恢复中途也能重跑,
  因此不需要 CLR(见 aries-notes.md 的诚实说明)。
"""
import os
import pickle
import struct
import zlib

HDR = struct.Struct("<II")  # len, crc32


class LogManager:
    def __init__(self, path: str):
        new = not os.path.exists(path)
        self.f = open(path, "a+b" if not new else "w+b")
        self.f.seek(0, os.SEEK_END)
        self._end = self.f.tell()

    def end_lsn(self) -> int:
        return self._end

    def append(self, rec: dict) -> int:
        payload = pickle.dumps(rec, protocol=4)
        lsn = self._end
        blob = HDR.pack(len(payload), zlib.crc32(payload) & 0xFFFFFFFF) + payload
        self.f.seek(lsn)
        self.f.write(blob)
        self._end += len(blob)
        return lsn

    def flush(self):
        """WAL 规则: COMMIT 返回前必须 flush+fsync."""
        self.f.flush()
        os.fsync(self.f.fileno())

    def iter_from(self, lsn: int):
        """从 lsn(含)开始顺序产出 (lsn, rec);遇到截断/坏帧即停(崩溃可能导致尾部半条记录)。"""
        self.f.seek(0, os.SEEK_END)
        size = self.f.tell()
        pos = lsn
        while pos + HDR.size <= size:
            self.f.seek(pos)
            hdr = self.f.read(HDR.size)
            if len(hdr) < HDR.size:
                break
            (length, crc) = HDR.unpack(hdr)
            if length > 64 * 1024 * 1024:  # 坏帧保护
                break
            payload = self.f.read(length)
            if len(payload) < length:
                break  # 尾部半条记录:kill -9 的典型现场,直接丢弃
            if zlib.crc32(payload) & 0xFFFFFFFF != crc:
                break  # 坏帧:停(不应发生;发生了说明有 bug)
            yield pos, pickle.loads(payload)
            pos += HDR.size + length

    def scan_all(self):
        return self.iter_from(0)

    def close(self):
        self.f.close()
