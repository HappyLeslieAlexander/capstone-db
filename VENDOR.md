# Vendored dependencies (复用资产清单)

本仓库站在两个已独立验证的 Skill 肩膀上。以下文件是 vendor 拷贝，
**不是本 Capstone 的新代码**（不计入 1500 行新代码目标）：

## 1. Raft 共识层 — `src/vendor/raft/raft_node.py`
- 来源：`~/workspace/skills/distributed-systems/bin/raft_node.py`
- 状态：**一处修改**（`_apply_new`）：先 apply 成功再推进 `last_applied`。
  原代码先 `+=1` 再 apply，若 `_apply_entry` 抛异常，该条目会被永久跳过
  （重启后 `last_applied=max(commit,applied)` 直接越过它）。
  改后 apply 失败则停住不推进，重启后从 applied 游标重放。
  （2026-10-09 实测抓到此 bug。）
- 已验证：选主 / 日志复制 / 提交推进 / 冲突截断 / 磁盘持久化，
  故障注入 26/26 断言通过（见原 skill `references/transcripts/`）。
- 复用方式：`src/node/db_node.py` 以 `DBNode(RaftNode)` 子类化复用，
  只重写状态机应用点 `_apply_entry` / `_load`，Raft 核心逻辑未动。

## 2. 存储引擎 — `src/vendor/minidb/`
- 来源：`~/workspace/skills/db-engine/bin/`（`disk.py` `wal.py` `buffer.py`
  `btree.py` `engine.py`）
- 状态：
  - `wal.py` `buffer.py`：**字节一致**，零修改。
  - `disk.py`：**一处修改**（`allocate_page` 用内存高水位 `_next_pid`
    而非每次都 `num_pages()`）：
    根因是 2026-10-09 实测抓到的真 bug——`allocate_page` 只做 buffered
    write 不 sync，kill -9 可能丢掉这次文件扩展；重启后 `num_pages()`
    变小，新页号复用已存在的页 → B+ 树成环 → 持锁线程卡死。
    改后页号单调递增永不复用（高水位初值 = max(文件页数, WAL 最大页号+1)）。
  - `btree.py`：**两处防御性修改**（`_find_leaf` 与 `scan` 的成环检测，
    出错抛 `RuntimeError` 而非无限循环）：
    纵深防御。正常路径下永不触发；若页损坏，触发即大声报错而非静默卡死。
  - `engine.py`：三处修改，外加原有最小适配
    （与 `engine.py.orig` 的 diff 见文件头注释，约 20 行净改动）：
    1. `MiniDB.__init__` 中 `_recover()` 之前用 WAL 最大页号初始化
       `disk._next_pid`（`_max_pid_in_wal`）：防 kill -9 丢文件扩展后的
       页号复用（2026-10-09 实测抓到真 bug）。
    2. `_recover` 的 Redo 改为两遍：先只重放 PAGE_WRITE（物理），再重放
       KV_UPDATE/COMMIT（逻辑）。原因：若按 LSN 混排，早期的 COMMIT
       会在后期的 PAGE_WRITE（新 root 页）还没重放时，用 final root
       去查一个还是全零的新页，导致"key missing"误报
       （2026-10-09 实测抓到，1000 行 kill -9 恢复必现）。
  - `engine.py`：**最小适配**（与 `engine.py.orig` 的 diff 见文件头注释，
    约 20 行净改动）：
    1. `Txn.put(key, val)`：val 改为任意 dict，不再维护原来演示用的
       `dept` 二级索引（SQL 层按设计做全表扫描，见 README 诚实标注）。
    2. `Txn.delete(key)`：同上，去掉 `dept` 索引维护。
    3. 删除 `scan_dept()` / `update_where()`（原 skill 幻读演示的专用 helper，
       非引擎核心）。
  - MVCC 版本链、WAL、ARIES 式三阶段恢复与原 skill **完全一致**，
    崩溃恢复能力继承（kill -9 实测见原 skill transcript）。
- 已验证（原 skill）：B+ 树 2949 断言、隔离级别 19 断言、崩溃恢复 8 项铁律。

## 3. 本 Capstone 真正的新代码（`src/` 其余、`bin/`、`tests/`、`demo/`）
- SQL 分词 / 递归下降解析 / AST（`src/sql/`）
- 表目录 + 行编解码 + Raft-applied 游标（`src/storage/store.py`）
- 计划生成（AST→可复制的 plan dict）与执行器（`src/sql/executor.py`）
- 分布式节点（Raft 状态机 = SQL 执行器）与 HTTP 服务（`src/node/db_node.py`）
- 客户端、集群脚本、测试、演示、文档
